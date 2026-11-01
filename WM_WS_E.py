"""
WM_WS_E - Engine de la Watering Station (caudalímetro + electroválvula).

Uso: python WM_WS_E.py <ip_broker:puerto> <ip_monitor:puerto>

Socket con su Monitor (protocol.py), cada segundo:
  SALUD#<id_ws>   -> OK#<id_ws> | KO#<id_ws>
  El ID de la WS lo recibe del Monitor, que ya lo ha validado con la Central.

Kafka:
  Recibe de la Central (wm.ordenes):  REGAR / PARAR / BLOQUEAR / ACTIVAR
  Publica a la Central (wm.telemetria): INICIO / RECHAZO / RIEGO (cada segundo) / FIN
  Riego manual desde la estación: publica una SOLICITUD en wm.solicitudes, que la
  Central valida igual que si viniera de un operador.
"""

import argparse
import random
import re
import socket
import threading
import time

from protocol import (ENQ, ACK, FinConversacion, recibir_byte,
                      recibir_mensaje, enviar_mensaje, unir, separar)
from kafka_comun import (TOPIC_ORDENES, TOPIC_TELEMETRIA, TOPIC_SOLICITUDES, TOPIC_RESPUESTAS,
                         asegurar_topics, crear_productor, crear_consumidor,
                         enviar, mensajes)

CAUDAL_MIN, CAUDAL_MAX = 8.0, 15.0   # L/min simulados por el caudalímetro
ID_VALIDO = re.compile(r"^[A-Za-z0-9_-]{1,20}$")

MOTIVOS = {
    "LIMITE_TIEMPO": "se cumplió el tiempo programado",
    "FINALIZADO_EN_WS": "se detuvo desde la estación",
    "FUGA": "se detectó una fuga",
    "BLOQUEO": "la Central bloqueó la estación",
    "APAGADO_ENGINE": "se apagó la estación",
    "ORDEN_CENTRAL": "lo ordenó la Central",
    "DESCONEXION": "la estación perdió la conexión con la Central",
    "SIN_TELEMETRIA": "la Central dejó de recibir datos",
    "OCUPADA": "ya está regando",
    "FUERA_SERVICIO": "está fuera de servicio",
}


def texto_motivo(m):
    return MOTIVOS.get(m, (m or "").replace("_", " ").lower())


DENEGACIONES = {
    "OPERADOR_NO_REGISTRADO": "el operario no está registrado en la Central",
    "WS_FUGA": "la estación tiene una fuga",
    "WS_FUERA_SERVICIO": "la estación está fuera de servicio",
    "WS_DESCONECTADA": "la estación está desconectada",
    "WS_REGANDO": "la estación ya está regando",
    "WS_OCUPADA": "la estación está ocupada",
    "WS_NO_REGISTRADA": "esa estación no está registrada",
    "LA_WS_NO_RESPONDE": "la estación no ha respondido a tiempo",
}


def texto_denegacion(m):
    return DENEGACIONES.get(m, (m or "").replace("_", " ").lower())


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] [ENGINE] {msg}", flush=True)


class Engine:
    def __init__(self, broker, ip_monitor, puerto_monitor):
        self.broker = broker
        self.ip_monitor = ip_monitor
        self.puerto_monitor = puerto_monitor
        self.id_ws = None        # lo asigna el Monitor en el primer SALUD
        self.averia = False      # True mientras se simula una fuga (KO)
        self.bloqueada = False   # Fuera de Servicio por orden de la Central
        self.productor = None
        self.lock = threading.RLock()
        self.riego = None        # dict con los datos del riego en curso
        self.parar_riego = threading.Event()

    def publicar(self, tipo, **datos):
        if self.productor and self.id_ws:
            enviar(self.productor, TOPIC_TELEMETRIA, dict(tipo=tipo, id_ws=self.id_ws, **datos),
                   clave=self.id_ws)

    # =====================================================
    # SOCKET CON EL MONITOR
    # =====================================================

    def conectar_monitor(self):
        """Se conecta al Monitor y atiende sus comprobaciones. Si se cae, reintenta."""
        while True:
            try:
                with socket.create_connection((self.ip_monitor, self.puerto_monitor), timeout=5) as s:
                    s.settimeout(None)
                    s.sendall(ENQ)
                    if recibir_byte(s) != ACK:
                        log("El Monitor no aceptó la conexión")
                    else:
                        log("Conectado al Monitor")
                        self.atender_comprobaciones(s)
            except FinConversacion:
                log("El Monitor cerró la conversación (EOT)")
            except (ConnectionError, OSError) as e:
                log(f"Sin conexión con el Monitor ({e}). Reintento en 3 s")
            time.sleep(3)

    def atender_comprobaciones(self, s):
        while True:
            msg = recibir_mensaje(s)
            if msg is None:
                continue  # trama corrupta: ya se ha enviado NACK
            campos = separar(msg)
            if campos[0] == "SALUD" and len(campos) >= 2:
                if self.id_ws != campos[1]:
                    self.id_ws = campos[1]
                    log(f"ID asignado por el Monitor: {self.id_ws}")
                enviar_mensaje(s, unir("KO" if self.averia else "OK", self.id_ws))
            else:
                enviar_mensaje(s, unir("ERROR", "PETICION_DESCONOCIDA"))

    # =====================================================
    # RIEGO (electroválvula + caudalímetro)
    # =====================================================

    def iniciar_riego(self, id_fo, duracion):
        with self.lock:
            motivo = ("FUGA" if self.averia else "FUERA_SERVICIO" if self.bloqueada
                      else "OCUPADA" if self.riego else None)
            if motivo:
                log(f"Rechazo el riego de {id_fo}: la estación {texto_motivo(motivo)}" if motivo != "FUGA" else f"Rechazo el riego de {id_fo}: la estación tiene una fuga")
                self.publicar("RECHAZO", id_fo=id_fo, motivo=motivo)
                return
            self.parar_riego.clear()
            self.riego = {"id_fo": id_fo, "duracion": duracion, "motivo": None}
        log(f">>> ELECTROVÁLVULA ABIERTA. Riego para {id_fo} "
            f"({f'{duracion} s' if duracion else 'sin límite, F para terminar'})")
        self.publicar("INICIO", id_fo=id_fo, duracion=duracion)
        threading.Thread(target=self.bucle_riego, daemon=True).start()

    def detener_riego(self, motivo):
        with self.lock:
            if self.riego and not self.riego["motivo"]:
                self.riego["motivo"] = motivo
                self.parar_riego.set()

    def bucle_riego(self):
        r = self.riego
        inicio = time.time()
        volumen = 0.0
        while True:
            if self.parar_riego.wait(timeout=1):
                motivo = r["motivo"]
                break
            segundos = time.time() - inicio
            caudal = round(random.uniform(CAUDAL_MIN, CAUDAL_MAX), 2)
            volumen += caudal / 60  # L/min durante 1 s
            print(f"    [{self.id_ws}] regando para {r['id_fo']}: {caudal:5.2f} L/min | "
                  f"{volumen:7.2f} L | {segundos:4.0f} s", flush=True)
            self.publicar("RIEGO", id_fo=r["id_fo"], caudal=caudal,
                          volumen=round(volumen, 2), segundos=round(segundos, 1))
            if r["duracion"] and segundos >= r["duracion"]:
                motivo = "LIMITE_TIEMPO"
                break
        segundos = time.time() - inicio
        with self.lock:
            self.riego = None
        log(f"<<< ELECTROVÁLVULA CERRADA: {texto_motivo(motivo)}")
        log(f"Riego de {r['id_fo']} finalizado: {volumen:.2f} L en {segundos:.0f} s")
        self.publicar("FIN", id_fo=r["id_fo"], motivo=motivo,
                      volumen=round(volumen, 2), segundos=round(segundos, 1))

    # =====================================================
    # RIEGO MANUAL DESDE LA ESTACIÓN (lo pide un operario)
    # =====================================================

    def pedir_riego_manual(self, id_fo, duracion):
        """Un operario pide el riego desde el menú de la estación. La Central lo valida
        como cualquier otra petición de operario (incluido que esté registrado)."""
        if not self.id_ws or not self.productor:
            log("Todavía no tengo ID o no estoy conectado a Kafka")
            return False
        if not id_fo or not ID_VALIDO.match(id_fo):
            log("Riego manual cancelado: hay que indicar un operario válido (letras, números, - o _)")
            return False
        log(f"El operario {id_fo} solicita a la Central un riego manual en esta estación")
        enviar(self.productor, TOPIC_SOLICITUDES,
               {"tipo": "SOLICITUD", "origen": "FO", "id_fo": id_fo,
                "id_ws": self.id_ws, "duracion": duracion or None})
        return True

    def bucle_respuestas(self):
        """Para enseñar en pantalla cuando la Central deniega un riego de esta estación."""
        asegurar_topics(self.broker)
        consumidor = crear_consumidor(self.broker, TOPIC_RESPUESTAS)
        for _, msg in mensajes(consumidor):
            if (self.id_ws and msg.get("id_ws") == self.id_ws and msg["tipo"] == "DENEGADO"
                    and not str(msg.get("motivo", "")).startswith("WS_RECHAZA_")):
                log(f"La Central deniega el riego de {msg.get('id_fo')}: "
                    f"{texto_denegacion(msg.get('motivo'))}")

    # =====================================================
    # KAFKA: ÓRDENES DE LA CENTRAL
    # =====================================================

    def bucle_ordenes(self):
        asegurar_topics(self.broker)
        self.productor = crear_productor(self.broker)
        consumidor = crear_consumidor(self.broker, TOPIC_ORDENES)
        log("Conectado a Kafka, esperando órdenes de la Central")
        for clave, msg in mensajes(consumidor):
            if not self.id_ws or clave != self.id_ws:
                continue  # no es para esta WS
            tipo = msg["tipo"]
            if tipo == "REGAR":
                dur = msg.get("duracion")
                log(f"La Central ordena regar para {msg.get('id_fo')} "
                    f"({f'{dur} s' if dur else 'sin límite'})")
                self.iniciar_riego(msg.get("id_fo"), msg.get("duracion"))
            elif tipo == "PARAR":
                log(f"La Central ordena parar el riego: {texto_motivo(msg.get('motivo', 'ORDEN_CENTRAL'))}")
                self.detener_riego(msg.get("motivo", "ORDEN_CENTRAL"))
            elif tipo == "BLOQUEAR":
                if not self.bloqueada:
                    log("*** FUERA DE SERVICIO por orden de la Central ***")
                self.bloqueada = True
                self.detener_riego("BLOQUEO")
            elif tipo == "ACTIVAR":
                if self.bloqueada:
                    log("*** ACTIVADA de nuevo por la Central ***")
                self.bloqueada = False

    # =====================================================
    # MENÚ DE LA ESTACIÓN
    # =====================================================

    def mostrar_estado(self):
        estado = ("FUGA" if self.averia else "FUERA DE SERVICIO" if self.bloqueada
                  else "REGANDO" if self.riego else "DISPONIBLE")
        print(f"WS {self.id_ws or '(sin ID todavía)'}: {estado}", flush=True)

    def menu(self):
        ayuda = ("Teclas (+ Enter):  K = simular fuga (KO) | O = resolver fuga | "
                 "R [seg] = regar manualmente\n                   F = finalizar riego | "
                 "E = ver estado | S = salir")
        print(ayuda, flush=True)
        while True:
            try:
                partes = input().strip().split()
            except EOFError:
                log("Sin consola interactiva: menú desactivado")
                while True:
                    time.sleep(3600)
            if not partes:
                continue
            opcion = partes[0].upper()
            if opcion == "K":
                self.averia = True
                log("*** FUGA simulada: respondo KO al Monitor ***")
                self.detener_riego("FUGA")
            elif opcion == "O":
                self.averia = False
                log("Fuga resuelta: respondo OK al Monitor")
            elif opcion == "R":
                if not self.id_ws or not self.productor:
                    log("Todavía no tengo ID o no estoy conectado a Kafka")
                    continue
                duracion = int(partes[1]) if len(partes) > 1 and partes[1].isdigit() else None
                id_fo = input("Operario que pide el riego: ").strip()
                self.pedir_riego_manual(id_fo, duracion)
            elif opcion == "F":
                if self.riego:
                    self.detener_riego("FINALIZADO_EN_WS")
                else:
                    log("No hay ningún riego en curso")
            elif opcion == "E":
                self.mostrar_estado()
            elif opcion == "S":
                self.detener_riego("APAGADO_ENGINE")
                time.sleep(1.5)  # para que salga el FIN
                log("Apagando Engine")
                break
            else:
                print(ayuda)


def main():
    parser = argparse.ArgumentParser(description="WM_WS_E")
    parser.add_argument("broker", help="IP:puerto del broker Kafka")
    parser.add_argument("monitor", help="IP:puerto del Monitor de esta WS")
    args = parser.parse_args()

    ip_monitor, puerto_monitor = args.monitor.rsplit(":", 1)
    engine = Engine(args.broker, ip_monitor, int(puerto_monitor))
    log(f"Monitor: {args.monitor} | Broker: {args.broker}")
    import engine_web; engine_web.arrancar(engine)

    threading.Thread(target=engine.conectar_monitor, daemon=True).start()
    threading.Thread(target=engine.bucle_ordenes, daemon=True).start()
    threading.Thread(target=engine.bucle_respuestas, daemon=True).start()
    try:
        engine.menu()
    except KeyboardInterrupt:
        log("Apagando Engine")


if __name__ == "__main__":
    main()