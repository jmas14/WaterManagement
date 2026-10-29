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
import socket
import threading
import time

from protocol import (ENQ, ACK, FinConversacion, recibir_byte,
                      recibir_mensaje, enviar_mensaje, unir, separar)
from kafka_comun import (TOPIC_ORDENES, TOPIC_TELEMETRIA, TOPIC_SOLICITUDES,
                         asegurar_topics, crear_productor, crear_consumidor,
                         enviar, mensajes)

CAUDAL_MIN, CAUDAL_MAX = 8.0, 15.0   # L/min simulados por el caudalímetro


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
                log(f"Rechazo el riego de {id_fo}: {motivo}")
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
        log(f"<<< ELECTROVÁLVULA CERRADA ({motivo}). Total {volumen:.2f} L en {segundos:.0f} s")
        self.publicar("FIN", id_fo=r["id_fo"], motivo=motivo,
                      volumen=round(volumen, 2), segundos=round(segundos, 1))

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
                self.iniciar_riego(msg.get("id_fo"), msg.get("duracion"))
            elif tipo == "PARAR":
                log(f"La Central ordena parar el riego ({msg.get('motivo')})")
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
                log("Solicito a la Central un riego manual en esta estación")
                enviar(self.productor, TOPIC_SOLICITUDES,
                       {"tipo": "SOLICITUD", "origen": "WS", "id_fo": f"LOCAL-{self.id_ws}",
                        "id_ws": self.id_ws, "duracion": duracion})
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

    threading.Thread(target=engine.conectar_monitor, daemon=True).start()
    threading.Thread(target=engine.bucle_ordenes, daemon=True).start()
    try:
        engine.menu()
    except KeyboardInterrupt:
        log("Apagando Engine")


if __name__ == "__main__":
    main()
