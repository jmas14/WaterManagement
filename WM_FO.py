"""
WM_FO - Aplicación del operario de campo (Field Operator).

Uso: python WM_FO.py <ip_broker:puerto> <id_operador> [--fichero servicios.txt]

Kafka:
  Publica en wm.solicitudes:  SOLICITUD (id_fo, id_ws, duracion)
  Lee de wm.respuestas lo que lleva su id_fo como clave:
      INFO / AUTORIZADO / DENEGADO / RIEGO (cada segundo) / FIN (resumen)
  Lee de wm.estado_red la foto de todas las WS (comando 'lista').

Formato del fichero: una WS por línea, opcionalmente con la duración en segundos.
      WS-01
      WS-02#15
  Las líneas vacías o que empiezan por '#' se ignoran. Tras cada servicio
  (con éxito o no) espera 4 s antes de pedir el siguiente.
"""

import argparse
import queue
import threading
import time

from kafka_comun import (TOPIC_SOLICITUDES, TOPIC_RESPUESTAS, TOPIC_ESTADO_RED,
                         asegurar_topics, crear_productor, crear_consumidor,
                         enviar, mensajes)

ESPERA_ENTRE_SERVICIOS = 4     # s (lo pide el enunciado)
TIMEOUT_SIN_NOTICIAS = 10      # s sin mensajes de la Central -> servicio fallido
DURACION_POR_DEFECTO = 10      # s de riego si no se indica otra cosa (0 = sin límite)

MOTIVOS = {
    "LIMITE_TIEMPO": "se cumplió el tiempo programado",
    "FINALIZADO_EN_WS": "se detuvo desde la estación",
    "FUGA": "se detectó una fuga",
    "BLOQUEO": "la Central bloqueó la estación",
    "DESCONEXION": "la estación se desconectó",
    "SIN_TELEMETRIA": "la estación dejó de enviar datos",
    "WS_FUGA": "la estación tiene una fuga",
    "WS_FUERA_SERVICIO": "la estación está fuera de servicio",
    "WS_DESCONECTADA": "la estación está desconectada",
    "WS_REGANDO": "la estación ya está regando",
    "WS_OCUPADA": "la estación está ocupada",
    "WS_NO_REGISTRADA": "esa estación no existe",
    "LA_WS_NO_RESPONDE": "la estación no responde",
    "OPERADOR_ID_INVALIDO": "el ID de operador no es válido",
    "OPERADOR_NO_REGISTRADO": "el operador no está registrado: escribe 'registro' primero",
}


def texto_motivo(m):
    """Traduce el código de motivo que manda la Central a una frase natural."""
    if m and m.startswith("WS_RECHAZA_"):
        return "la estación rechazó el riego"
    return MOTIVOS.get(m, (m or "").replace("_", " ").lower())


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] [FO] {msg}", flush=True)


class Operador:
    def __init__(self, broker, id_fo):
        self.broker = broker
        self.id_fo = id_fo
        self.respuestas = queue.Queue()
        self.estaciones = []   # última foto de la red publicada por la Central
        asegurar_topics(broker)
        self.productor = crear_productor(broker)
        threading.Thread(target=self.escuchar_respuestas,
                         args=(crear_consumidor(broker, TOPIC_RESPUESTAS),), daemon=True).start()
        threading.Thread(target=self.escuchar_estado_red,
                         args=(crear_consumidor(broker, TOPIC_ESTADO_RED),), daemon=True).start()
        log(f"Operador {id_fo} conectado a Kafka en {broker}")

    def escuchar_respuestas(self, consumidor):
        for clave, msg in mensajes(consumidor):
            if clave == self.id_fo:
                self.respuestas.put(msg)

    def escuchar_estado_red(self, consumidor):
        for _, msg in mensajes(consumidor):
            if msg["tipo"] == "ESTADO_RED":
                self.estaciones = msg.get("estaciones", [])

    def mostrar_red(self):
        if not self.estaciones:
            print("Aún no he recibido el estado de la red (la Central lo publica cada 5 s)")
            return
        disponibles = [w["id"] for w in self.estaciones if w["estado"] == "DISPONIBLE"]
        print(f"WS disponibles: {', '.join(disponibles) or 'ninguna'}")
        for w in self.estaciones:
            print(f"   {w['id']:<10}{w['ubicacion'][:23]:<24}{w['estado']}")

    def registrar(self):
        """Pide a la Central que dé de alta a este operador. Devuelve True si queda registrado."""
        while not self.respuestas.empty():   # descartar restos de un servicio anterior
            self.respuestas.get_nowait()
        log(f"Solicito el registro del operador {self.id_fo} en la Central")
        if not enviar(self.productor, TOPIC_SOLICITUDES, {"tipo": "REGISTRO", "id_fo": self.id_fo}):
            log("No se pudo enviar el registro: Kafka no está disponible")
            return False
        limite = time.time() + TIMEOUT_SIN_NOTICIAS
        while time.time() < limite:
            try:
                msg = self.respuestas.get(timeout=1)
            except queue.Empty:
                continue
            if msg.get("tipo") == "REGISTRO_OK":
                log("Operador registrado en la Central" if msg.get("nuevo")
                    else "El operador ya estaba registrado en la Central")
                return True
            if msg.get("tipo") == "REGISTRO_KO":
                log(f"Registro rechazado: {texto_motivo(msg.get('motivo'))}")
                return False
        log("Sin noticias de la Central: no se ha podido registrar")
        return False

    def solicitar(self, id_ws, duracion=DURACION_POR_DEFECTO):
        """Pide un riego y muestra su evolución. Devuelve True si terminó con éxito."""
        while not self.respuestas.empty():   # descartar restos de un servicio anterior
            self.respuestas.get_nowait()
        log(f"Solicito riego en {id_ws} ({f'{duracion} s' if duracion else 'sin límite'})")
        if not enviar(self.productor, TOPIC_SOLICITUDES,
                      {"tipo": "SOLICITUD", "origen": "FO", "id_fo": self.id_fo,
                       "id_ws": id_ws, "duracion": duracion or None}):
            log("No se pudo enviar la solicitud: Kafka no está disponible")
            return False

        while True:
            try:
                msg = self.respuestas.get(timeout=TIMEOUT_SIN_NOTICIAS)
            except queue.Empty:
                print()
                log(f"Sin noticias de la Central en {TIMEOUT_SIN_NOTICIAS} s: servicio FALLIDO")
                return False
            if msg.get("id_ws") != id_ws:
                continue
            tipo = msg["tipo"]
            if tipo == "INFO":
                log(msg.get("texto", ""))
            elif tipo == "AUTORIZADO":
                log(f"AUTORIZADO: {msg.get('texto', '')}")
            elif tipo == "DENEGADO":
                log(f"DENEGADO: {texto_motivo(msg.get('motivo'))}")
                return False
            elif tipo == "RIEGO":
                print(f"\r    {id_ws} regando: {msg.get('caudal', 0):5.2f} L/min | "
                      f"{msg.get('volumen', 0):7.2f} L | {msg.get('segundos') or 0:4.0f} s",
                      end="", flush=True)
            elif tipo == "FIN":
                print()
                ok = bool(msg.get("exito"))
                log(f"Riego {'finalizado' if ok else 'interrumpido'}: {texto_motivo(msg.get('motivo'))}. "
                    f"{msg.get('volumen')} L en {msg.get('duracion')} s")
                return ok

    def modo_fichero(self, ruta):
        servicios = []
        with open(ruta, encoding="utf-8") as f:
            for linea in f:
                linea = linea.strip()
                if not linea or linea.startswith("#"):
                    continue
                partes = linea.split("#")
                duracion = int(partes[1]) if len(partes) > 1 and partes[1].isdigit() else DURACION_POR_DEFECTO
                servicios.append((partes[0], duracion))
        log(f"{len(servicios)} servicios leídos de {ruta}")
        resultados = []
        for i, (id_ws, duracion) in enumerate(servicios):
            resultados.append(self.solicitar(id_ws, duracion))
            if i < len(servicios) - 1:
                log(f"Espero {ESPERA_ENTRE_SERVICIOS} s antes del siguiente servicio")
                time.sleep(ESPERA_ENTRE_SERVICIOS)
        log(f"Fichero terminado: {sum(resultados)} de {len(resultados)} servicios con éxito")

    def modo_interactivo(self):
        print("Comandos:  <id_ws> [segundos]  pedir riego (0 = sin límite)  |  "
              "registro  darme de alta  |  lista  ver WS  |  salir", flush=True)
        while True:
            partes = input("> ").strip().split()
            if not partes:
                continue
            if partes[0].lower() == "salir":
                break
            if partes[0].lower() == "lista":
                self.mostrar_red()
                continue
            if partes[0].lower() == "registro":
                self.registrar()
                continue
            duracion = int(partes[1]) if len(partes) > 1 and partes[1].isdigit() else DURACION_POR_DEFECTO
            self.solicitar(partes[0], duracion)


def main():
    parser = argparse.ArgumentParser(description="WM_FO")
    parser.add_argument("broker", help="IP:puerto del broker Kafka")
    parser.add_argument("id_operador", help="Identificador del operador")
    parser.add_argument("--fichero", help="Fichero con los servicios a solicitar")
    args = parser.parse_args()

    try:
        operador = Operador(args.broker, args.id_operador)
        import operario_web; operario_web.arrancar(operador)
        if args.fichero:
            operador.modo_fichero(args.fichero)
        else:
            operador.modo_interactivo()
    except (KeyboardInterrupt, EOFError):
        pass
    log("Operador desconectado")


if __name__ == "__main__":
    main()