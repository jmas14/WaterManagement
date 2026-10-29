"""
WM_WS_M - Monitor de la Watering Station.

Uso: python WM_WS_M.py <puerto_engine> <ip_central> <puerto_central> <id_ws> [ubicacion]

Hace de puente entre su Engine y la Central (todo por sockets, protocol.py):
  - Se registra en la Central con su ID, ubicación y salud actual
    (REG#<id_ws>#<ubicacion>#<OK|KO>). La Central valida el ID.
  - Solo cuando la Central lo ha confirmado acepta la conexión de su Engine.
  - Cada segundo le pregunta SALUD#<id_ws> al Engine (OK / KO).
  - Si el Engine responde KO, no responde o se cae, avisa a la Central con
    FUGA#<id_ws>; cuando vuelve a OK, con RESUELTO#<id_ws>.
  - Cada 3 s envía VIVO#<id_ws> para que la Central sepa que sigue conectado.
  - Si la Central se cae, reintenta la conexión cada 3 s y, al volver,
    se registra de nuevo y le reenvía el estado de avería.
"""

import argparse
import os
import socket
import threading
import time

from protocol import (ENQ, ACK, EOT, FinConversacion, recibir_byte,
                      recibir_mensaje, enviar_mensaje, unir, separar)

INTERVALO_SALUD = 1       # s entre comprobaciones al Engine
TIMEOUT_ENGINE = 3        # s máximos esperando la respuesta del Engine
INTERVALO_LATIDO = 3      # s entre VIVO a la Central
REINTENTO_CENTRAL = 3     # s entre intentos de reconexión con la Central


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] [MONITOR] {msg}", flush=True)


class RegistroRechazado(Exception):
    pass


class Monitor:
    def __init__(self, id_ws, ubicacion, ip_central, puerto_central, puerto_engine):
        self.id_ws = id_ws
        self.ubicacion = ubicacion
        self.ip_central = ip_central
        self.puerto_central = puerto_central
        self.puerto_engine = puerto_engine
        self.central = None
        self.averia = True             # hasta que el Engine diga OK, la WS no está sana
        self.averia_informada = None   # último estado que conoce la Central
        self.registrada = threading.Event()

    # =====================================================
    # CENTRAL
    # =====================================================

    def peticion_central(self, texto):
        if not texto.startswith("VIVO"):
            log(f"-> CENTRAL {texto}")
        if not enviar_mensaje(self.central, texto):
            raise ConnectionError("la Central ha respondido NACK")
        respuesta = recibir_mensaje(self.central)
        if respuesta is None:
            raise ConnectionError("respuesta corrupta de la Central")
        if not texto.startswith("VIVO"):
            log(f"<- CENTRAL {respuesta}")
        return separar(respuesta)

    def conectar_central(self):
        self.central = socket.create_connection((self.ip_central, self.puerto_central), timeout=5)
        self.central.sendall(ENQ)
        if recibir_byte(self.central) != ACK:
            raise ConnectionError("la Central no ha aceptado la conexión")
        averia = self.averia
        respuesta = self.peticion_central(unir("REG", self.id_ws, self.ubicacion,
                                               "KO" if averia else "OK"))
        if respuesta[0] != "OK":
            raise RegistroRechazado(respuesta[1] if len(respuesta) > 1 else "SIN_MOTIVO")
        self.averia_informada = averia
        log(f"{self.id_ws} registrada y validada por la Central (estado {respuesta[2]})")
        self.registrada.set()

    def cerrar_central(self):
        if self.central:
            try:
                self.central.close()
            except OSError:
                pass
        self.central = None

    def hilo_central(self):
        """Único hilo que usa el socket de la Central: registro, avisos y latido."""
        ultimo_envio = 0
        while True:
            try:
                if self.central is None:
                    self.conectar_central()
                    ultimo_envio = time.time()
                if self.averia != self.averia_informada:
                    averia = self.averia
                    if averia:
                        log("*** AVERÍA: aviso de FUGA a la Central ***")
                    self.peticion_central(unir("FUGA" if averia else "RESUELTO", self.id_ws))
                    self.averia_informada = averia
                    ultimo_envio = time.time()
                elif time.time() - ultimo_envio >= INTERVALO_LATIDO:
                    self.peticion_central(unir("VIVO", self.id_ws))
                    ultimo_envio = time.time()
                time.sleep(0.2)
            except RegistroRechazado as motivo:
                self.cerrar_central()
                if str(motivo) in ("ID_INVALIDO", "FORMATO_INCORRECTO"):
                    log(f"La Central rechaza el ID '{self.id_ws}': {motivo}. Termino.")
                    os._exit(1)  # sys.exit en un hilo solo terminaría el hilo
                log(f"Registro rechazado ({motivo}). Reintento en {REINTENTO_CENTRAL} s")
                time.sleep(REINTENTO_CENTRAL)
            except (FinConversacion, ConnectionError, OSError) as e:
                if self.central is not None:
                    log(f"Conexión con la Central perdida ({e})")
                else:
                    log(f"No puedo conectar con la Central {self.ip_central}:{self.puerto_central} ({e}). "
                        f"Reintento en {REINTENTO_CENTRAL} s")
                self.cerrar_central()
                time.sleep(REINTENTO_CENTRAL)

    # =====================================================
    # ENGINE
    # =====================================================

    def esperar_engine(self, servidor):
        log(f"Esperando al Engine en el puerto {self.puerto_engine}...")
        while True:
            try:
                conn, addr = servidor.accept()
            except socket.timeout:
                continue
            try:
                conn.settimeout(TIMEOUT_ENGINE)
                if recibir_byte(conn) != ENQ:
                    log(f"{addr} no envió ENQ, cierro la conexión")
                    conn.close()
                    continue
                conn.sendall(ACK)
            except OSError:
                conn.close()
                continue
            log(f"Engine conectado desde {addr}")
            return conn

    def vigilar_engine(self, conn):
        """Comprueba la salud del Engine cada segundo hasta que se cae."""
        fallos_seguidos = 0
        while True:
            if not enviar_mensaje(conn, unir("SALUD", self.id_ws)):
                fallos_seguidos += 1
            else:
                respuesta = recibir_mensaje(conn)
                estado = separar(respuesta)[0] if respuesta else None
                if estado in ("OK", "KO"):
                    fallos_seguidos = 0
                    if estado == "KO" and not self.averia:
                        log("El Engine informa de KO (fuga)")
                    if estado == "OK" and self.averia:
                        log("El Engine vuelve a estar OK")
                    self.averia = estado == "KO"
                else:
                    fallos_seguidos += 1
            if fallos_seguidos >= 3:
                log("El Engine no da respuestas válidas: lo trato como avería")
                self.averia = True
            time.sleep(INTERVALO_SALUD)

    def run(self):
        threading.Thread(target=self.hilo_central, daemon=True).start()
        log(f"WS {self.id_ws} ('{self.ubicacion}'). Registrándome en la Central "
            f"{self.ip_central}:{self.puerto_central}...")
        # El Engine recibe su ID de nosotros, así que esperamos a que la Central lo valide
        while not self.registrada.wait(timeout=1):
            pass

        servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        servidor.bind(("0.0.0.0", self.puerto_engine))
        servidor.listen(1)
        servidor.settimeout(1)  # para que Ctrl+C funcione en Windows
        try:
            while True:
                conn = self.esperar_engine(servidor)
                try:
                    self.vigilar_engine(conn)
                except (FinConversacion, ConnectionError, OSError) as e:
                    log(f"Conexión con el Engine perdida ({e}): lo trato como avería")
                    self.averia = True
                finally:
                    conn.close()
        finally:
            servidor.close()
            if self.central:
                try:
                    self.central.sendall(EOT)
                except OSError:
                    pass
                self.cerrar_central()


def main():
    parser = argparse.ArgumentParser(description="WM_WS_M")
    parser.add_argument("puerto_engine", type=int, help="Puerto donde escucho a mi Engine")
    parser.add_argument("ip_central", help="IP o nombre de la Central")
    parser.add_argument("puerto_central", type=int, help="Puerto de la Central")
    parser.add_argument("id_ws", help="Identificador de la Watering Station")
    parser.add_argument("ubicacion", nargs="?", default="Sin ubicacion")
    args = parser.parse_args()
    try:
        Monitor(args.id_ws, args.ubicacion, args.ip_central, args.puerto_central,
                args.puerto_engine).run()
    except KeyboardInterrupt:
        log("Apagando Monitor")


if __name__ == "__main__":
    main()
