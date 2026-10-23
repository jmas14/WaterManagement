"""
WM_WS_M - Monitor de la Watering Station.
Hace de puente entre su Engine y la Central:
  - Se registra en la Central (cliente socket).
  - Espera a que su Engine se conecte (servidor socket en <puerto_engine>).
  - Cada segundo le pregunta SALUD#<id_ws> al Engine.
  - Si el Engine pasa a KO (o se cae) avisa a la Central con FUGA#<id_ws>;
    cuando vuelve a OK, con RESUELTO#<id_ws>.

Uso: python WM_WS_M.py <puerto_engine> <ip_central> <puerto_central> <id_ws> [ubicacion]
"""

import argparse
import socket
import sys
import time

from protocol import (ENQ, ACK, EOT, FinConversacion, recibir_byte,
                      recibir_mensaje, enviar_mensaje, unir, separar)

INTERVALO_SALUD = 1  # segundos entre comprobaciones


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] [MONITOR] {msg}")


class Monitor:
    def __init__(self, id_ws, ubicacion, ip_central, puerto_central, puerto_engine):
        self.id_ws = id_ws
        self.ubicacion = ubicacion
        self.ip_central = ip_central
        self.puerto_central = puerto_central
        self.puerto_engine = puerto_engine
        self.central = None
        self.averia = False  # último estado comunicado a la Central

    # ---------------- Central ----------------

    def conectar_central(self):
        try:
            self.central = socket.create_connection((self.ip_central, self.puerto_central), timeout=5)
        except OSError as e:
            sys.exit(f"[MONITOR] No puedo conectar con la Central {self.ip_central}:{self.puerto_central} -> {e}")
        self.central.settimeout(None)
        self.central.sendall(ENQ)
        if recibir_byte(self.central) != ACK:
            sys.exit("[MONITOR] La Central no ha aceptado la conexión")

        respuesta = self.peticion_central(unir("REG", self.id_ws, self.ubicacion))
        if respuesta is None or separar(respuesta)[0] != "OK":
            sys.exit(f"[MONITOR] Registro rechazado: {respuesta}")
        log(f"Registrada {self.id_ws} en la Central")

    def peticion_central(self, texto):
        log(f"-> CENTRAL {texto}")
        if not enviar_mensaje(self.central, texto):
            log("La Central ha respondido NACK")
            return None
        respuesta = recibir_mensaje(self.central)
        log(f"<- CENTRAL {respuesta}")
        return respuesta

    def informar(self, averia):
        """Avisa a la Central solo cuando el estado cambia."""
        if averia == self.averia:
            return
        self.averia = averia
        self.peticion_central(unir("FUGA" if averia else "RESUELTO", self.id_ws))

    # ---------------- Engine ----------------

    def esperar_engine(self, servidor):
        log(f"Esperando al Engine en el puerto {self.puerto_engine}...")
        while True:
            try:
                conn, addr = servidor.accept()
            except socket.timeout:
                continue
            conn.settimeout(None)
            if recibir_byte(conn) != ENQ:
                log(f"{addr} no envió ENQ, cierro la conexión")
                conn.close()
                continue
            conn.sendall(ACK)
            log(f"Engine conectado desde {addr}")
            return conn

    def vigilar_engine(self, conn):
        """Comprueba la salud del Engine cada segundo hasta que se cae."""
        while True:
            if not enviar_mensaje(conn, unir("SALUD", self.id_ws)):
                log("El Engine ha respondido NACK")
                time.sleep(INTERVALO_SALUD)
                continue
            respuesta = recibir_mensaje(conn)
            if respuesta is not None:
                estado = separar(respuesta)[0]
                if estado == "KO":
                    self.informar(True)
                elif estado == "OK":
                    self.informar(False)
                else:
                    log(f"Respuesta inesperada del Engine: {respuesta}")
            time.sleep(INTERVALO_SALUD)

    def run(self):
        self.conectar_central()

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
                    self.informar(True)
                finally:
                    conn.close()
        except KeyboardInterrupt:
            log("Apagando Monitor")
        finally:
            servidor.close()
            try:
                self.central.sendall(EOT)
                self.central.close()
            except OSError:
                pass


def main():
    parser = argparse.ArgumentParser(description="WM_WS_M")
    parser.add_argument("puerto_engine", type=int, help="Puerto donde escucho a mi Engine")
    parser.add_argument("ip_central", help="IP o nombre de la Central")
    parser.add_argument("puerto_central", type=int, help="Puerto de la Central")
    parser.add_argument("id_ws", help="Identificador de la Watering Station")
    parser.add_argument("ubicacion", nargs="?", default="Sin ubicacion")
    args = parser.parse_args()

    Monitor(args.id_ws, args.ubicacion, args.ip_central, args.puerto_central,
            args.puerto_engine).run()


if __name__ == "__main__":
    main()
