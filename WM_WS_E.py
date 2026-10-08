"""
WM_WS_E - Engine de la Watering Station.
Versión actual: solo el socket con el Monitor (comprobaciones de salud).

Uso: python WM_WS_E.py <ip_broker:puerto> <ip_monitor:puerto>

Mensajes Monitor -> Engine (cada segundo):
  SALUD#<id_ws>   -> OK#<id_ws> | KO#<id_ws>
"""

import argparse
import socket
import threading
import time

from protocol import (ENQ, ACK, FinConversacion, recibir_byte,
                      recibir_mensaje, enviar_mensaje, unir, separar)


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] [ENGINE] {msg}")


class Engine:
    def __init__(self, ip_monitor, puerto_monitor):
        self.ip_monitor = ip_monitor
        self.puerto_monitor = puerto_monitor
        self.id_ws = None      # lo asigna el Monitor en el primer SALUD
        self.averia = False    # True mientras se simula un KO

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
                estado = "KO" if self.averia else "OK"
                enviar_mensaje(s, unir(estado, self.id_ws))
            else:
                enviar_mensaje(s, unir("ERROR", "PETICION_DESCONOCIDA"))

    def menu(self):
        print("Teclas: K + Enter = simular avería (KO) | O + Enter = resolverla | S + Enter = salir")
        while True:
            opcion = input().strip().upper()
            if opcion == "K":
                self.averia = True
                log("Avería simulada: responderé KO al Monitor")
            elif opcion == "O":
                self.averia = False
                log("Avería resuelta: responderé OK al Monitor")
            elif opcion == "S":
                log("Apagando Engine")
                break


def main():
    parser = argparse.ArgumentParser(description="WM_WS_E")
    parser.add_argument("broker", help="IP:puerto del broker Kafka")
    parser.add_argument("monitor", help="IP:puerto del Monitor de esta WS")
    args = parser.parse_args()

    ip_monitor, puerto_monitor = args.monitor.split(":")
    engine = Engine(ip_monitor, int(puerto_monitor))
    log(f"Monitor: {args.monitor} | Broker: {args.broker} (aún sin usar)")

    threading.Thread(target=engine.conectar_monitor, daemon=True).start()
    engine.menu()


if __name__ == "__main__":
    main()