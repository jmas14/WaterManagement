"""
WM_Central - Central de control de WaterManagement.
Versión actual: solo atiende a los Monitores (WM_WS_M) por sockets. Sin BD.

Uso: python WM_Central.py <puerto_escucha> <ip_broker:puerto>

Mensajes Monitor -> Central:
  REG#<id_ws>#<ubicacion>   alta o reconexión   -> OK#<id_ws>#<estado> | KO#<motivo>
  FUGA#<id_ws>              avería detectada    -> OK#<id_ws>          | KO#<motivo>
  RESUELTO#<id_ws>          avería resuelta     -> OK#<id_ws>          | KO#<motivo>
"""

import argparse
import socket
import threading
import time

from protocol import (ENQ, ACK, FinConversacion, recibir_byte,
                      recibir_mensaje, enviar_mensaje, unir, separar)

# =========================================================
# CONFIGURACIÓN Y ESTADO GLOBAL
# =========================================================

DISPONIBLE = "DISPONIBLE"
REGANDO = "REGANDO"
FUGA = "FUGA"
FUERA_SERVICIO = "FUERA_SERVICIO"
DESCONECTADA = "DESCONECTADA"

ESTACIONES = {}   # {id_ws: {"ubicacion": str, "estado": str}}
CONEXIONES = {}   # {id_ws: conn}  sockets activos de los Monitores
lock = threading.RLock()


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] [CENTRAL] {msg}")


def cambiar_estado(id_ws, estado):
    with lock:
        ESTACIONES[id_ws]["estado"] = estado
    log(f"{id_ws} -> {estado}")
    mostrar_panel()


# =========================================================
# PANEL (versión consola)
# =========================================================

def mostrar_panel():
    with lock:
        print("\n" + "=" * 50)
        print(f"{'ID':<10}{'UBICACIÓN':<24}{'ESTADO':<16}")
        print("-" * 50)
        for id_ws, datos in sorted(ESTACIONES.items()):
            print(f"{id_ws:<10}{datos['ubicacion']:<24}{datos['estado']:<16}")
        print("=" * 50 + "\n")


# =========================================================
# LÓGICA DE PETICIONES DEL MONITOR
# =========================================================

def procesar(campos, conn, id_sesion):
    """Devuelve (respuesta, id_sesion). id_sesion es la WS registrada en esta conexión."""
    codigo = campos[0]

    if codigo == "REG":
        if len(campos) < 3:
            return unir("KO", "FORMATO_INCORRECTO"), id_sesion
        id_ws, ubicacion = campos[1], campos[2]
        with lock:
            if id_ws in CONEXIONES and CONEXIONES[id_ws] is not conn:
                return unir("KO", "YA_CONECTADA"), id_sesion
            nueva = id_ws not in ESTACIONES
            CONEXIONES[id_ws] = conn
            ESTACIONES[id_ws] = {"ubicacion": ubicacion, "estado": DISPONIBLE}
        log(f"{'Alta' if nueva else 'Reconexión'} de {id_ws} en '{ubicacion}'")
        mostrar_panel()
        return unir("OK", id_ws, DISPONIBLE), id_ws

    # El resto de peticiones exige haberse registrado antes en esta conexión
    if id_sesion is None:
        return unir("KO", "NO_REGISTRADA"), id_sesion
    if len(campos) < 2 or campos[1] != id_sesion:
        return unir("KO", "ID_NO_COINCIDE"), id_sesion

    if codigo == "FUGA":
        cambiar_estado(id_sesion, FUGA)
        return unir("OK", id_sesion), id_sesion

    if codigo == "RESUELTO":
        cambiar_estado(id_sesion, DISPONIBLE)
        return unir("OK", id_sesion), id_sesion

    return unir("KO", "PETICION_DESCONOCIDA"), id_sesion


def atender_monitor(conn, addr):
    id_ws = None
    log(f"Nueva conexión desde {addr}")
    try:
        if recibir_byte(conn) != ENQ:
            log(f"{addr} no envió ENQ, cierro la conexión")
            return
        conn.sendall(ACK)

        while True:
            try:
                msg = recibir_mensaje(conn)
            except FinConversacion:
                log(f"{id_ws or addr} cierra la conversación (EOT)")
                break
            if msg is None:
                log(f"Trama corrupta de {id_ws or addr}, enviado NACK")
                continue
            campos = separar(msg)
            log(f"Recibido de {id_ws or addr}: {campos}")
            respuesta, id_ws = procesar(campos, conn, id_ws)
            enviar_mensaje(conn, respuesta)

    except (ConnectionError, OSError) as e:
        log(f"Conexión con {id_ws or addr} perdida: {e}")
    finally:
        conn.close()
        if id_ws:
            with lock:
                if CONEXIONES.get(id_ws) is conn:
                    del CONEXIONES[id_ws]
                    cambiar_estado(id_ws, DESCONECTADA)


# =========================================================
# MAIN
# =========================================================

def main():
    parser = argparse.ArgumentParser(description="WM_Central")
    parser.add_argument("puerto", type=int, help="Puerto de escucha para los Monitores")
    parser.add_argument("broker", help="IP:puerto del broker Kafka")
    args = parser.parse_args()

    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind(("0.0.0.0", args.puerto))
    servidor.listen()
    servidor.settimeout(1)  # para que Ctrl+C funcione en Windows
    log(f"Escuchando Monitores en el puerto {args.puerto} | Broker: {args.broker} (aún sin usar)")

    try:
        while True:
            try:
                conn, addr = servidor.accept()
            except socket.timeout:
                continue
            conn.settimeout(None)
            threading.Thread(target=atender_monitor, args=(conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        log("Apagando Central")
    finally:
        servidor.close()


if __name__ == "__main__":
    main()