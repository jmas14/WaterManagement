"""
WM_WS_M - Monitor (prueba mínima).
Uso: python3 WM_WS_M.py <puerto_engine> <ip_central> <puerto_central> <id_ws> [ubicacion]
"""
import socket
import sys

from protocolo import ENQ, ACK, EOT, enviar, recibir, leer_byte

ip, puerto, id_ws = sys.argv[2], int(sys.argv[3]), sys.argv[4]
ubicacion = sys.argv[5] if len(sys.argv) > 5 else "Sin ubicacion"

try:
    sock = socket.create_connection((ip, puerto), timeout=5)
except OSError as e:
    sys.exit(f"[MON] No puedo conectar con {ip}:{puerto} -> {e}")
sock.settimeout(None)

# Saludo: ENQ -> ACK
sock.sendall(bytes([ENQ]))
if leer_byte(sock) != ACK:
    sys.exit("[MON] La central no ha aceptado la conexion")


def peticion(texto):
    """Manda un mensaje y devuelve la respuesta de la central."""
    print(f"[MON] -> {texto}")
    if not enviar(sock, texto):
        print("[MON] La central no ha dado ACK")
        return None
    respuesta = recibir(sock)
    print(f"[MON] <- {respuesta}")
    return respuesta


respuesta = peticion(f"REGISTRO#{id_ws}#{ubicacion}")
if respuesta is None or not respuesta.startswith("STATUS#OK"):
    sys.exit("[MON] Registro rechazado")

print("Escribe mensajes para la central ('salir' para terminar):")
try:
    while True:
        texto = input()
        if texto == "salir":
            break
        peticion(texto)
except (KeyboardInterrupt, EOFError):
    pass

sock.sendall(bytes([EOT]))
sock.close()
