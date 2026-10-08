"""
Protocolo de tramas para los sockets de WaterManagement.

Formato de trama: <STX><DATA><ETX><LRC>
  - DATA: "CODIGO#campo1#campo2#..." en UTF-8
  - LRC:  XOR byte a byte de DATA (1 byte)
"""

STX = b"\x02"
ETX = b"\x03"
EOT = b"\x04"
ENQ = b"\x05"
ACK = b"\x06"
NACK = b"\x15"

SEP = "#"


class FinConversacion(Exception):
    """El otro extremo ha enviado EOT: la conversación termina."""


def calcular_lrc(datos: bytes) -> int:
    lrc = 0
    for b in datos:
        lrc ^= b
    return lrc


def construir_trama(mensaje: str) -> bytes:
    datos = mensaje.encode("utf-8")
    return STX + datos + ETX + bytes([calcular_lrc(datos)])


def recibir_byte(sock) -> bytes:
    b = sock.recv(1)
    if not b:
        raise ConnectionError("Conexión cerrada por el otro extremo")
    return b


def leer_trama(sock):
    """Lee una trama completa. Devuelve (mensaje, lrc_correcto).
    Lanza FinConversacion si recibe EOT en lugar de una trama."""
    while True:
        b = recibir_byte(sock)
        if b == STX:
            break
        if b == EOT:
            raise FinConversacion()
    datos = bytearray()
    while True:
        b = recibir_byte(sock)
        if b == ETX:
            break
        datos += b
    lrc_recibido = recibir_byte(sock)[0]
    lrc_ok = lrc_recibido == calcular_lrc(datos)
    return datos.decode("utf-8", errors="replace"), lrc_ok


def enviar_mensaje(sock, mensaje: str) -> bool:
    """Envía una trama y espera ACK/NACK. Devuelve True si recibe ACK."""
    sock.sendall(construir_trama(mensaje))
    return recibir_byte(sock) == ACK


def recibir_mensaje(sock):
    """Recibe una trama y responde ACK o NACK. Devuelve el mensaje, o None si llegó corrupta."""
    mensaje, ok = leer_trama(sock)
    sock.sendall(ACK if ok else NACK)
    return mensaje if ok else None


def unir(*campos) -> str:
    return SEP.join(str(c) for c in campos)


def separar(mensaje: str) -> list:
    return mensaje.split(SEP)