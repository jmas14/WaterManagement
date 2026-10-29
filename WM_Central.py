"""
WM_Central - Central de control de WaterManagement.

Uso: python WM_Central.py <puerto_escucha> <ip_broker:puerto> [--db wm.db]

Comunicación:
  - Sockets (protocol.py) con los Monitores (WM_WS_M): alta, averías y latido.
  - Kafka (kafka_comun.py) con los Engines (WM_WS_E) y los Operadores (WM_FO).
  - SQLite para estaciones, operadores, riegos y eventos.

Mensajes Monitor -> Central (socket):
  REG#<id_ws>#<ubicacion>#<OK|KO>  alta o reconexión, con la salud actual
                                                -> OK#<id_ws>#<estado> | KO#<motivo>
  FUGA#<id_ws>              avería detectada    -> OK#<id_ws>          | KO#<motivo>
  RESUELTO#<id_ws>          avería resuelta     -> OK#<id_ws>          | KO#<motivo>
  VIVO#<id_ws>              latido cada 3 s     -> OK#<id_ws>          | KO#<motivo>
Si un Monitor pasa TIMEOUT_MONITOR segundos sin enviar nada, su WS queda DESCONECTADA.
"""

import argparse
import re
import socket
import sqlite3
import threading
import time

from protocol import (ENQ, ACK, FinConversacion, recibir_byte,
                      recibir_mensaje, enviar_mensaje, unir, separar)
from kafka_comun import (TOPIC_SOLICITUDES, TOPIC_RESPUESTAS, TOPIC_ORDENES,
                         TOPIC_TELEMETRIA, TOPIC_ESTADO_RED, asegurar_topics,
                         crear_productor, crear_consumidor, enviar, mensajes)

# =========================================================
# CONFIGURACIÓN
# =========================================================

DISPONIBLE = "DISPONIBLE"
REGANDO = "REGANDO"
FUGA = "FUGA"
FUERA_SERVICIO = "FUERA_SERVICIO"
DESCONECTADA = "DESCONECTADA"

TIMEOUT_MONITOR = 10        # s sin noticias del Monitor -> DESCONECTADA
TIMEOUT_AUTORIZACION = 5    # s que esperamos a que el Engine abra la válvula
TIMEOUT_TELEMETRIA = 5      # s sin telemetría durante un riego -> se da por terminado
PERIODO_ESTADO_RED = 5      # s entre publicaciones del estado de la red
OPERADORES_INICIALES = ["FO-01", "FO-02", "FO-03", "FO-04", "FO-05"]
ID_VALIDO = re.compile(r"^[A-Za-z0-9_-]{1,20}$")
FINALES_CORRECTOS = {"LIMITE_TIEMPO", "FINALIZADO_EN_WS"}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] [CENTRAL] {msg}", flush=True)


# =========================================================
# BASE DE DATOS (SQLite)
# =========================================================

class BaseDatos:
    def __init__(self, ruta):
        self.conn = sqlite3.connect(ruta, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        with self.lock, self.conn:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS estaciones (
                    id TEXT PRIMARY KEY,
                    ubicacion TEXT NOT NULL,
                    estado TEXT NOT NULL,
                    bloqueada INTEGER NOT NULL DEFAULT 0,
                    caudal REAL NOT NULL DEFAULT 0,
                    volumen REAL NOT NULL DEFAULT 0,
                    id_fo TEXT,
                    actualizado TEXT
                );
                CREATE TABLE IF NOT EXISTS operadores (
                    id TEXT PRIMARY KEY,
                    alta TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS riegos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    id_ws TEXT NOT NULL,
                    id_fo TEXT,
                    inicio TEXT,
                    fin TEXT,
                    volumen REAL,
                    duracion REAL,
                    motivo TEXT
                );
                CREATE TABLE IF NOT EXISTS eventos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    texto TEXT NOT NULL
                );
            """)
            for id_fo in OPERADORES_INICIALES:
                self.conn.execute("INSERT OR IGNORE INTO operadores VALUES (?, ?)",
                                  (id_fo, _ahora()))

    def cargar_estaciones(self):
        with self.lock:
            return [dict(f) for f in self.conn.execute("SELECT * FROM estaciones")]

    def guardar_estacion(self, ws):
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO estaciones VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (ws.id, ws.ubicacion, ws.estado, int(ws.bloqueada),
                 round(ws.caudal, 2), round(ws.volumen, 2), ws.id_fo, _ahora()))

    def operador_existe(self, id_fo):
        with self.lock:
            return self.conn.execute("SELECT 1 FROM operadores WHERE id = ?",
                                     (id_fo,)).fetchone() is not None

    def alta_operador(self, id_fo):
        with self.lock, self.conn:
            self.conn.execute("INSERT OR IGNORE INTO operadores VALUES (?, ?)", (id_fo, _ahora()))

    def operadores(self):
        with self.lock:
            return [f["id"] for f in self.conn.execute("SELECT id FROM operadores ORDER BY id")]

    def guardar_riego(self, id_ws, id_fo, inicio, volumen, duracion, motivo):
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT INTO riegos (id_ws, id_fo, inicio, fin, volumen, duracion, motivo)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (id_ws, id_fo, inicio, _ahora(), round(volumen, 2), round(duracion, 1), motivo))

    def evento(self, texto):
        with self.lock, self.conn:
            self.conn.execute("INSERT INTO eventos (ts, texto) VALUES (?, ?)", (_ahora(), texto))


def _ahora():
    return time.strftime("%Y-%m-%d %H:%M:%S")


# =========================================================
# ESTACIÓN (estado que la Central conoce de cada WS)
# =========================================================

class Estacion:
    def __init__(self, id_ws, ubicacion, bloqueada=False):
        self.id = id_ws
        self.ubicacion = ubicacion
        self.conectada = False   # hay un Monitor registrado y vivo
        self.averia = False      # el Monitor ha avisado de FUGA
        self.bloqueada = bloqueada  # orden de la Central (persistente)
        self.regando = False
        self.caudal = 0.0
        self.volumen = 0.0
        self.id_fo = None
        self.inicio_riego = None       # time.time() del INICIO
        self.ultima_telemetria = 0.0
        self.reserva = None            # (id_fo, instante) mientras se espera al Engine

    @property
    def estado(self):
        """El estado se deduce de los indicadores, por orden de prioridad."""
        if not self.conectada:
            return DESCONECTADA
        if self.averia:
            return FUGA
        if self.bloqueada:
            return FUERA_SERVICIO
        if self.regando:
            return REGANDO
        return DISPONIBLE

    def resumen(self):
        return {"id": self.id, "ubicacion": self.ubicacion, "estado": self.estado,
                "caudal": round(self.caudal, 2), "volumen": round(self.volumen, 2),
                "id_fo": self.id_fo if self.regando else None}


# =========================================================
# CENTRAL
# =========================================================

class Central:
    def __init__(self, puerto, broker, ruta_db):
        self.puerto = puerto
        self.broker = broker
        self.db = BaseDatos(ruta_db)
        self.lock = threading.RLock()
        self.conexiones = {}   # {id_ws: socket del Monitor}
        self.ws = {}
        # Punto 1: al arrancar, cargar las WS de la BD. Todas DESCONECTADAS
        # hasta que su Monitor vuelva a conectar.
        for fila in self.db.cargar_estaciones():
            self.ws[fila["id"]] = Estacion(fila["id"], fila["ubicacion"], bool(fila["bloqueada"]))
        for ws in self.ws.values():
            self.db.guardar_estacion(ws)
        self.productor = None

    # ---------------- utilidades ----------------

    def evento(self, texto):
        log(texto)
        self.db.evento(texto)

    def actualizar(self, ws, estado_anterior):
        """Guarda la WS y, si su estado ha cambiado, lo anuncia."""
        self.db.guardar_estacion(ws)
        if ws.estado != estado_anterior:
            self.evento(f"{ws.id}: {estado_anterior} -> {ws.estado}")
            self.mostrar_panel()
            self.publicar_estado_red()

    def responder_fo(self, id_fo, tipo, id_ws, **datos):
        if self.productor and id_fo:
            enviar(self.productor, TOPIC_RESPUESTAS,
                   dict(tipo=tipo, id_fo=id_fo, id_ws=id_ws, **datos), clave=id_fo)

    def orden_engine(self, id_ws, tipo, **datos):
        if self.productor:
            enviar(self.productor, TOPIC_ORDENES, dict(tipo=tipo, id_ws=id_ws, **datos), clave=id_ws)

    def publicar_estado_red(self):
        if self.productor:
            with self.lock:
                foto = [ws.resumen() for ws in sorted(self.ws.values(), key=lambda w: w.id)]
            enviar(self.productor, TOPIC_ESTADO_RED, {"tipo": "ESTADO_RED", "estaciones": foto})

    # ---------------- panel (consola) ----------------

    def mostrar_panel(self):
        with self.lock:
            lineas = ["", "=" * 86,
                      f"{'ID':<10}{'UBICACIÓN':<24}{'ESTADO':<16}{'CAUDAL L/min':>13}"
                      f"{'VOLUMEN L':>11}  {'OPERADOR':<10}",
                      "-" * 86]
            for ws in sorted(self.ws.values(), key=lambda w: w.id):
                regando = ws.estado == REGANDO
                lineas.append(
                    f"{ws.id:<10}{ws.ubicacion[:23]:<24}{ws.estado:<16}"
                    f"{(f'{ws.caudal:.1f}' if regando else '-'):>13}"
                    f"{(f'{ws.volumen:.1f}' if regando else '-'):>11}  "
                    f"{(ws.id_fo or '-') if regando else '-':<10}")
            if not self.ws:
                lineas.append("(no hay estaciones registradas)")
            lineas += ["=" * 86, ""]
        print("\n".join(lineas), flush=True)

    # =====================================================
    # SOCKETS: MONITORES
    # =====================================================

    def procesar_monitor(self, campos, conn, id_sesion):
        """Devuelve (respuesta, id_sesion). id_sesion es la WS registrada en esta conexión."""
        codigo = campos[0]

        if codigo == "REG":
            if len(campos) < 3:
                return unir("KO", "FORMATO_INCORRECTO"), id_sesion
            id_ws, ubicacion = campos[1], campos[2] or "Sin ubicacion"
            if not ID_VALIDO.match(id_ws):
                return unir("KO", "ID_INVALIDO"), id_sesion
            with self.lock:
                if id_ws in self.conexiones and self.conexiones[id_ws] is not conn:
                    return unir("KO", "YA_CONECTADA"), id_sesion
                nueva = id_ws not in self.ws
                if nueva:
                    self.ws[id_ws] = Estacion(id_ws, ubicacion)
                ws = self.ws[id_ws]
                anterior = DESCONECTADA if nueva else ws.estado
                ws.ubicacion = ubicacion
                ws.conectada = True
                ws.averia = len(campos) > 3 and campos[3] == "KO"
                self.conexiones[id_ws] = conn
                self.evento(f"{'Alta' if nueva else 'Reconexión'} de {id_ws} en '{ubicacion}'")
                self.actualizar(ws, anterior)
                estado = ws.estado
            if ws.bloqueada:
                self.orden_engine(id_ws, "BLOQUEAR")  # por si el Engine se ha reiniciado
            return unir("OK", id_ws, estado), id_ws

        # El resto de peticiones exige haberse registrado antes en esta conexión
        if id_sesion is None:
            return unir("KO", "NO_REGISTRADA"), id_sesion
        if len(campos) < 2 or campos[1] != id_sesion:
            return unir("KO", "ID_NO_COINCIDE"), id_sesion

        if codigo == "VIVO":
            return unir("OK", id_sesion), id_sesion

        if codigo == "FUGA":
            with self.lock:
                ws = self.ws[id_sesion]
                anterior = ws.estado
                ws.averia = True
                self.evento(f"{id_sesion}: FUGA detectada por el Monitor")
                if ws.regando:
                    self.orden_engine(ws.id, "PARAR", motivo="FUGA")
                    self.terminar_riego(ws, "FUGA")
                self.actualizar(ws, anterior)
            return unir("OK", id_sesion), id_sesion

        if codigo == "RESUELTO":
            with self.lock:
                ws = self.ws[id_sesion]
                anterior = ws.estado
                ws.averia = False
                self.evento(f"{id_sesion}: avería resuelta")
                self.actualizar(ws, anterior)
            return unir("OK", id_sesion), id_sesion

        return unir("KO", "PETICION_DESCONOCIDA"), id_sesion

    def atender_monitor(self, conn, addr):
        id_ws = None
        log(f"Nueva conexión desde {addr}")
        try:
            conn.settimeout(TIMEOUT_MONITOR)
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
                if campos[0] != "VIVO":
                    log(f"Recibido de {id_ws or addr}: {campos}")
                respuesta, id_ws = self.procesar_monitor(campos, conn, id_ws)
                enviar_mensaje(conn, respuesta)
        except socket.timeout:
            log(f"{id_ws or addr} lleva {TIMEOUT_MONITOR} s sin dar señales")
        except (ConnectionError, OSError) as e:
            log(f"Conexión con {id_ws or addr} perdida: {e}")
        finally:
            conn.close()
            if id_ws:
                with self.lock:
                    if self.conexiones.get(id_ws) is conn:
                        del self.conexiones[id_ws]
                        ws = self.ws[id_ws]
                        anterior = ws.estado
                        ws.conectada = False
                        ws.reserva = None
                        if ws.regando:
                            self.orden_engine(ws.id, "PARAR", motivo="DESCONEXION")
                            self.terminar_riego(ws, "DESCONEXION")
                        self.actualizar(ws, anterior)

    def servidor_sockets(self):
        servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        servidor.bind(("0.0.0.0", self.puerto))
        servidor.listen()
        servidor.settimeout(1)  # para que Ctrl+C funcione en Windows
        log(f"Escuchando Monitores en el puerto {self.puerto}")
        try:
            while True:
                try:
                    conn, addr = servidor.accept()
                except socket.timeout:
                    continue
                threading.Thread(target=self.atender_monitor, args=(conn, addr), daemon=True).start()
        finally:
            servidor.close()

    # =====================================================
    # KAFKA: SOLICITUDES DE RIEGO
    # =====================================================

    def procesar_solicitud(self, msg):
        id_fo = msg.get("id_fo")
        id_ws = msg.get("id_ws")
        origen = msg.get("origen", "FO")
        duracion = msg.get("duracion")
        self.evento(f"Solicitud de riego de {id_fo} ({origen}) en {id_ws}"
                    f" ({f'{duracion} s' if duracion else 'sin límite'})")
        self.responder_fo(id_fo, "INFO", id_ws, texto="Solicitud recibida por CENTRAL. Comprobando la estación...")

        def denegar(motivo):
            self.evento(f"Solicitud de {id_fo} en {id_ws} DENEGADA: {motivo}")
            self.responder_fo(id_fo, "DENEGADO", id_ws, motivo=motivo)

        if origen == "FO" and not self.db.operador_existe(id_fo):
            return denegar("OPERADOR_NO_REGISTRADO")
        with self.lock:
            ws = self.ws.get(id_ws)
            if ws is None:
                return denegar("WS_NO_REGISTRADA")
            if ws.estado != DISPONIBLE:
                return denegar(f"WS_{ws.estado}")
            if ws.reserva:
                return denegar("WS_OCUPADA")
            ws.reserva = (id_fo, time.time())
        self.responder_fo(id_fo, "INFO", id_ws,
                          texto=f"{id_ws} disponible. Pidiendo a la estación que abra la electroválvula...")
        self.orden_engine(id_ws, "REGAR", id_fo=id_fo, duracion=duracion)

    def bucle_solicitudes(self):
        # Sin grupo: si la Central estaba caída, esas solicitudes ya han caducado
        # (el operador las ha dado por fallidas por timeout), así que no se reprocesan.
        consumidor = crear_consumidor(self.broker, TOPIC_SOLICITUDES)
        log("Escuchando solicitudes de riego (Kafka)")
        for _, msg in mensajes(consumidor):
            if msg["tipo"] == "SOLICITUD":
                try:
                    self.procesar_solicitud(msg)
                except Exception as e:  # un mensaje raro no debe tumbar la Central
                    log(f"Error procesando solicitud {msg}: {e}")

    # =====================================================
    # KAFKA: TELEMETRÍA DE LOS ENGINES
    # =====================================================

    def terminar_riego(self, ws, motivo, volumen=None, duracion=None):
        """Cierra el riego en curso (llamar con self.lock tomado)."""
        if volumen is None:
            volumen = ws.volumen
        if duracion is None:
            duracion = time.time() - (ws.inicio_riego or time.time())
        exito = motivo in FINALES_CORRECTOS
        self.evento(f"{ws.id}: riego de {ws.id_fo} terminado ({motivo}). "
                    f"{volumen:.1f} L en {duracion:.0f} s")
        self.db.guardar_riego(ws.id, ws.id_fo,
                              time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ws.inicio_riego or time.time())),
                              volumen, duracion, motivo)
        self.responder_fo(ws.id_fo, "FIN", ws.id, motivo=motivo, exito=exito,
                          volumen=round(volumen, 2), duracion=round(duracion, 1))
        ws.regando = False
        ws.caudal = 0.0

    def procesar_telemetria(self, msg):
        id_ws = msg.get("id_ws")
        tipo = msg["tipo"]
        with self.lock:
            ws = self.ws.get(id_ws)
            if ws is None:
                return
            anterior = ws.estado

            if tipo == "INICIO":
                ws.reserva = None
                ws.regando = True
                ws.id_fo = msg.get("id_fo")
                ws.volumen = ws.caudal = 0.0
                ws.inicio_riego = ws.ultima_telemetria = time.time()
                self.evento(f"{id_ws}: electroválvula abierta, riego de {ws.id_fo} iniciado")
                self.responder_fo(ws.id_fo, "AUTORIZADO", id_ws,
                                  texto=f"Riego autorizado. {id_ws} ha abierto la electroválvula")

            elif tipo == "RECHAZO":
                id_fo = ws.reserva[0] if ws.reserva else msg.get("id_fo")
                ws.reserva = None
                self.evento(f"{id_ws} rechaza el riego de {id_fo}: {msg.get('motivo')}")
                self.responder_fo(id_fo, "DENEGADO", id_ws, motivo=f"WS_RECHAZA_{msg.get('motivo')}")

            elif tipo == "RIEGO":
                if not ws.regando:
                    # La Central se ha reiniciado a mitad de un riego: lo recuperamos
                    ws.regando = True
                    ws.id_fo = msg.get("id_fo")
                    ws.inicio_riego = time.time() - msg.get("segundos", 0)
                ws.caudal = msg.get("caudal", 0.0)
                ws.volumen = msg.get("volumen", 0.0)
                ws.ultima_telemetria = time.time()
                self.responder_fo(ws.id_fo, "RIEGO", id_ws, caudal=ws.caudal,
                                  volumen=ws.volumen, segundos=msg.get("segundos"))

            elif tipo == "FIN":
                if msg.get("motivo") == "FUGA":
                    ws.averia = True  # el sensor está en el Engine: no esperamos al Monitor
                if ws.regando:
                    self.terminar_riego(ws, msg.get("motivo", "DESCONOCIDO"),
                                        msg.get("volumen"), msg.get("segundos"))
                else:
                    log(f"{id_ws}: FIN de un riego ya cerrado ({msg.get('motivo')})")

            self.actualizar(ws, anterior)

    def bucle_telemetria(self):
        # Con grupo: tras un reinicio se lee lo que mandaron los Engines mientras
        # tanto, para no perder ningún FIN y reconstruir los riegos en curso.
        consumidor = crear_consumidor(self.broker, TOPIC_TELEMETRIA, grupo="wm-central")
        log("Escuchando telemetría de los Engines (Kafka)")
        for _, msg in mensajes(consumidor):
            try:
                self.procesar_telemetria(msg)
            except Exception as e:
                log(f"Error procesando telemetría {msg}: {e}")

    # =====================================================
    # VIGILANCIA PERIÓDICA
    # =====================================================

    def vigilancia(self):
        ultimo_estado_red = 0
        while True:
            time.sleep(1)
            ahora = time.time()
            with self.lock:
                for ws in self.ws.values():
                    if ws.reserva and ahora - ws.reserva[1] > TIMEOUT_AUTORIZACION:
                        id_fo = ws.reserva[0]
                        ws.reserva = None
                        self.evento(f"{ws.id} no ha respondido a la orden de riego de {id_fo}")
                        self.responder_fo(id_fo, "DENEGADO", ws.id, motivo="LA_WS_NO_RESPONDE")
                    if ws.regando and ahora - ws.ultima_telemetria > TIMEOUT_TELEMETRIA:
                        anterior = ws.estado
                        self.orden_engine(ws.id, "PARAR", motivo="SIN_TELEMETRIA")
                        self.terminar_riego(ws, "SIN_TELEMETRIA")
                        self.actualizar(ws, anterior)
            if ahora - ultimo_estado_red >= PERIODO_ESTADO_RED:
                ultimo_estado_red = ahora
                self.publicar_estado_red()

    # =====================================================
    # MENÚ DE LA CENTRAL (punto 11)
    # =====================================================

    def objetivos(self, texto):
        with self.lock:
            if texto in ("*", "todas", "TODAS"):
                return sorted(self.ws)
            if texto in self.ws:
                return [texto]
        print(f"No existe la WS '{texto}'")
        return []

    def bloquear(self, id_ws):
        with self.lock:
            ws = self.ws[id_ws]
            anterior = ws.estado
            ws.bloqueada = True
            if ws.regando:
                self.orden_engine(id_ws, "PARAR", motivo="BLOQUEO")
                self.terminar_riego(ws, "BLOQUEO")
            self.evento(f"{id_ws}: bloqueada por CENTRAL (Fuera de Servicio)")
            self.actualizar(ws, anterior)
        self.orden_engine(id_ws, "BLOQUEAR")

    def activar(self, id_ws):
        with self.lock:
            ws = self.ws[id_ws]
            anterior = ws.estado
            ws.bloqueada = False
            self.evento(f"{id_ws}: activada por CENTRAL")
            self.actualizar(ws, anterior)
        self.orden_engine(id_ws, "ACTIVAR")

    def menu(self):
        ayuda = ("Comandos:  r <ws|*> [segundos]  iniciar riego  |  b <ws|*>  bloquear  |  "
                 "a <ws|*>  activar\n           o <id>  alta de operador  |  "
                 "l  listar operadores  |  p  panel  |  h  ayuda")
        print(ayuda, flush=True)
        while True:
            try:
                linea = input().strip()
            except EOFError:
                log("Sin consola interactiva: menú desactivado")
                return
            if not linea:
                continue
            partes = linea.split()
            cmd = partes[0].lower()
            if cmd == "p":
                self.mostrar_panel()
            elif cmd == "h":
                print(ayuda)
            elif cmd == "l":
                print("Operadores registrados:", ", ".join(self.db.operadores()))
            elif cmd == "o" and len(partes) == 2:
                if ID_VALIDO.match(partes[1]):
                    self.db.alta_operador(partes[1])
                    self.evento(f"Alta del operador {partes[1]}")
                else:
                    print("ID de operador no válido")
            elif cmd in ("r", "b", "a") and len(partes) >= 2:
                for id_ws in self.objetivos(partes[1]):
                    if cmd == "r":
                        duracion = int(partes[2]) if len(partes) > 2 and partes[2].isdigit() else None
                        self.procesar_solicitud({"id_fo": "CENTRAL", "id_ws": id_ws,
                                                 "origen": "CENTRAL", "duracion": duracion})
                    elif cmd == "b":
                        self.bloquear(id_ws)
                    else:
                        self.activar(id_ws)
            else:
                print("Comando no reconocido ('h' para ayuda)")

    # =====================================================
    # ARRANQUE
    # =====================================================

    def run(self):
        log(f"Arrancando. Broker: {self.broker}")
        self.mostrar_panel()
        # Los sockets primero: los Monitores pueden registrarse aunque Kafka tarde
        threading.Thread(target=self.servidor_sockets, daemon=True).start()
        asegurar_topics(self.broker)
        self.productor = crear_productor(self.broker)
        log("Conectada a Kafka")
        threading.Thread(target=self.bucle_solicitudes, daemon=True).start()
        threading.Thread(target=self.bucle_telemetria, daemon=True).start()
        threading.Thread(target=self.vigilancia, daemon=True).start()
        threading.Thread(target=self.menu, daemon=True).start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            log("Apagando Central")


def main():
    parser = argparse.ArgumentParser(description="WM_Central")
    parser.add_argument("puerto", type=int, help="Puerto de escucha para los Monitores")
    parser.add_argument("broker", help="IP:puerto del broker Kafka")
    parser.add_argument("--db", default="wm.db", help="Fichero SQLite (por defecto wm.db)")
    args = parser.parse_args()
    Central(args.puerto, args.broker, args.db).run()


if __name__ == "__main__":
    main()
