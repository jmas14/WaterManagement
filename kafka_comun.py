"""
kafka_comun - Utilidades de Kafka compartidas por todos los módulos.

Topics (1 partición cada uno, así se respeta el orden de los mensajes):
  wm.solicitudes  FO / WS / Central -> Central   peticiones de riego
  wm.respuestas   Central -> FO                   avisos, autorización, telemetría y resumen
  wm.ordenes      Central -> Engine               REGAR / PARAR / BLOQUEAR / ACTIVAR
  wm.telemetria   Engine -> Central               INICIO / RECHAZO / RIEGO / FIN
  wm.estado_red   Central -> FO                   foto de todas las WS (para 'lista')

Todos los mensajes son JSON con un campo "tipo". La clave del mensaje es el
destinatario (id_fo o id_ws), y cada consumidor ignora lo que no es para él.
"""

import json
import time

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

TOPIC_SOLICITUDES = "wm.solicitudes"
TOPIC_RESPUESTAS = "wm.respuestas"
TOPIC_ORDENES = "wm.ordenes"
TOPIC_TELEMETRIA = "wm.telemetria"
TOPIC_ESTADO_RED = "wm.estado_red"
TODOS_LOS_TOPICS = [TOPIC_SOLICITUDES, TOPIC_RESPUESTAS, TOPIC_ORDENES,
                    TOPIC_TELEMETRIA, TOPIC_ESTADO_RED]

REINTENTO = 3  # segundos entre intentos de conexión con el broker


def _log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] [KAFKA] {msg}", flush=True)


def asegurar_topics(broker):
    """Crea los topics si no existen. Reintenta hasta que el broker responda."""
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=broker, client_id="wm-admin")
            existentes = set(admin.list_topics())
            nuevos = [NewTopic(t, num_partitions=1, replication_factor=1)
                      for t in TODOS_LOS_TOPICS if t not in existentes]
            if nuevos:
                try:
                    admin.create_topics(nuevos)
                except TopicAlreadyExistsError:
                    pass  # otro módulo los ha creado a la vez
            admin.close()
            return
        except KafkaError as e:
            _log(f"Broker {broker} no disponible ({e.__class__.__name__}). Reintento en {REINTENTO} s")
            time.sleep(REINTENTO)


def crear_productor(broker):
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=broker,
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                max_block_ms=5000,   # si Kafka cae, no nos quedamos colgados en send()
                linger_ms=0,
            )
        except KafkaError as e:
            _log(f"No puedo crear el productor ({e.__class__.__name__}). Reintento en {REINTENTO} s")
            time.sleep(REINTENTO)


def crear_consumidor(broker, *topics, grupo=None):
    """Sin grupo: solo lee lo que llega a partir de ahora (latest).
    Con grupo: Kafka guarda por dónde iba, así que tras un reinicio sigue
    leyendo lo que se publicó mientras estaba caído."""
    while True:
        try:
            consumidor = KafkaConsumer(
                *topics,
                bootstrap_servers=broker,
                group_id=grupo,
                auto_offset_reset="latest",
                enable_auto_commit=grupo is not None,
                value_deserializer=_deserializar,
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
            )
            # Forzamos la asignación de particiones antes de devolverlo: si no,
            # un mensaje que llegue en los primeros instantes se perdería.
            inicio = time.time()
            while not consumidor.assignment() and time.time() - inicio < 15:
                consumidor.poll(timeout_ms=500)
            return consumidor
        except KafkaError as e:
            _log(f"No puedo crear el consumidor ({e.__class__.__name__}). Reintento en {REINTENTO} s")
            time.sleep(REINTENTO)


def _deserializar(valor):
    try:
        return json.loads(valor.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None  # mensaje mal formado: se ignora


def enviar(productor, topic, mensaje, clave=None):
    """Publica un mensaje. Devuelve False si Kafka no está disponible."""
    mensaje = dict(mensaje, ts=time.time())
    try:
        productor.send(topic, key=clave, value=mensaje)
        productor.flush(timeout=5)
        return True
    except KafkaError as e:
        _log(f"No se pudo publicar en {topic}: {e.__class__.__name__}")
        return False


def mensajes(consumidor):
    """Itera sobre los mensajes válidos (descarta los mal formados)."""
    for registro in consumidor:
        if isinstance(registro.value, dict) and "tipo" in registro.value:
            yield registro.key, registro.value
