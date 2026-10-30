"""
engine_web.py - Panel web del Engine de la estación (WM_WS_E).

Se engancha a WM_WS_E.py con UNA línea en main(), justo después de crear el Engine y
ANTES de lanzar sus hilos:
        import engine_web; engine_web.arrancar(engine)

No cambia la lógica del Engine. Cómo lo hace:
  - envuelve engine.publicar() para ver el caudal y el volumen en vivo y el resumen final;
  - envuelve la función log() del módulo para copiar a la web los mensajes de la consola;
  - los botones hacen exactamente lo mismo que las teclas del menú de consola
    (K, O, R, F), con los mismos mensajes, así que la consola sigue siendo válida.

Puerto: variable de entorno WEB_PORT (8082 por defecto).
"""

import collections
import os
import sys
import threading
import time

import web_comun

PUERTO_POR_DEFECTO = 8082


def _hora():
    return time.strftime("%H:%M:%S")


class _Panel:
    def __init__(self, engine):
        self.e = engine
        self.mod = sys.modules[type(engine).__module__]     # el módulo donde vive WM_WS_E (normalmente __main__)
        self.lock = threading.RLock()
        self.eventos = collections.deque(maxlen=40)
        self.vivo = {"caudal": 0.0, "volumen": 0.0, "segundos": 0.0}
        self.ultimo_fin = None

        publicar_original = engine.publicar

        def publicar(tipo, **datos):
            try:
                self._ver(tipo, datos)
            except Exception:
                pass
            return publicar_original(tipo, **datos)

        engine.publicar = publicar          # el atributo de instancia tiene prioridad sobre el método

        log_original = self.mod.log

        def log(msg):
            try:
                with self.lock:
                    self.eventos.appendleft({"hora": _hora(), "texto": str(msg).strip("* ").strip()})
            except Exception:
                pass
            log_original(msg)

        self.mod.log = log                  # las funciones del módulo buscan `log` al llamarla
        self.log = log

    def _ver(self, tipo, d):
        with self.lock:
            if tipo == "INICIO":
                self.vivo = {"caudal": 0.0, "volumen": 0.0, "segundos": 0.0}
            elif tipo == "RIEGO":
                self.vivo = {"caudal": float(d.get("caudal") or 0), "volumen": float(d.get("volumen") or 0),
                             "segundos": float(d.get("segundos") or 0)}
            elif tipo == "FIN":
                self.ultimo_fin = {"id_fo": d.get("id_fo"), "motivo": d.get("motivo"),
                                   "volumen": float(d.get("volumen") or 0), "segundos": float(d.get("segundos") or 0)}
                self.vivo = {"caudal": 0.0, "volumen": 0.0, "segundos": 0.0}
            elif tipo == "RECHAZO":
                self.eventos.appendleft({"hora": _hora(), "texto": f"Riego de {d.get('id_fo')} rechazado ({d.get('motivo')})"})

    # ---------- acciones: lo mismo que las teclas del menú ----------
    def accion(self, datos):
        e, accion = self.e, datos.get("accion")
        if accion == "fuga":
            e.averia = True
            self.log("*** FUGA simulada: respondo KO al Monitor ***")
            e.detener_riego("FUGA")
            return 200, {"ok": True}
        if accion == "resolver":
            e.averia = False
            self.log("Fuga resuelta: respondo OK al Monitor")
            return 200, {"ok": True}
        if accion == "regar":
            if not e.id_ws or not e.productor:
                return 409, {"error": "Todavía no tengo ID o no estoy conectada a Kafka."}
            if e.riego:
                return 409, {"error": "Ya hay un riego en curso."}
            seg = datos.get("segundos")
            if seg is not None and (not isinstance(seg, int) or isinstance(seg, bool) or not 0 <= seg <= 3600):
                return 400, {"error": "La duración debe estar entre 0 y 3600 segundos."}
            from kafka_comun import TOPIC_SOLICITUDES, enviar
            self.log("Solicito a la Central un riego manual en esta estación")
            enviar(e.productor, TOPIC_SOLICITUDES,
                   {"tipo": "SOLICITUD", "origen": "WS", "id_fo": f"LOCAL-{e.id_ws}",
                    "id_ws": e.id_ws, "duracion": seg or None})
            return 202, {"ok": True}
        if accion == "parar":
            if not e.riego:
                return 409, {"error": "No hay ningún riego en curso."}
            e.detener_riego("FINALIZADO_EN_WS")
            return 200, {"ok": True}
        return 400, {"error": "Acción desconocida."}

    # ---------- lo que ve la página ----------
    def estado(self):
        e = self.e
        riego = e.riego
        if not e.id_ws:
            est = "SIN_ID"
        elif e.averia:
            est = "FUGA"
        elif e.bloqueada:
            est = "FUERA_SERVICIO"
        elif riego:
            est = "REGANDO"
        else:
            est = "DISPONIBLE"
        with self.lock:
            return {"id_ws": e.id_ws, "estado": est, "averia": bool(e.averia), "bloqueada": bool(e.bloqueada),
                    "kafka": e.productor is not None,
                    "riego": ({"id_fo": riego.get("id_fo"), "duracion": riego.get("duracion"), **self.vivo}
                              if riego else None),
                    "ultimo_fin": self.ultimo_fin, "eventos": list(self.eventos), "hora": _hora()}


def arrancar(engine, puerto=None):
    """Lanza el panel web del Engine en un hilo aparte."""
    puerto = puerto or int(os.environ.get("WEB_PORT", PUERTO_POR_DEFECTO))
    panel = _Panel(engine)
    pagina = _PLANTILLA.replace("/*ESTILO*/", web_comun.ESTILO + web_comun.ESTILO_EXTRA)
    return web_comun.servir(puerto, pagina, panel.estado, {"/api/accion": panel.accion}, nombre="ENGINE")


_PLANTILLA = r"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>WaterManagement - Engine de la estación</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>
/*ESTILO*/
</style>
</head>
<body>
<header class="barra">
  <div class="barra-in">
    <div class="marca">
      <span class="logo" aria-hidden="true">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2.5c3.6 4.3 6.5 7.6 6.5 11.2a6.5 6.5 0 0 1-13 0C5.5 10.1 8.4 6.8 12 2.5z"/></svg>
      </span>
      <div><strong>WaterManagement</strong><span class="sub" id="sub">Engine de la estaci&oacute;n</span></div>
    </div>
    <div class="indicadores">
      <span class="pildora" id="p-mon" data-ok="unknown"><i class="punto"></i><span id="t-mon">Esperando al Monitor</span></span>
      <span class="pildora" id="p-kafka" data-ok="unknown"><i class="punto"></i><span id="t-kafka">Kafka</span></span>
      <time class="hora" id="hora"></time>
    </div>
  </div>
</header>

<main class="pagina">
  <div id="aviso" role="alert">Sin conexi&oacute;n con el Engine. Reintentando...</div>

  <section class="panel" aria-labelledby="t-est">
    <div class="panel-cab"><h2 id="t-est">Estaci&oacute;n</h2></div>
    <div class="cols">
      <article class="tarjeta grande" id="tarjeta" data-estado="SIN_ID">
        <div class="agua" aria-hidden="true"></div>
        <h3 class="id" id="e-id">Sin ID</h3>
        <p class="ubi" id="e-sub">Esperando al Monitor</p>
        <div class="medidas" id="e-medidas" hidden>
          <p class="caudal"><b id="e-caudal">0.0</b><span>L/min</span></p>
          <p class="detalle" id="e-vol"></p>
          <p class="detalle" id="e-op"></p>
        </div>
        <p class="estado" id="e-estado"></p>
      </article>

      <div>
        <div class="acciones">
          <button class="btn peligro" id="b-fuga" type="button">Simular fuga</button>
          <button class="btn ok" id="b-resolver" type="button">Resolver fuga</button>
          <button class="btn sec" id="b-parar" type="button">Finalizar riego</button>
        </div>
        <form id="form-regar" novalidate style="margin-top: 24px; max-width: 340px;">
          <div class="campo">
            <label for="seg">Riego manual, duraci&oacute;n en segundos</label>
            <input id="seg" type="number" min="0" max="3600" step="1" value="10" inputmode="numeric">
            <p class="ayuda">Con 0 riega sin l&iacute;mite. La petici&oacute;n pasa por la Central, que la valida como la de cualquier operario.</p>
          </div>
          <button class="btn" id="b-regar" type="submit">Regar</button>
        </form>
        <p class="retro" id="retro" role="status"></p>
      </div>
    </div>
  </section>

  <section class="panel" aria-labelledby="t-ev">
    <div class="panel-cab"><h2 id="t-ev">Mensajes de la estaci&oacute;n</h2></div>
    <ul class="lineas" id="lineas"><li class="vacia">Aqu&iacute; saldr&aacute;n los mensajes del Engine.</li></ul>
  </section>
  <p class="pie">La p&aacute;gina se actualiza sola cada segundo. Los botones hacen lo mismo que las teclas K, O, R y F de la consola, que sigue funcionando.</p>
</main>

<script>
const ESTADOS = {
  DISPONIBLE: "Disponible", REGANDO: "Regando", FUGA: "Fuga",
  FUERA_SERVICIO: "Fuera de Servicio", SIN_ID: "Esperando al Monitor"
};
const CAUDAL_MAX = 20;
const $ = id => document.getElementById(id);
function el(tag, clase, texto) {
  const e = document.createElement(tag);
  if (clase) e.className = clase;
  if (texto !== undefined) e.textContent = texto;
  return e;
}

function pintarLineas(eventos) {
  const ul = $("lineas");
  ul.replaceChildren();
  if (!eventos.length) { ul.append(el("li", "vacia", "Aquí saldrán los mensajes del Engine.")); return; }
  for (const e of eventos) {
    const li = el("li");
    li.append(el("time", "", e.hora), document.createTextNode(e.texto));
    ul.append(li);
  }
}

function pintar(d) {
  const t = $("tarjeta");
  t.dataset.estado = d.estado === "SIN_ID" ? "DESCONECTADA" : d.estado;
  $("e-id").textContent = d.id_ws || "Sin ID";
  $("e-estado").textContent = ESTADOS[d.estado] || d.estado;
  const r = d.riego;
  $("e-medidas").hidden = !r;
  if (r) {
    $("e-caudal").textContent = r.caudal.toFixed(1);
    $("e-vol").textContent = "Volumen: " + r.volumen.toFixed(1) + " L";
    $("e-op").textContent = "Para " + r.id_fo + ", " + Math.round(r.segundos) + " s" + (r.duracion ? " de " + r.duracion : " (sin límite)");
    t.style.setProperty("--nivel", Math.min(Math.max(r.caudal / CAUDAL_MAX, 0.12), 0.92).toFixed(2));
    $("e-sub").textContent = "Electroválvula abierta";
  } else if (d.estado === "FUGA") {
    $("e-sub").textContent = "Respondiendo KO al Monitor";
  } else if (d.estado === "FUERA_SERVICIO") {
    $("e-sub").textContent = "Bloqueada por la Central";
  } else if (d.estado === "SIN_ID") {
    $("e-sub").textContent = "Arranca el Monitor de esta estación";
  } else {
    const f = d.ultimo_fin;
    $("e-sub").textContent = f ? "Último riego: " + f.volumen.toFixed(1) + " L en " + Math.round(f.segundos) + " s (" + f.motivo + ")"
                               : "Electroválvula cerrada, esperando órdenes";
  }
  $("p-mon").dataset.ok = d.id_ws ? "true" : "unknown";
  $("t-mon").textContent = d.id_ws ? "Monitor: " + d.id_ws : "Esperando al Monitor";
  $("p-kafka").dataset.ok = String(d.kafka);
  $("t-kafka").textContent = d.kafka ? "Kafka conectado" : "Kafka sin conexión";
  $("sub").textContent = d.id_ws ? "Engine de " + d.id_ws : "Engine de la estación";
  $("hora").textContent = d.hora;
  $("b-fuga").disabled = d.averia || !d.id_ws;
  $("b-resolver").disabled = !d.averia;
  $("b-parar").disabled = !r;
  $("b-regar").disabled = !d.id_ws || !d.kafka || !!r;
  pintarLineas(d.eventos);
}

async function accion(cuerpo) {
  const retro = $("retro");
  retro.className = "retro"; retro.textContent = "";
  try {
    const r = await fetch("/api/accion", {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(cuerpo)});
    const j = await r.json();
    if (!r.ok) { retro.className = "retro error"; retro.textContent = j.error || "No se pudo hacer."; }
  } catch (e) {
    retro.className = "retro error"; retro.textContent = "Sin conexión con el Engine.";
  }
  actualizar();
}
$("b-fuga").addEventListener("click", () => accion({accion: "fuga"}));
$("b-resolver").addEventListener("click", () => accion({accion: "resolver"}));
$("b-parar").addEventListener("click", () => accion({accion: "parar"}));
$("form-regar").addEventListener("submit", ev => {
  ev.preventDefault();
  const txt = $("seg").value.trim();
  if (!/^[0-9]{1,4}$/.test(txt)) { $("retro").className = "retro error"; $("retro").textContent = "Escribe la duración en segundos (0 para sin límite)."; return; }
  accion({accion: "regar", segundos: parseInt(txt, 10)});
});

async function actualizar() {
  try {
    const r = await fetch("/api/estado", {cache: "no-store"});
    if (!r.ok) throw new Error(r.status);
    pintar(await r.json());
    $("aviso").style.display = "none";
  } catch (e) {
    $("aviso").style.display = "block";
    $("p-mon").dataset.ok = "false"; $("t-mon").textContent = "Sin conexión";
  }
}
actualizar();
setInterval(actualizar, 1000);
</script>
</body>
</html>
"""
