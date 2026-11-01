"""
operario_web.py - Aplicación web del operario (WM_FO).

Se engancha a WM_FO.py con UNA línea en main(), justo después de crear el Operador:
        import operario_web; operario_web.arrancar(operador)

No cambia nada de la lógica de WM_FO: la consola, el modo fichero y los tiempos
(4 s entre servicios, 10 s sin noticias de la Central) siguen igual.
Cómo se entera de lo que pasa: cambia la cola `operador.respuestas` por una que, además de
guardar el mensaje para `solicitar()`, avisa a la web. Por eso la web ve los servicios
pedidos desde la consola y los pedidos desde la web.

Puerto: variable de entorno WEB_PORT (8081 por defecto). Con varios operarios en el
mismo PC, cada uno necesita un WEB_PORT distinto.
"""

import collections
import os
import queue
import re
import tempfile
import threading
import time

import web_comun

PUERTO_POR_DEFECTO = 8081
TIMEOUT_SIN_NOTICIAS = 12     # s; WM_FO se rinde a los 10 s, aquí damos 2 s de margen
RE_ID = re.compile(r"^[A-Za-z0-9_-]{1,20}$")
RE_LINEA = re.compile(r"^[A-Za-z0-9_-]{1,20}(#[0-9]{1,4})?$")

MOTIVOS = {
    "LIMITE_TIEMPO": "se cumplió el tiempo programado",
    "FINALIZADO_EN_WS": "riego detenido",
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
    "OPERADOR_NO_REGISTRADO": "el operador no está registrado: pulsa «Registrarme»",
}


def _texto_motivo(m):
    """Frase natural para el código de motivo de la Central."""
    if m and m.startswith("WS_RECHAZA_"):
        return "la estación rechazó el riego"
    return MOTIVOS.get(m, (m or "").replace("_", " ").lower())


def _hora():
    return time.strftime("%H:%M:%S")


class _ColaObservada(queue.Queue):
    """Igual que queue.Queue, pero cada mensaje que entra también se le cuenta a la web."""

    def __init__(self, seguimiento):
        super().__init__()
        self._seguimiento = seguimiento

    def put(self, item, *args, **kwargs):
        try:
            self._seguimiento.registrar(item)
        except Exception:
            pass    # la web nunca debe impedir que el operario reciba sus mensajes
        super().put(item, *args, **kwargs)


class _Seguimiento:
    def __init__(self, operador):
        self.op = operador
        self.lock = threading.RLock()
        self.servicio = None
        self.eventos = collections.deque(maxlen=40)
        self.fichero_activo = False
        self.registro = None      # último resultado del registro en la Central

    # ---------- lo que llega de la Central ----------
    def _evento(self, texto):
        self.eventos.appendleft({"hora": _hora(), "texto": texto})

    def _nuevo(self, id_ws, duracion=None):
        self.servicio = {"id_ws": id_ws, "duracion": duracion, "fase": "SOLICITANDO", "terminado": False,
                         "resultado": None, "motivo": None, "caudal": 0.0, "volumen": 0.0,
                         "segundos": 0.0, "ultimo": time.time()}
        return self.servicio

    def registrar(self, msg):
        tipo, id_ws = msg.get("tipo"), msg.get("id_ws")
        if tipo in ("REGISTRO_OK", "REGISTRO_KO"):
            with self.lock:
                if tipo == "REGISTRO_OK":
                    texto = ("Operador registrado en la Central" if msg.get("nuevo")
                             else "El operador ya estaba registrado en la Central")
                else:
                    texto = "Registro rechazado: " + _texto_motivo(msg.get("motivo"))
                self.registro = {"ok": tipo == "REGISTRO_OK", "texto": texto}
                self._evento(texto)
            return
        with self.lock:
            s = self.servicio
            if tipo in ("INFO", "AUTORIZADO", "DENEGADO") and (s is None or s["terminado"] or s["id_ws"] != id_ws):
                s = self._nuevo(id_ws)          # servicio pedido desde la consola o desde el fichero
            if s is None or s["id_ws"] != id_ws:
                return
            s["ultimo"] = time.time()
            if tipo == "INFO":
                s["fase"] = "COMPROBANDO"
                self._evento(f"{id_ws}: {msg.get('texto', '')}")
            elif tipo == "AUTORIZADO":
                s["fase"] = "AUTORIZADO"
                self._evento(f"{id_ws}: autorizado. {msg.get('texto', '')}")
            elif tipo == "AVISO":
                self._evento(f"{id_ws}: {msg.get('texto', '')}")
            elif tipo == "RIEGO" and not s["terminado"]:
                s["fase"] = "REGANDO"
                s["caudal"] = float(msg.get("caudal") or 0)
                s["volumen"] = float(msg.get("volumen") or 0)
                s["segundos"] = float(msg.get("segundos") or 0)
            elif tipo == "DENEGADO":
                s.update(terminado=True, resultado="DENEGADO", motivo=msg.get("motivo"),
                         motivo_texto=_texto_motivo(msg.get("motivo")))
                self._evento(f"{id_ws}: riego denegado, {_texto_motivo(msg.get('motivo'))}")
            elif tipo == "FIN":
                s.update(terminado=True, resultado="COMPLETADO" if msg.get("exito") else "INTERRUMPIDO",
                         motivo=msg.get("motivo"), motivo_texto=_texto_motivo(msg.get("motivo")),
                         volumen=float(msg.get("volumen") or 0),
                         segundos=float(msg.get("duracion") or 0))
                if msg.get("motivo") == "FINALIZADO_EN_WS":
                    self._evento(f"{id_ws}: riego detenido. {s['volumen']:.1f} L en {s['segundos']:.0f} s")
                else:
                    self._evento(f"{id_ws}: riego {'finalizado' if msg.get('exito') else 'interrumpido'}, "
                                 f"{_texto_motivo(msg.get('motivo'))}. {s['volumen']:.1f} L en {s['segundos']:.0f} s")

    def _caducar(self):
        """Si la Central lleva más de TIMEOUT_SIN_NOTICIAS sin decir nada, el servicio ha fallado."""
        s = self.servicio
        if s and not s["terminado"] and time.time() - s["ultimo"] > TIMEOUT_SIN_NOTICIAS:
            s.update(terminado=True, resultado="FALLIDO", motivo="SIN_NOTICIAS")
            self._evento(f"{s['id_ws']}: sin noticias de la Central, servicio fallido")

    def _ocupado(self):
        self._caducar()
        return self.fichero_activo or (self.servicio is not None and not self.servicio["terminado"])

    # ---------- acciones desde la web ----------
    def solicitar(self, datos):
        id_ws, duracion = str(datos.get("id_ws", "")).strip(), datos.get("duracion", 10)
        if not RE_ID.match(id_ws):
            return 400, {"error": "Elige una estación válida."}
        if not isinstance(duracion, int) or isinstance(duracion, bool) or not 0 <= duracion <= 3600:
            return 400, {"error": "La duración debe ser un número de segundos entre 0 y 3600."}
        with self.lock:
            if self._ocupado():
                return 409, {"error": "Ya hay un servicio en curso. Espera a que termine."}
            self._nuevo(id_ws, duracion)
            self._evento(f"{id_ws}: solicito riego ({f'{duracion} s' if duracion else 'sin límite'})")
        threading.Thread(target=self.op.solicitar, args=(id_ws, duracion), daemon=True).start()
        return 202, {"ok": True}

    def parar(self, datos):
        """Botón «Parar riego»: pide a la Central que detenga el riego en curso de este operario."""
        with self.lock:
            s = self.servicio
            if not s or s["terminado"] or s["fase"] not in ("AUTORIZADO", "REGANDO"):
                return 409, {"error": "No hay ningún riego en curso que parar."}
            id_ws = s["id_ws"]
            self._evento(f"{id_ws}: pido detener el riego")
        threading.Thread(target=self.op.detener, args=(id_ws,), daemon=True).start()
        return 202, {"ok": True}

    def registrar_operador(self, datos):
        with self.lock:
            if self._ocupado():
                return 409, {"error": "Ya hay un servicio en curso. Espera a que termine."}
            self.registro = {"ok": None, "texto": "Registrando en la Central..."}
            self._evento("Solicito el registro del operador en la Central")
        threading.Thread(target=self.op.registrar, daemon=True).start()
        return 202, {"ok": True}

    def fichero(self, datos):
        lineas = []
        for n, linea in enumerate(str(datos.get("texto", "")).splitlines(), 1):
            linea = linea.strip()
            if not linea or linea.startswith("#"):
                continue
            if not RE_LINEA.match(linea):
                return 400, {"error": f"La línea {n} no es válida: usa WS-01 o WS-01#15."}
            lineas.append(linea)
        if not lineas:
            return 400, {"error": "Escribe al menos un servicio."}
        if len(lineas) > 50:
            return 400, {"error": "Máximo 50 servicios."}
        with self.lock:
            if self._ocupado():
                return 409, {"error": "Ya hay un servicio en curso. Espera a que termine."}
            self.fichero_activo = True
            self._evento(f"Fichero de servicios: {len(lineas)} servicio(s)")
        threading.Thread(target=self._ejecutar_fichero, args=(lineas,), daemon=True).start()
        return 202, {"ok": True}

    def _ejecutar_fichero(self, lineas):
        ruta = None
        try:
            with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8") as f:
                f.write("\n".join(lineas) + "\n")
                ruta = f.name
            self.op.modo_fichero(ruta)          # la lógica de siempre: pide, espera el final, 4 s, siguiente
        except Exception as e:
            with self.lock:
                self._evento(f"Fichero de servicios: error ({e})")
        finally:
            with self.lock:
                self.fichero_activo = False
                self._evento("Fichero de servicios terminado")
            if ruta:
                try:
                    os.remove(ruta)
                except OSError:
                    pass

    # ---------- lo que ve la página ----------
    def estado(self):
        estaciones = []
        for w in list(self.op.estaciones):
            try:
                estaciones.append({"id": str(w["id"]), "ubicacion": str(w.get("ubicacion", "")),
                                   "estado": str(w.get("estado", ""))})
            except (KeyError, AttributeError, TypeError):
                continue
        with self.lock:
            self._caducar()
            return {"id_fo": self.op.id_fo, "estaciones": estaciones,
                    "servicio": dict(self.servicio) if self.servicio else None,
                    "eventos": list(self.eventos), "fichero": self.fichero_activo, "hora": _hora(),
                    "registro": dict(self.registro) if self.registro else None}


def arrancar(operador, puerto=None):
    """Lanza la web del operario en un hilo aparte."""
    puerto = puerto or int(os.environ.get("WEB_PORT", PUERTO_POR_DEFECTO))
    seguimiento = _Seguimiento(operador)
    operador.respuestas = _ColaObservada(seguimiento)
    pagina = _PLANTILLA.replace("/*ESTILO*/", web_comun.ESTILO + web_comun.ESTILO_EXTRA)
    return web_comun.servir(puerto, pagina, seguimiento.estado,
                            {"/api/solicitar": seguimiento.solicitar, "/api/fichero": seguimiento.fichero,
                             "/api/registro": seguimiento.registrar_operador,
                             "/api/parar": seguimiento.parar},
                            nombre="FO")


_PLANTILLA = r"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>WaterManagement - Operario</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>
/*ESTILO*/
.fila-botones { display: flex; flex-wrap: wrap; gap: 12px; align-items: center; }
</style>
</head>
<body>
<header class="barra">
  <div class="barra-in">
    <div class="marca">
      <span class="logo" aria-hidden="true">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2.5c3.6 4.3 6.5 7.6 6.5 11.2a6.5 6.5 0 0 1-13 0C5.5 10.1 8.4 6.8 12 2.5z"/></svg>
      </span>
      <div><strong>WaterManagement</strong><span class="sub" id="sub">Aplicación del operario</span></div>
    </div>
    <div class="indicadores">
      <span class="pildora" id="p-op" data-ok="unknown"><i class="punto"></i><span id="t-op">Conectando</span></span>
      <span class="pildora" id="p-red" data-ok="unknown"><i class="punto"></i><span id="t-red">Esperando la red</span></span>
      <time class="hora" id="hora"></time>
    </div>
  </div>
</header>

<main class="pagina">
  <div id="aviso" role="alert">Sin conexi&oacute;n con la aplicaci&oacute;n del operario. Reintentando...</div>

  <section class="panel" aria-labelledby="t-registro">
    <div class="panel-cab"><h2 id="t-registro">Registro del operador</h2></div>
    <form id="form-registro" novalidate>
      <p class="ayuda" id="reg-ayuda">Para pedir riegos, tu operador tiene que estar registrado en la Central.</p>
      <button class="btn sec" id="registrar" type="submit">Registrarme</button>
      <p class="retro" id="reg-estado" role="status"></p>
    </form>
  </section>

  <section class="panel" aria-labelledby="t-pedir">
    <div class="panel-cab"><h2 id="t-pedir">Pedir un riego</h2></div>
    <div class="cols">
      <form id="form" novalidate>
        <div class="campo">
          <label for="ws">Estaci&oacute;n</label>
          <select id="ws"><option value="">Esperando estaciones...</option></select>
        </div>
        <div class="campo">
          <label for="seg">Duraci&oacute;n en segundos</label>
          <input id="seg" type="number" min="0" max="3600" step="1" value="10" inputmode="numeric">
          <p class="ayuda">Con 0 riega sin l&iacute;mite, hasta que se pare desde la estaci&oacute;n.</p>
        </div>
        <div class="fila-botones">
          <button class="btn" id="pedir" type="submit">Pedir riego</button>
          <button class="btn peligro" id="parar" type="button" disabled>Parar riego</button>
        </div>
        <p class="retro" id="retro" role="status"></p>
      </form>
      <div>
        <p class="vacio" id="sin-servicio">Todav&iacute;a no has pedido ning&uacute;n riego.</p>
        <article class="tarjeta grande" id="servicio" hidden>
          <div class="agua" aria-hidden="true"></div>
          <h3 class="id" id="s-id"></h3>
          <p class="ubi" id="s-fase"></p>
          <div class="medidas" id="s-medidas">
            <p class="caudal"><b id="s-caudal">0.0</b><span>L/min</span></p>
            <p class="detalle" id="s-vol"></p>
            <p class="detalle" id="s-seg"></p>
          </div>
          <p class="estado" id="s-estado"></p>
        </article>
      </div>
    </div>
  </section>

  <section class="panel" aria-labelledby="t-red-h">
    <div class="panel-cab">
      <h2 id="t-red-h">Estaciones de la red</h2>
      <ul class="leyenda" id="leyenda" aria-label="Recuento por estado"></ul>
    </div>
    <div id="rejilla"><p class="vacio">Esperando el estado de la red. La Central lo publica cada 5 segundos.</p></div>
  </section>

  <section class="panel" aria-labelledby="t-fichero">
    <div class="panel-cab"><h2 id="t-fichero">Servicios desde fichero</h2></div>
    <form id="form-fichero" novalidate>
      <div class="campo">
        <label for="texto">Un servicio por l&iacute;nea</label>
        <textarea id="texto" spellcheck="false" placeholder="WS-01&#10;WS-02#15&#10;# las l&iacute;neas que empiezan por # se ignoran"></textarea>
        <p class="ayuda">Sin especificar tiempo, el riego dura 10 segundos. Con WS-02#15 dura 15.</p>
      </div>
      <div class="fila-botones">
        <button class="btn sec" id="lanzar" type="submit">Ejecutar servicios</button>
        <button class="btn" id="subir" type="button">Subir fichero .txt y ejecutar</button>
        <input id="archivo" type="file" accept=".txt,text/plain" hidden>
      </div>
      <p class="retro" id="retro-fichero" role="status"></p>
    </form>
  </section>

  <section class="panel" aria-labelledby="t-seg">
    <div class="panel-cab"><h2 id="t-seg">Seguimiento</h2></div>
    <ul class="lineas" id="lineas"><li class="vacia">Aqu&iacute; saldr&aacute;n los pasos de cada riego.</li></ul>
  </section>
  <p class="pie">La p&aacute;gina se actualiza sola cada segundo. La consola del operario sigue funcionando igual.</p>
</main>

<script>
const ESTADOS = {
  DISPONIBLE: ["Disponible", "var(--disponible)"], REGANDO: ["Regando", "var(--regando)"],
  FUGA: ["Fuga", "var(--fuga)"], FUERA_SERVICIO: ["Fuera de Servicio", "var(--fuera)"],
  DESCONECTADA: ["Desconectada", "var(--desconectada)"]
};
const CAUDAL_MAX = 20;
const $ = id => document.getElementById(id);
function el(tag, clase, texto) {
  const e = document.createElement(tag);
  if (clase) e.className = clase;
  if (texto !== undefined) e.textContent = texto;      // textContent: nada de lo recibido se interpreta como HTML
  return e;
}

/* ---------- estaciones ---------- */
const tarjetas = new Map();
let seleccion = "";

function crearTarjeta(id) {
  const t = el("article", "tarjeta compacta");
  t.tabIndex = 0; t.setAttribute("role", "button");
  const agua = el("div", "agua"); agua.setAttribute("aria-hidden", "true");
  const eId = el("h3", "id"), ubi = el("p", "ubi"), estado = el("p", "estado");
  t.append(agua, eId, ubi, estado);
  const elegir = () => { seleccion = id; $("ws").value = id; marcarSeleccion(); $("seg").focus(); };
  t.addEventListener("click", elegir);
  t.addEventListener("keydown", ev => { if (ev.key === "Enter" || ev.key === " ") { ev.preventDefault(); elegir(); } });
  return {el: t, id: eId, ubi, estado};
}
function marcarSeleccion() {
  for (const [id, r] of tarjetas) r.el.setAttribute("aria-pressed", String(id === seleccion));
}

function pintarEstaciones(estaciones) {
  const rejilla = $("rejilla");
  rejilla.querySelector(".vacio")?.remove();
  const vistos = new Set();
  estaciones.forEach((e, i) => {
    vistos.add(e.id);
    if (!tarjetas.has(e.id)) tarjetas.set(e.id, crearTarjeta(e.id));
    const r = tarjetas.get(e.id);
    r.el.dataset.estado = e.estado;
    r.id.textContent = e.id; r.ubi.textContent = e.ubicacion;
    r.estado.textContent = (ESTADOS[e.estado] || [e.estado])[0];
    r.el.style.setProperty("--nivel", "0.4");
    if (rejilla.children[i] !== r.el) rejilla.insertBefore(r.el, rejilla.children[i] || null);
  });
  for (const [id, r] of tarjetas) if (!vistos.has(id)) { r.el.remove(); tarjetas.delete(id); }
  if (!estaciones.length) rejilla.append(el("p", "vacio", "Esperando el estado de la red. La Central lo publica cada 5 segundos."));
  marcarSeleccion();

  const ley = $("leyenda"); ley.replaceChildren();
  for (const [clave, [nombre, color]] of Object.entries(ESTADOS)) {
    const n = estaciones.filter(e => e.estado === clave).length;
    const li = el("li"), punto = el("i");
    punto.style.background = color;
    li.append(punto, document.createTextNode(nombre + " "), el("b", "", String(n)));
    ley.append(li);
  }
  $("p-red").dataset.ok = estaciones.length ? "true" : "unknown";
  $("t-red").textContent = estaciones.length ? "Red recibida" : "Esperando la red";
}

let firmaSelect = "";
function pintarSelect(estaciones) {
  const sel = $("ws");
  const firma = estaciones.map(e => e.id).join(",");
  if (firma !== firmaSelect) {                       // solo se reconstruye si cambia la lista, para no cerrar el desplegable
    firmaSelect = firma;
    const actual = seleccion || sel.value;
    sel.replaceChildren();
    if (!estaciones.length) sel.append(new Option("Esperando estaciones...", ""));
    for (const e of estaciones) sel.append(new Option("", e.id));
    if (actual && estaciones.some(e => e.id === actual)) sel.value = actual;
  }
  for (const o of sel.options) {
    const e = estaciones.find(x => x.id === o.value);
    if (e) o.textContent = e.id + " - " + e.ubicacion + " (" + (ESTADOS[e.estado] || [e.estado])[0] + ")";
  }
  if (sel.value) seleccion = sel.value;
}

/* ---------- servicio en curso ---------- */
function vista(s) {
  if (!s.terminado) {
    if (s.fase === "REGANDO") return ["REGANDO", "Riego en curso", "Regando"];
    if (s.fase === "AUTORIZADO") return ["PENDIENTE", "Autorizado, abriendo la electroválvula", "Autorizado"];
    return ["PENDIENTE", "Comprobando la estación...", "Esperando a la Central"];
  }
  const m = s.motivo_texto ? ", " + s.motivo_texto : "";
  if (s.motivo === "FINALIZADO_EN_WS" && s.resultado === "COMPLETADO") return ["DISPONIBLE", "Riego detenido", "Riego detenido"];
  switch (s.resultado) {
    case "COMPLETADO":   return ["DISPONIBLE", "Riego finalizado" + m, "Finalizado"];
    case "INTERRUMPIDO": return ["FUERA_SERVICIO", "Riego interrumpido" + m, "Interrumpido"];
    case "DENEGADO":     return ["FUGA", "Riego denegado" + m, "Denegado"];
    default:             return ["DESCONECTADA", "Sin noticias de la Central", "Servicio fallido"];
  }
}
function pintarServicio(s) {
  $("sin-servicio").hidden = !!s;
  $("servicio").hidden = !s;
  if (!s) return;
  const [estado, texto, etiqueta] = vista(s);
  const t = $("servicio");
  t.dataset.estado = estado;
  $("s-id").textContent = s.id_ws;
  $("s-fase").textContent = texto;
  $("s-estado").textContent = etiqueta;
  const mide = s.fase === "REGANDO" || s.resultado === "COMPLETADO" || s.resultado === "INTERRUMPIDO";
  $("s-medidas").hidden = !mide;
  $("s-caudal").textContent = s.caudal.toFixed(1);
  $("s-vol").textContent = "Volumen: " + s.volumen.toFixed(1) + " L";
  $("s-seg").textContent = "Tiempo: " + Math.round(s.segundos) + " s" + (s.duracion ? " de " + s.duracion : "");
  t.style.setProperty("--nivel", Math.min(Math.max(s.caudal / CAUDAL_MAX, 0.12), 0.92).toFixed(2));
}

function pintarLineas(eventos) {
  const ul = $("lineas");
  ul.replaceChildren();
  if (!eventos.length) { ul.append(el("li", "vacia", "Aquí saldrán los pasos de cada riego.")); return; }
  for (const e of eventos) {
    const li = el("li"), h = el("time", "", e.hora);
    li.append(h, document.createTextNode(e.texto));
    ul.append(li);
  }
}

/* ---------- acciones ---------- */
async function enviar(url, cuerpo, retro) {
  retro.className = "retro"; retro.textContent = "Enviando...";
  try {
    const r = await fetch(url, {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(cuerpo)});
    const j = await r.json();
    if (!r.ok) { retro.className = "retro error"; retro.textContent = j.error || "No se pudo enviar."; return false; }
    retro.textContent = "Petición enviada.";
    setTimeout(() => { if (retro.textContent === "Petición enviada.") retro.textContent = ""; }, 3000);
    return true;
  } catch (e) {
    retro.className = "retro error"; retro.textContent = "Sin conexión con la aplicación del operario.";
    return false;
  }
}
$("ws").addEventListener("change", () => { seleccion = $("ws").value; marcarSeleccion(); });
$("form").addEventListener("submit", async ev => {
  ev.preventDefault();
  const ws = $("ws").value;
  const txt = $("seg").value.trim();
  if (!ws) { $("retro").className = "retro error"; $("retro").textContent = "Elige una estación."; return; }
  if (!/^[0-9]{1,4}$/.test(txt)) { $("retro").className = "retro error"; $("retro").textContent = "Escribe la duración en segundos (0 para sin límite)."; return; }
  await enviar("/api/solicitar", {id_ws: ws, duracion: parseInt(txt, 10)}, $("retro"));
});
$("parar").addEventListener("click", async () => {
  await enviar("/api/parar", {}, $("retro"));
});
$("form-registro").addEventListener("submit", async ev => {
  ev.preventDefault();
  await enviar("/api/registro", {}, $("reg-estado"));
});
$("subir").addEventListener("click", () => $("archivo").click());
$("archivo").addEventListener("change", async () => {
  const f = $("archivo").files[0];
  $("archivo").value = "";
  const retro = $("retro-fichero");
  if (!f) return;
  if (f.size > 60000) { retro.className = "retro error"; retro.textContent = "El fichero es demasiado grande."; return; }
  const texto = await f.text();
  $("texto").value = texto;
  await enviar("/api/fichero", {texto}, retro);
});
$("form-fichero").addEventListener("submit", async ev => {
  ev.preventDefault();
  await enviar("/api/fichero", {texto: $("texto").value}, $("retro-fichero"));
});

/* ---------- bucle ---------- */
async function actualizar() {
  try {
    const r = await fetch("/api/estado", {cache: "no-store"});
    if (!r.ok) throw new Error(r.status);
    const d = await r.json();
    $("aviso").style.display = "none";
    $("sub").textContent = "Operario " + d.id_fo;
    $("p-op").dataset.ok = "true"; $("t-op").textContent = d.id_fo + " en línea";
    $("hora").textContent = d.hora;
    pintarEstaciones(d.estaciones); pintarSelect(d.estaciones);
    pintarServicio(d.servicio); pintarLineas(d.eventos);
    if (d.registro) {
      $("reg-estado").className = "retro" + (d.registro.ok === false ? " error" : "");
      $("reg-estado").textContent = d.registro.texto;
    }
    const ocupado = d.fichero || (d.servicio && !d.servicio.terminado);
    const sv = d.servicio;
    $("parar").disabled = !(sv && !sv.terminado && (sv.fase === "REGANDO" || sv.fase === "AUTORIZADO"));
    $("pedir").disabled = !!ocupado; $("lanzar").disabled = !!ocupado; $("subir").disabled = !!ocupado; $("registrar").disabled = !!ocupado;
    if (d.fichero && !$("retro-fichero").textContent) $("retro-fichero").textContent = "Ejecutando el fichero de servicios...";
    if (!d.fichero && $("retro-fichero").textContent.startsWith("Ejecutando")) $("retro-fichero").textContent = "";
  } catch (e) {
    $("aviso").style.display = "block";
    $("p-op").dataset.ok = "false"; $("t-op").textContent = "Sin conexión";
  }
}
actualizar();
setInterval(actualizar, 1000);
</script>
</body>
</html>
"""