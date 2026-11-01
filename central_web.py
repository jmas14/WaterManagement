"""
central_web.py - Panel web de la Central (Figura 2 del enunciado).

Se engancha a WM_Central.py con una línea en Central.run():
        import central_web; central_web.arrancar(self)

Sirve, con el servidor HTTP de la librería estándar (sin instalar nada):
  GET /             -> la página del panel (HTML incrustado aquí abajo)
  GET /api/estado   -> JSON con las estaciones, para que la página lo consulte cada segundo

Puerto: variable de entorno PORT (la que Railway asigna) o, si no existe, 8080.

Colores por estado (enunciado):
  DISPONIBLE -> verde | REGANDO -> verde parpadeante (+caudal, volumen y operario)
  FUGA -> rojo | FUERA_SERVICIO -> naranja ("Fuera de Servicio") | DESCONECTADA -> gris
"""

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PUERTO_POR_DEFECTO = 8080

PAGINA = r"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>WaterManagement - Panel de la Central</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>
  :root {
    /* escala de espacios: 4 8 12 16 24 32 | radios: 12 (pildora/chip) 16 (kpi) 20 (tarjeta) 24 (panel) */
    --fondo: #f3ecfa; --cielo: #cfe6ff; --rosa: #f6d9ee;
    --vidrio: rgba(255, 255, 255, .72); --vidrio-fuerte: rgba(255, 255, 255, .86);
    --borde: rgba(110, 90, 190, .16); --borde-fuerte: rgba(110, 90, 190, .26);
    --tinta: #2c2552; --suave: #6f6494;
    --sombra-1: 0 1px 2px rgba(50, 30, 110, .08), 0 8px 20px -10px rgba(50, 30, 110, .25);
    --sombra-2: 0 2px 4px rgba(50, 30, 110, .1), 0 18px 36px -14px rgba(50, 30, 110, .35);
    --disponible: #2a9462; --regando: #2fa56d; --fuga: #d2455b; --fuera: #ffb46e; --desconectada: #7a739a;
    --tx-claro: #ffffff; --tx-oscuro: #4a2400;
    --ok: #2a9462; --mal: #d2455b;
    --fuente: "Nunito", system-ui, -apple-system, "Segoe UI", sans-serif;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --fondo: #1a1533; --cielo: #1d3558; --rosa: #3a1f4f;
      --vidrio: rgba(40, 32, 76, .62); --vidrio-fuerte: rgba(44, 36, 84, .86);
      --borde: rgba(190, 170, 255, .14); --borde-fuerte: rgba(190, 170, 255, .26);
      --tinta: #efeaff; --suave: #b3a8d9;
      --sombra-1: 0 1px 2px rgba(0, 0, 0, .3), 0 8px 20px -10px rgba(0, 0, 0, .6);
      --sombra-2: 0 2px 4px rgba(0, 0, 0, .35), 0 18px 36px -14px rgba(0, 0, 0, .7);
    }
  }
  * { box-sizing: border-box; }
  html { background: var(--fondo); }
  body {
    margin: 0; min-height: 100vh; color: var(--tinta); font-family: var(--fuente); line-height: 1.45;
    background:
      radial-gradient(900px 420px at 8% -5%, var(--rosa), transparent 70%),
      radial-gradient(900px 480px at 100% 0%, var(--cielo), transparent 70%),
      var(--fondo);
    background-repeat: no-repeat;
  }

  /* ---------- barra superior ---------- */
  .barra { position: sticky; top: 0; z-index: 10; background: var(--vidrio-fuerte); border-bottom: 1px solid var(--borde);
           -webkit-backdrop-filter: saturate(1.4) blur(14px); backdrop-filter: saturate(1.4) blur(14px); }
  .barra-in { max-width: 1180px; margin: 0 auto; padding: 12px 24px; display: flex; flex-wrap: wrap;
              align-items: center; justify-content: space-between; gap: 12px 24px; }
  .marca { display: flex; align-items: center; gap: 12px; }
  .logo { width: 36px; height: 36px; border-radius: 12px; display: grid; place-items: center; color: #fff;
          background: linear-gradient(160deg, #7cc8ff, #4a7fe8); box-shadow: 0 6px 14px -6px rgba(60, 110, 230, .7), inset 0 1px 0 rgba(255,255,255,.4); }
  .marca strong { display: block; font-size: 1.05rem; font-weight: 800; letter-spacing: -.01em; line-height: 1.15; }
  .marca span.sub { display: block; font-size: .85rem; color: var(--suave); line-height: 1.2; }
  .indicadores { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; }
  .pildora { display: inline-flex; align-items: center; gap: 8px; padding: 6px 12px; border-radius: 999px; font-size: .88rem; font-weight: 600;
             background: var(--vidrio); border: 1px solid var(--borde); box-shadow: var(--sombra-1); }
  .punto { width: 8px; height: 8px; border-radius: 50%; background: var(--suave); display: block; }
  .pildora[data-ok="true"] .punto { background: var(--ok); box-shadow: 0 0 0 0 rgba(42, 148, 98, .5); animation: latido 2s ease-out infinite; }
  .pildora[data-ok="false"] .punto { background: var(--mal); }
  .hora { font-size: .88rem; color: var(--suave); font-variant-numeric: tabular-nums; padding-left: 4px; }

  /* ---------- pagina ---------- */
  .pagina { max-width: 1180px; margin: 0 auto; padding: 24px 24px 48px; }
  #aviso { display: none; margin: 0 0 16px; padding: 12px 16px; border-radius: 12px; background: var(--mal); color: #fff;
           font-weight: 700; box-shadow: var(--sombra-1); }

  .kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin: 0 0 24px; }
  .kpi { padding: 16px 20px; border-radius: 16px; background: var(--vidrio); border: 1px solid var(--borde); box-shadow: var(--sombra-1);
         -webkit-backdrop-filter: blur(10px); backdrop-filter: blur(10px); }
  .kpi-n { margin: 0; font-size: 2rem; font-weight: 800; letter-spacing: -.02em; line-height: 1.1; font-variant-numeric: tabular-nums; }
  .kpi-n small { font-size: .95rem; font-weight: 700; color: var(--suave); margin-left: 4px; letter-spacing: 0; }
  .kpi-t { margin: 4px 0 0; font-size: .92rem; color: var(--suave); font-weight: 600; }

  .panel { padding: 24px; border-radius: 24px; background: var(--vidrio); border: 1px solid var(--borde); box-shadow: var(--sombra-2);
           -webkit-backdrop-filter: blur(12px); backdrop-filter: blur(12px); }
  .panel-cab { display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 12px 24px; margin: 0 0 24px; padding: 0 0 16px;
               border-bottom: 1px solid var(--borde); }
  .panel-cab h2 { margin: 0; font-size: 1.25rem; font-weight: 800; letter-spacing: -.01em; }
  .leyenda { display: flex; flex-wrap: wrap; gap: 8px; margin: 0; padding: 0; list-style: none; }
  .leyenda li { display: flex; align-items: center; gap: 8px; padding: 4px 12px 4px 8px; border: 1px solid var(--borde); border-radius: 999px;
                background: var(--vidrio-fuerte); font-size: .85rem; font-weight: 600; }
  .leyenda i { width: 12px; height: 12px; border-radius: 50%; display: block; box-shadow: inset 0 0 0 1px rgba(255,255,255,.35); }
  .leyenda b { font-weight: 800; font-variant-numeric: tabular-nums; }

  #rejilla { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 16px; }
  .vacio { grid-column: 1 / -1; margin: 0; padding: 32px 0; text-align: center; color: var(--suave); }
  .pie { margin: 16px 4px 0; font-size: .85rem; color: var(--suave); }

  /* ---------- tarjeta de estacion ---------- */
  .tarjeta { position: relative; overflow: hidden; isolation: isolate; min-height: 200px; padding: 20px; border-radius: 20px;
             display: flex; flex-direction: column; color: var(--tx-claro);
             border: 1px solid rgba(255, 255, 255, .3);
             box-shadow: 0 1px 2px rgba(50, 30, 110, .2), 0 14px 26px -12px rgba(50, 30, 110, .5), inset 0 1px 0 rgba(255,255,255,.3), inset 0 -1px 0 rgba(0,0,0,.08);
             transition: background-color .4s ease, box-shadow .25s ease, transform .25s ease; }
  .tarjeta:hover { transform: translateY(-3px);
             box-shadow: 0 2px 4px rgba(50, 30, 110, .22), 0 22px 36px -12px rgba(50, 30, 110, .55), inset 0 1px 0 rgba(255,255,255,.3), inset 0 -1px 0 rgba(0,0,0,.08); }
  .tarjeta[data-estado="DISPONIBLE"]     { background: linear-gradient(165deg, #34a873, var(--disponible)); }
  .tarjeta[data-estado="REGANDO"]        { background: linear-gradient(165deg, #3ab67c, var(--regando)); animation: parpadeo 1.6s ease-in-out infinite; text-shadow: 0 1px 6px rgba(20, 40, 110, .45); }
  .tarjeta[data-estado="FUGA"]           { background: linear-gradient(165deg, #de5a6f, var(--fuga)); }
  .tarjeta[data-estado="FUERA_SERVICIO"] { background: linear-gradient(165deg, #ffc58a, var(--fuera)); color: var(--tx-oscuro); }
  .tarjeta[data-estado="DESCONECTADA"]   { background: linear-gradient(165deg, #8b84ab, var(--desconectada)); }

  .id { margin: 0; font-size: 1.75rem; font-weight: 800; letter-spacing: -.02em; line-height: 1; text-shadow: 0 2px 8px rgba(30, 20, 80, .35); }
  .tarjeta[data-estado="FUERA_SERVICIO"] .id { text-shadow: 0 1px 0 rgba(255, 255, 255, .45); }
  .ubi { margin: 6px 0 0; font-size: 1rem; opacity: .94; overflow-wrap: anywhere; }
  .medidas { margin-top: 16px; }
  .caudal { margin: 0; display: flex; align-items: baseline; gap: 6px; }
  .caudal b { font-size: 2.75rem; font-weight: 800; letter-spacing: -.03em; line-height: 1; font-variant-numeric: tabular-nums; text-shadow: 0 3px 10px rgba(20, 40, 110, .4); }
  .caudal span { font-size: 1rem; font-weight: 700; opacity: .94; }
  .detalle { margin: 8px 0 0; font-size: .95rem; font-variant-numeric: tabular-nums; opacity: .96; }
  .estado { margin: auto 0 0; padding-top: 12px; font-size: 1rem; font-weight: 800; }

  /* el agua: sube con el caudal y ondula; solo existe cuando la estacion riega */
  .agua { position: absolute; left: 0; right: 0; bottom: 0; height: calc(var(--nivel, .2) * 100%); z-index: -1; transition: height 1s ease; display: none; }
  .tarjeta[data-estado="REGANDO"] .agua { display: block; }
  .agua::before { content: ""; position: absolute; left: 0; right: 0; top: -13px; height: 14px;
    background: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='80' height='14' viewBox='0 0 80 14'%3E%3Cpath d='M0 7 Q20 -1 40 7 T80 7 V14 H0Z' fill='%236ec0ff' fill-opacity='.88'/%3E%3C/svg%3E") repeat-x;
    background-size: 80px 14px; animation: ola 3.2s linear infinite; }
  .agua::after { content: ""; position: absolute; inset: 0; background: linear-gradient(180deg, rgba(110, 192, 255, .88) 0%, rgba(70, 130, 238, .92) 100%); }

  @keyframes ola { to { background-position-x: 80px; } }
  @keyframes parpadeo { 0%, 100% { filter: brightness(1); } 50% { filter: brightness(1.2); } }
  @keyframes latido { 0% { box-shadow: 0 0 0 0 rgba(42, 148, 98, .5); } 70%, 100% { box-shadow: 0 0 0 8px rgba(42, 148, 98, 0); } }
  @media (prefers-reduced-motion: reduce) {
    .tarjeta:hover { transform: none; }
    .tarjeta[data-estado="REGANDO"], .agua::before, .pildora[data-ok="true"] .punto { animation: none; }
    .agua { transition: none; }
  }
  @media (max-width: 560px) {
    .barra-in, .pagina { padding-left: 16px; padding-right: 16px; }
    .panel { padding: 16px; border-radius: 20px; }
  }
</style>
</head>
<body>
<header class="barra">
  <div class="barra-in">
    <div class="marca">
      <span class="logo" aria-hidden="true">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2.5c3.6 4.3 6.5 7.6 6.5 11.2a6.5 6.5 0 0 1-13 0C5.5 10.1 8.4 6.8 12 2.5z"/></svg>
      </span>
      <div><strong>WaterManagement</strong><span class="sub">Panel de la Central</span></div>
    </div>
    <div class="indicadores">
      <span class="pildora" id="p-central" data-ok="unknown"><i class="punto"></i><span id="t-central">Conectando</span></span>
      <span class="pildora" id="p-kafka" data-ok="unknown"><i class="punto"></i><span id="t-kafka">Kafka</span></span>
      <time class="hora" id="hora"></time>
    </div>
  </div>
</header>

<main class="pagina">
  <div id="aviso" role="alert">Sin conexi&oacute;n con la Central. Reintentando...</div>

  <section class="kpis" aria-label="Resumen de la red">
    <div class="kpi"><p class="kpi-n" id="k-total">0</p><p class="kpi-t">Estaciones registradas</p></div>
    <div class="kpi"><p class="kpi-n" id="k-regando">0</p><p class="kpi-t">Regando ahora</p></div>
    <div class="kpi"><p class="kpi-n"><span id="k-caudal">0.0</span><small>L/min</small></p><p class="kpi-t">Caudal total en curso</p></div>
    <div class="kpi"><p class="kpi-n"><span id="k-volumen">0.0</span><small>L</small></p><p class="kpi-t">Volumen de los riegos en curso</p></div>
  </section>

  <section class="panel" aria-labelledby="t-panel">
    <div class="panel-cab">
      <h2 id="t-panel">Estaciones de riego</h2>
      <ul class="leyenda" id="leyenda" aria-label="Recuento por estado"></ul>
    </div>
    <div id="rejilla"><p class="vacio">Esperando datos...</p></div>
  </section>
  <p class="pie">La p&aacute;gina se actualiza sola cada segundo.</p>
</main>

<script>
const ESTADOS = {
  DISPONIBLE: ["Disponible", "var(--disponible)"],
  REGANDO: ["Regando", "var(--regando)"],
  FUGA: ["Fuga", "var(--fuga)"],
  FUERA_SERVICIO: ["Fuera de Servicio", "var(--fuera)"],
  DESCONECTADA: ["Desconectada", "var(--desconectada)"]
};
const CAUDAL_MAX = 20;            // L/min que corresponden a "tanque lleno" en el dibujo
const tarjetas = new Map();       // id -> referencias a los elementos de su tarjeta

function el(tag, clase, texto) {
  const e = document.createElement(tag);
  if (clase) e.className = clase;
  if (texto !== undefined) e.textContent = texto;   // textContent: nada de lo que mande un Monitor se interpreta como HTML
  return e;
}
const $ = id => document.getElementById(id);

function crearTarjeta() {
  const t = el("article", "tarjeta");
  const agua = el("div", "agua"); agua.setAttribute("aria-hidden", "true");
  const id = el("h3", "id"), ubi = el("p", "ubi");
  const medidas = el("div", "medidas");
  const caudal = el("p", "caudal"), cNum = el("b"), cUni = el("span", "", "L/min");
  caudal.append(cNum, cUni);
  const vol = el("p", "detalle"), fo = el("p", "detalle");
  medidas.append(caudal, vol, fo);
  const estado = el("p", "estado");
  t.append(agua, id, ubi, medidas, estado);
  return {el: t, id, ubi, medidas, cNum, vol, fo, estado};
}

function actualizarTarjeta(r, e) {
  r.el.dataset.estado = e.estado;
  r.id.textContent = e.id;
  r.ubi.textContent = e.ubicacion;
  r.estado.textContent = (ESTADOS[e.estado] || [e.estado])[0];
  const regando = e.estado === "REGANDO";
  r.medidas.hidden = !regando;
  if (regando) {
    r.cNum.textContent = e.caudal.toFixed(1);
    r.vol.textContent = "Volumen: " + e.volumen.toFixed(1) + " L";
    r.fo.textContent = "Operario: " + (e.id_fo || "-");
    r.el.style.setProperty("--nivel", Math.min(Math.max(e.caudal / CAUDAL_MAX, 0.12), 0.92).toFixed(2));
  }
}

function pintarLeyenda(estaciones) {
  const ley = $("leyenda");
  ley.replaceChildren();
  for (const [clave, [nombre, color]] of Object.entries(ESTADOS)) {
    const n = estaciones.filter(e => e.estado === clave).length;
    const li = el("li"), punto = el("i"), cuenta = el("b", "", String(n));
    punto.style.background = color;
    li.append(punto, document.createTextNode(nombre + " "), cuenta);
    ley.append(li);
  }
}

function pintarResumen(estaciones) {
  const regando = estaciones.filter(e => e.estado === "REGANDO");
  $("k-total").textContent = estaciones.length;
  $("k-regando").textContent = regando.length;
  $("k-caudal").textContent = regando.reduce((s, e) => s + e.caudal, 0).toFixed(1);
  $("k-volumen").textContent = regando.reduce((s, e) => s + e.volumen, 0).toFixed(1);
}

function pintarConexion(central, kafka, hora) {
  $("p-central").dataset.ok = String(central);
  $("t-central").textContent = central ? "Central en directo" : "Central sin conexión";
  if (kafka === null) { $("p-kafka").dataset.ok = "unknown"; $("t-kafka").textContent = "Kafka"; }
  else { $("p-kafka").dataset.ok = String(kafka); $("t-kafka").textContent = kafka ? "Kafka conectado" : "Kafka sin conexión"; }
  if (hora) $("hora").textContent = hora;
}

function pintar(datos) {
  const rejilla = $("rejilla");
  rejilla.querySelector(".vacio")?.remove();
  const vistos = new Set();
  datos.estaciones.forEach((e, i) => {
    vistos.add(e.id);
    if (!tarjetas.has(e.id)) tarjetas.set(e.id, crearTarjeta());
    const r = tarjetas.get(e.id);
    actualizarTarjeta(r, e);
    if (rejilla.children[i] !== r.el) rejilla.insertBefore(r.el, rejilla.children[i] || null);
  });
  for (const [id, r] of tarjetas) if (!vistos.has(id)) { r.el.remove(); tarjetas.delete(id); }
  if (!datos.estaciones.length) rejilla.append(el("p", "vacio", "Todavía no hay estaciones registradas. Arranca un Monitor para darla de alta."));
  pintarLeyenda(datos.estaciones);
  pintarResumen(datos.estaciones);
  pintarConexion(true, datos.kafka, datos.hora);
}

async function actualizar() {
  try {
    const r = await fetch("/api/estado", {cache: "no-store"});
    if (!r.ok) throw new Error(r.status);
    pintar(await r.json());
    $("aviso").style.display = "none";
  } catch (e) {
    $("aviso").style.display = "block";
    pintarConexion(false, null, "");
  }
}
actualizar();
setInterval(actualizar, 1000);
</script>
</body>
</html>
"""


def _crear_manejador(central):
    class Manejador(BaseHTTPRequestHandler):
        def _responder(self, codigo, tipo, cuerpo):
            datos = cuerpo.encode("utf-8")
            self.send_response(codigo)
            self.send_header("Content-Type", tipo)
            self.send_header("Content-Length", str(len(datos)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(datos)

        def do_GET(self):
            ruta = self.path.split("?")[0]
            if ruta == "/":
                self._responder(200, "text/html; charset=utf-8", PAGINA)
            elif ruta == "/api/estado":
                with central.lock:
                    estaciones = [ws.resumen() for ws in sorted(central.ws.values(), key=lambda w: w.id)]
                    kafka = central.productor is not None
                cuerpo = json.dumps({"estaciones": estaciones, "kafka": kafka,
                                     "hora": time.strftime("%H:%M:%S")})
                self._responder(200, "application/json; charset=utf-8", cuerpo)
            else:
                self._responder(404, "text/plain; charset=utf-8", "No encontrado")

        def log_message(self, *args):
            pass  # no ensuciar el panel de consola con una línea por petición

    return Manejador


def arrancar(central, puerto=None):
    """Lanza el servidor web en un hilo aparte y devuelve el hilo."""
    puerto = puerto or int(os.environ.get("PORT", PUERTO_POR_DEFECTO))
    servidor = ThreadingHTTPServer(("0.0.0.0", puerto), _crear_manejador(central))
    hilo = threading.Thread(target=servidor.serve_forever, daemon=True)
    hilo.start()
    print(f"[{time.strftime('%H:%M:%S')}] [CENTRAL] Panel web en http://0.0.0.0:{puerto}", flush=True)
    return hilo