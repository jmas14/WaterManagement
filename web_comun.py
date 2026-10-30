"""
web_comun.py - Piezas compartidas por los paneles web (operario y engine).

  ESTILO         hoja de estilos comun, la misma del panel de la Central
  servir(...)    pequeno servidor HTTP (libreria estandar) con una pagina, un
                 JSON de estado y, si hace falta, rutas POST para las acciones

Seguridad basica: las rutas POST solo aceptan JSON (Content-Type: application/json).
Un navegador no puede enviar eso desde otra pagina web sin un permiso previo que
aqui no se concede, asi que ninguna web ajena puede pulsar los botones por ti.
"""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ESTILO = r"""  :root {
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
"""

ESTILO_EXTRA = r"""
  /* ---------- formularios, botones y listas (paneles del operario y del engine) ---------- */
  [hidden] { display: none !important; }
  .panel + .panel, .panel + .pie { margin-top: 24px; }
  .cols { display: grid; grid-template-columns: minmax(260px, 340px) 1fr; gap: 24px; align-items: start; }
  @media (max-width: 760px) { .cols { grid-template-columns: 1fr; } }
  .campo { margin: 0 0 16px; }
  label { display: block; margin: 0 0 6px; font-size: .9rem; font-weight: 700; }
  input, select, textarea { width: 100%; min-height: 44px; padding: 10px 12px; border-radius: 12px; border: 1px solid var(--borde-fuerte);
                            background: var(--vidrio-fuerte); color: var(--tinta); font: inherit; }
  textarea { min-height: 120px; resize: vertical; line-height: 1.5; }
  input:focus-visible, select:focus-visible, textarea:focus-visible, .btn:focus-visible,
  .tarjeta[tabindex]:focus-visible { outline: 3px solid #6aa7ff; outline-offset: 2px; }
  .ayuda { margin: 6px 0 0; font-size: .85rem; color: var(--suave); }
  .btn { display: inline-flex; align-items: center; justify-content: center; gap: 8px; min-height: 44px; padding: 0 20px; border: 0; border-radius: 12px;
         font: inherit; font-weight: 800; color: #fff; cursor: pointer; background: linear-gradient(165deg, #7cc8ff, #4a7fe8);
         box-shadow: 0 1px 2px rgba(50, 30, 110, .2), 0 10px 20px -8px rgba(60, 110, 230, .7), inset 0 1px 0 rgba(255,255,255,.35);
         transition: transform .15s ease, box-shadow .15s ease, filter .15s ease; }
  .btn:hover:not(:disabled) { transform: translateY(-2px); filter: brightness(1.06); }
  .btn:disabled { opacity: .45; cursor: not-allowed; }
  .btn.sec { color: var(--tinta); background: var(--vidrio-fuerte); border: 1px solid var(--borde-fuerte); box-shadow: var(--sombra-1); }
  .btn.peligro { background: linear-gradient(165deg, #de5a6f, #d2455b); box-shadow: 0 1px 2px rgba(50,30,110,.2), 0 10px 20px -8px rgba(210, 69, 91, .7), inset 0 1px 0 rgba(255,255,255,.3); }
  .btn.ok { background: linear-gradient(165deg, #3ab67c, #2a9462); box-shadow: 0 1px 2px rgba(50,30,110,.2), 0 10px 20px -8px rgba(42, 148, 98, .7), inset 0 1px 0 rgba(255,255,255,.3); }
  .acciones { display: flex; flex-wrap: wrap; gap: 12px; }
  .retro { min-height: 1.4em; margin: 12px 0 0; font-size: .9rem; font-weight: 700; color: var(--suave); }
  .retro.error { color: var(--mal); }
  .lineas { list-style: none; margin: 0; padding: 0; max-height: 280px; overflow: auto; border: 1px solid var(--borde); border-radius: 12px; background: var(--vidrio-fuerte); }
  .lineas li { display: flex; gap: 12px; padding: 8px 12px; border-bottom: 1px solid var(--borde); font-size: .92rem; }
  .lineas li:last-child { border-bottom: 0; }
  .lineas time { color: var(--suave); font-variant-numeric: tabular-nums; flex: none; }
  .lineas .vacia { color: var(--suave); }
  .tarjeta.grande { min-height: 250px; }
  .tarjeta.grande .id { font-size: 2.1rem; }
  .tarjeta.grande .caudal b { font-size: 3.4rem; }
  .tarjeta.compacta { min-height: 130px; cursor: pointer; }
  .tarjeta[data-estado="PENDIENTE"] { background: linear-gradient(165deg, #8b84ab, var(--desconectada)); }
  .tarjeta[aria-pressed="true"] { outline: 3px solid #6aa7ff; outline-offset: 2px; }

"""


def servir(puerto, pagina, estado, posts=None, nombre="WEB"):
    """Arranca el servidor en un hilo. Devuelve el hilo, o None si el puerto esta ocupado."""
    posts = posts or {}

    class Manejador(BaseHTTPRequestHandler):
        def _enviar(self, codigo, tipo, cuerpo):
            datos = cuerpo.encode("utf-8")
            self.send_response(codigo)
            self.send_header("Content-Type", tipo)
            self.send_header("Content-Length", str(len(datos)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(datos)

        def _json(self, codigo, objeto):
            self._enviar(codigo, "application/json; charset=utf-8", json.dumps(objeto))

        def do_GET(self):
            ruta = self.path.split("?")[0]
            if ruta == "/":
                self._enviar(200, "text/html; charset=utf-8", pagina)
            elif ruta == "/api/estado":
                self._json(200, estado())
            else:
                self._enviar(404, "text/plain; charset=utf-8", "No encontrado")

        def do_POST(self):
            ruta = self.path.split("?")[0]
            manejar = posts.get(ruta)
            if manejar is None:
                return self._json(404, {"error": "No encontrado"})
            if not self.headers.get("Content-Type", "").startswith("application/json"):
                return self._json(415, {"error": "Se espera application/json"})
            largo = int(self.headers.get("Content-Length") or 0)
            if largo > 65536:
                return self._json(413, {"error": "Peticion demasiado grande"})
            try:
                datos = json.loads(self.rfile.read(largo) or b"{}")
                if not isinstance(datos, dict):
                    raise ValueError
            except ValueError:
                return self._json(400, {"error": "JSON no valido"})
            try:
                codigo, cuerpo = manejar(datos)
            except Exception as e:  # una accion que falla no debe tumbar el servidor
                codigo, cuerpo = 500, {"error": str(e)}
            self._json(codigo, cuerpo)

        def log_message(self, *args):
            pass

    try:
        servidor = ThreadingHTTPServer(("0.0.0.0", puerto), Manejador)
    except OSError as e:
        print(f"[{time.strftime('%H:%M:%S')}] [{nombre}] No se pudo abrir el panel web en el puerto {puerto}: {e}", flush=True)
        return None
    hilo = threading.Thread(target=servidor.serve_forever, daemon=True)
    hilo.start()
    print(f"[{time.strftime('%H:%M:%S')}] [{nombre}] Panel web en http://0.0.0.0:{puerto}", flush=True)
    return hilo