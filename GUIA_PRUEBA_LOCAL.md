# Guía de prueba en local — WaterManagement

Todo corre en Docker: no hace falta tener Python ni Kafka instalados.
Cada componente se abre en **su propia terminal (CMD)**, situada en la carpeta del proyecto.

## 1. Arrancar Kafka y la Central

```
docker compose down
docker compose up -d --build
docker attach central
```

En esta terminal queda el panel de la Central. Comandos que acepta:

| Comando | Qué hace |
|---|---|
| `r WS-01 10` | Iniciar un riego de 10 s (sin número = sin límite). `r * 10` = todas |
| `b WS-01` | Bloquear (Fuera de Servicio). Para el riego si lo hay. `b *` = todas |
| `a WS-01` | Activar de nuevo. `a *` = todas |
| `o FO-07` | Dar de alta un operador |
| `l` | Listar operadores |
| `p` | Mostrar el panel |

Para salir de la Central **sin pararla**: `Ctrl+P` y después `Ctrl+Q`.
Para volver: `docker attach central`.

Operadores dados de alta de serie: `FO-01` a `FO-05`.

## 2. Estación WS-01 (dos terminales)

Monitor:
```
docker compose run --rm --name ws1-m monitor 6001 central 5000 WS-01 "Parque Central"
```
Engine:
```
docker compose run --rm engine kafka:29092 ws1-m:6001
```

Teclas del Engine (+ Enter): `K` simular fuga · `O` resolver fuga · `R 10` riego manual de 10 s
(`R` solo = sin límite) · `F` finalizar el riego · `E` ver estado · `S` salir.

## 3. Estación WS-02 (dos terminales más)

```
docker compose run --rm --name ws2-m monitor 6001 central 5000 WS-02 "Jardin Norte"
```
```
docker compose run --rm engine kafka:29092 ws2-m:6001
```

## 4. Operadores

Interactivo:
```
docker compose run --rm operador kafka:29092 FO-01
```
Comandos: `WS-01 10` (riego de 10 s; `0` = sin límite; sin número = 10 s) · `lista` · `salir`.

Desde fichero (espera 4 s entre servicios):
```
docker compose run --rm operador kafka:29092 FO-02 --fichero servicios.txt
```

## 5. Pruebas que cubren el enunciado

| # | Prueba | Resultado esperado |
|---|---|---|
| 1 | Arrancar Monitor sin Engine | WS en FUGA (el Monitor no recibe respuesta del Engine) |
| 2 | Arrancar el Engine | WS pasa a DISPONIBLE |
| 3 | FO-01: `WS-01 10` | Mensajes paso a paso, AUTORIZADO, caudal y volumen cada segundo, resumen final |
| 4 | FO con `--fichero servicios.txt` | 4 servicios con 4 s entre ellos; WS-99 DENEGADO |
| 5 | Operador no registrado (`FO-99`) | DENEGADO: OPERADOR_NO_REGISTRADO |
| 6 | Riego largo y `K` en el Engine | Riego cortado al momento, FO recibe INTERRUMPIDO (FUGA), Central en FUGA |
| 7 | Pedir riego en WS con fuga, luego `O` | DENEGADO: WS_FUGA; tras `O`, DISPONIBLE |
| 8 | Central `b WS-02` con riego en curso | Riego cortado (BLOQUEO), WS en FUERA_SERVICIO, peticiones denegadas |
| 9 | Central `a WS-02` | DISPONIBLE de nuevo |
| 10 | Central `r WS-01 5` | Riega como si lo pidiera un operador |
| 11 | Engine `R` y luego `F` | Riego manual validado por la Central; termina con FINALIZADO_EN_WS |
| 12 | Dos FO piden la misma WS a la vez | Una lo consigue, la otra DENEGADO: WS_OCUPADA |
| 13 | Cerrar la terminal de un Engine | WS en FUGA; al relanzarlo, DISPONIBLE |
| 14 | Cerrar un Monitor durante un riego | Riego cortado (DESCONEXION), WS DESCONECTADA; al relanzarlo vuelve |
| 15 | `docker restart central` durante un riego | Monitores se reconectan solos, el panel sale de la BD y el riego sigue |
| 16 | `docker restart kafka` | Todos los módulos se reconectan solos |

## 6. Ver la base de datos

```
docker exec central python -c "import sqlite3; c=sqlite3.connect('/app/wm.db'); [print(r) for r in c.execute('select * from estaciones')]"
```
Tablas: `estaciones`, `operadores`, `riegos`, `eventos`.

## 7. Apagar todo

```
docker compose down
```
Para empezar con la BD vacía, borrar `wm.db`.
