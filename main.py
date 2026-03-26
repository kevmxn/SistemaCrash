#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import aiohttp
import websockets
import json
import sqlite3
import time
import random
import os
from datetime import datetime
from typing import Set, Dict, Any
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn

# ============================================
# CONFIGURACIÓN
# ============================================
API_CRASH = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakecrash/latest'
API_SLIDE = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakeslide/latest'

SPACEMAN_WS = 'wss://dga.pragmaticplaylive.net/ws'
SPACEMAN_CASINO_ID = 'ppcdk00000005349'
SPACEMAN_CURRENCY = 'BRL'
SPACEMAN_GAME_ID = 1301

DB_FILE = 'data/eventos.db'
MAX_HISTORY = 100000

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:60.0) Gecko/20100101 Firefox/60.0",
    "Mozilla/5.0 (Windows NT 6.1; rv:52.0) Gecko/20100101 Firefox/52.0",
    "Mozilla/5.0 (Windows NT 5.1; rv:45.0) Gecko/20100101 Firefox/45.0",
    "Mozilla/5.0 (Windows NT 5.1; rv:38.0) Gecko/20100101 Firefox/38.0",
    "Mozilla/5.0 (Windows NT 5.1; rv:11.0) Gecko/20100101 Firefox/11.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0",
    "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0",
    "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0 Waterfox/109.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:60.9) Gecko/20100101 Goanna/4.9 Firefox/60.9 PaleMoon/28.9.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Goanna/5.0 Firefox/78.0 PaleMoon/29.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:52.9) Gecko/20100101 Goanna/4.0 Firefox/52.9 Basilisk/2019.10.29",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0) Gecko/20100101 Firefox/4.0 SeaMonkey/2.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:38.0) Gecko/20100101 Firefox/38.0 SeaMonkey/2.35",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.28) Gecko/20120306 Firefox/3.6.28 (K-Meleon 1.5.4)",
    "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko",
    "Mozilla/5.0 (Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko",
    "Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko",
    "Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)",
    "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux i686; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:102.0) Gecko/20100101 Firefox/102.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:91.0) Gecko/20100101 Firefox/91.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0",
    "Mozilla/5.0 (X11; Linux i686; rv:68.0) Gecko/20100101 Firefox/68.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0",
    "Mozilla/5.0 (X11; Linux i686; rv:52.0) Gecko/20100101 Firefox/52.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:45.0) Gecko/20100101 Firefox/45.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:31.0) Gecko/20100101 Firefox/31.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:60.9) Gecko/20100101 Goanna/4.9 Firefox/60.9 PaleMoon/28.9.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:52.9) Gecko/20100101 Goanna/4.0 Firefox/52.9 Basilisk/2019.10.29",
    "Mozilla/5.0 (X11; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0 SeaMonkey/2.35",
    "Mozilla/5.0 (X11; Linux i686; rv:2.0) Gecko/20100101 Firefox/4.0 SeaMonkey/2.1",
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.2.28) Gecko/20120306 Firefox/3.6.28 (K-Meleon 1.5.4)",
]

BASE_SLEEP = 1.0
MAX_SLEEP = 60.0

crash_ids: Set[str] = set()
slide_ids: Set[str] = set()
spaceman_last_multiplier: float = None

api_status = {
    'crash': {'consecutive_errors': 0, 'next_allowed_time': 0},
    'slide': {'consecutive_errors': 0, 'next_allowed_time': 0}
}

app = FastAPI()
active_connections: Set[WebSocket] = set()

# ============================================
# BASE DE DATOS
# ============================================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS eventos
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  api TEXT,
                  event_id TEXT,
                  maxMultiplier REAL,
                  roundDuration REAL,
                  startedAt TEXT,
                  timestamp_recepcion TEXT)''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_api ON eventos (api)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON eventos (timestamp_recepcion)')
    conn.commit()
    conn.close()

def guardar_evento_sync(api: str, event_id: str, maxMultiplier: float, roundDuration: float, startedAt: str) -> str:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    timestamp = datetime.now().isoformat()
    c.execute('''INSERT INTO eventos (api, event_id, maxMultiplier, roundDuration, startedAt, timestamp_recepcion)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (api, event_id, maxMultiplier, roundDuration, startedAt, timestamp))
    conn.commit()
    c.execute('''DELETE FROM eventos WHERE id IN (
                    SELECT id FROM eventos WHERE api = ? ORDER BY timestamp_recepcion DESC LIMIT -1 OFFSET ?
                )''', (api, MAX_HISTORY))
    conn.commit()
    conn.close()
    return timestamp

async def guardar_evento(api: str, event_id: str, maxMultiplier: float, roundDuration: float, startedAt: str) -> str:
    return await asyncio.to_thread(guardar_evento_sync, api, event_id, maxMultiplier, roundDuration, startedAt)

async def obtener_ultimos_eventos(api: str, limite: int = MAX_HISTORY) -> list:
    def _sync():
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute('''SELECT api, event_id, maxMultiplier, roundDuration, startedAt, timestamp_recepcion
                     FROM eventos WHERE api = ? ORDER BY timestamp_recepcion DESC LIMIT ?''', (api, limite))
        filas = c.fetchall()
        conn.close()
        eventos = []
        for fila in filas:
            eventos.append({
                'api': fila[0],
                'event_id': fila[1],
                'maxMultiplier': fila[2],
                'roundDuration': fila[3],
                'startedAt': fila[4],
                'timestamp_recepcion': fila[5]
            })
        return eventos
    return await asyncio.to_thread(_sync)

# ============================================
# WEBSOCKET
# ============================================
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.add(websocket)
    try:
        for api in ['crash', 'slide', 'spaceman']:
            eventos = await obtener_ultimos_eventos(api, MAX_HISTORY)
            if eventos:
                await websocket.send_json({
                    'tipo': 'historial',
                    'api': api,
                    'eventos': eventos
                })
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_connections.remove(websocket)

async def broadcast(event_data: Dict[str, Any]):
    if not active_connections:
        return
    for connection in list(active_connections):
        try:
            await connection.send_json(event_data)
        except Exception:
            pass

# ============================================
# HELPERS HTTP
# ============================================
def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)

async def consultar_con_backoff(session: aiohttp.ClientSession, url: str, api_nombre: str) -> dict | None:
    status = api_status[api_nombre]
    now = time.time()
    if now < status['next_allowed_time']:
        wait = status['next_allowed_time'] - now
        print(f"⏳ {api_nombre} en espera por {wait:.1f}s (backoff)")
        await asyncio.sleep(wait)
        return None

    headers = {'User-Agent': get_random_user_agent()}
    try:
        async with session.get(url, headers=headers, timeout=5) as resp:
            if 'Retry-After' in resp.headers:
                retry_after = int(resp.headers['Retry-After'])
                status['next_allowed_time'] = time.time() + retry_after
                status['consecutive_errors'] += 1
                print(f"⚠️ {api_nombre} pide esperar {retry_after}s")
                return None
            if resp.status == 200:
                status['consecutive_errors'] = 0
                return await resp.json()
            elif resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 2 ** status['consecutive_errors']))
                status['next_allowed_time'] = time.time() + retry_after
                status['consecutive_errors'] += 1
                print(f"⚠️ {api_nombre} rate limited. Esperando {retry_after}s")
                return None
            elif 500 <= resp.status < 600:
                status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** status['consecutive_errors']))
                status['next_allowed_time'] = time.time() + backoff
                print(f"❌ {api_nombre} error {resp.status}. Backoff {backoff:.1f}s")
                return None
            else:
                print(f"⚠️ {api_nombre} código no esperado: {resp.status}")
                return None
    except asyncio.TimeoutError:
        status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** status['consecutive_errors']))
        status['next_allowed_time'] = time.time() + backoff
        print(f"⏰ {api_nombre} timeout. Backoff {backoff:.1f}s")
        return None
    except Exception as e:
        status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** status['consecutive_errors']))
        status['next_allowed_time'] = time.time() + backoff
        print(f"💥 {api_nombre} error: {e}. Backoff {backoff:.1f}s")
        return None

# ============================================
# MONITORES
# ============================================
async def monitor_crash():
    global crash_ids
    async with aiohttp.ClientSession() as session:
        while True:
            data = await consultar_con_backoff(session, API_CRASH, 'crash')
            if data:
                api_id = data.get('id')
                if api_id and api_id not in crash_ids:
                    crash_ids.add(api_id)
                    data_inner = data.get('data', {})
                    result = data_inner.get('result', {})
                    max_mult = result.get('maxMultiplier')
                    round_dur = result.get('roundDuration')
                    started_at = data_inner.get('startedAt')
                    if max_mult is not None and max_mult > 0:
                        timestamp = await guardar_evento('crash', api_id, max_mult, round_dur, started_at)
                        await broadcast({
                            'tipo': 'crash',
                            'id': api_id,
                            'maxMultiplier': max_mult,
                            'roundDuration': round_dur,
                            'startedAt': started_at,
                            'timestamp_recepcion': timestamp
                        })
                        print(f"✅ Crash nuevo: ID={api_id} maxMult={max_mult}")
                    else:
                        print(f"⚠️ Crash ID {api_id} con multiplicador inválido: {max_mult}")
            await asyncio.sleep(0.5)

async def monitor_slide():
    global slide_ids
    async with aiohttp.ClientSession() as session:
        while True:
            data = await consultar_con_backoff(session, API_SLIDE, 'slide')
            if data:
                api_id = data.get('id')
                if api_id and api_id not in slide_ids:
                    slide_ids.add(api_id)
                    data_inner = data.get('data', {})
                    result = data_inner.get('result', {})
                    max_mult = result.get('maxMultiplier')
                    started_at = data_inner.get('startedAt')
                    if max_mult is not None and max_mult > 0:
                        timestamp = await guardar_evento('slide', api_id, max_mult, None, started_at)
                        await broadcast({
                            'tipo': 'slide',
                            'id': api_id,
                            'maxMultiplier': max_mult,
                            'roundDuration': None,
                            'startedAt': started_at,
                            'timestamp_recepcion': timestamp
                        })
                        print(f"✅ Slide nuevo: ID={api_id} maxMult={max_mult}")
                    else:
                        print(f"⚠️ Slide ID {api_id} con multiplicador inválido: {max_mult}")
            await asyncio.sleep(0.5)

async def monitor_spaceman():
    global spaceman_last_multiplier
    reconnect_delay = BASE_SLEEP
    while True:
        try:
            async with websockets.connect(SPACEMAN_WS) as ws:
                print("✅ Spaceman WebSocket conectado")
                subscribe_msg = {
                    "type": "subscribe",
                    "casinoId": SPACEMAN_CASINO_ID,
                    "currency": SPACEMAN_CURRENCY,
                    "key": [SPACEMAN_GAME_ID]
                }
                await ws.send(json.dumps(subscribe_msg))
                print("📡 Suscripción Spaceman enviada")
                reconnect_delay = BASE_SLEEP
                async for message in ws:
                    try:
                        data = json.loads(message)
                        if "gameResult" in data and data["gameResult"]:
                            result_str = data["gameResult"][0].get("result")
                            if result_str:
                                multiplier = float(result_str)
                                if multiplier >= 1.00 and multiplier != spaceman_last_multiplier:
                                    spaceman_last_multiplier = multiplier
                                    event_id = data.get("gameId") or f"spaceman_{int(time.time())}"
                                    started_at = datetime.now().isoformat()
                                    timestamp = await guardar_evento('spaceman', event_id, multiplier, None, started_at)
                                    await broadcast({
                                        'tipo': 'spaceman',
                                        'id': event_id,
                                        'maxMultiplier': multiplier,
                                        'roundDuration': None,
                                        'startedAt': started_at,
                                        'timestamp_recepcion': timestamp
                                    })
                                    print(f"🚀 Spaceman nuevo: {multiplier:.2f}x")
                    except (json.JSONDecodeError, KeyError, ValueError, IndexError):
                        pass
        except (websockets.exceptions.ConnectionClosed, ConnectionRefusedError) as e:
            print(f"🔴 Spaceman conexión cerrada: {e}. Reintentando en {reconnect_delay:.1f}s")
        except Exception as e:
            print(f"💥 Spaceman error inesperado: {e}. Reintentando en {reconnect_delay:.1f}s")
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(MAX_SLEEP, reconnect_delay * 2)

# ============================================
# LIFESPAN
# ============================================
@app.on_event("startup")
async def startup_event():
    await asyncio.to_thread(init_db)
    asyncio.create_task(monitor_crash())
    asyncio.create_task(monitor_slide())
    asyncio.create_task(monitor_spaceman())
    print("🚀 Monitoreo unificado iniciado (Crash, Slide, Spaceman)")

# ============================================
# SERVIR HTML
# ============================================
@app.get("/")
async def get_index():
    with open("index.html", "r", encoding="utf-8") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content)

# ============================================
# EJECUCIÓN
# ============================================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
