import asyncio, json, os, uuid
from datetime import datetime, timezone
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Rust Mobile X3", version="0.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])
RATES = {"wood": 3, "stone": 3, "metal_ore": 3, "sulfur_ore": 3, "scrap": 3, "crafting": 1}
players = {}
sockets = set()
lock = asyncio.Lock()

@app.get("/")
async def root():
    return {"name": "Rust Mobile X3", "status": "online", "version": "0.1.0"}

@app.get("/health")
async def health():
    return {"ok": True, "players_online": len(players)}

@app.get("/config")
async def config():
    return {"server_name": "Rust Mobile X3", "rates": RATES,
            "note": "Prototype: player data resets when the service restarts."}

async def broadcast(data):
    dead = []
    payload = json.dumps(data)
    for ws in list(sockets):
        try: await ws.send_text(payload)
        except Exception: dead.append(ws)
    for ws in dead: sockets.discard(ws)

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    pid = str(uuid.uuid4())
    await ws.send_text(json.dumps({"type":"welcome", "player_id":pid, "rates":RATES}))
    try:
        while True:
            try: data = json.loads(await ws.receive_text())
            except json.JSONDecodeError:
                await ws.send_text(json.dumps({"type":"error","message":"Invalid JSON"}))
                continue
            event = data.get("type")
            if event == "join":
                nickname = str(data.get("nickname","Survivor")).strip()[:20] or "Survivor"
                async with lock:
                    players[pid] = {"id":pid, "nickname":nickname,
                                    "joined_at":datetime.now(timezone.utc).isoformat()}
                    sockets.add(ws)
                await broadcast({"type":"players","players":list(players.values()),
                                 "players_online":len(players)})
            elif event == "get_players":
                await ws.send_text(json.dumps({"type":"players","players":list(players.values()),
                                               "players_online":len(players)}))
            elif event == "ping":
                await ws.send_text(json.dumps({"type":"pong"}))
            else:
                await ws.send_text(json.dumps({"type":"error","message":"Use join, get_players or ping"}))
    except WebSocketDisconnect:
        pass
    finally:
        sockets.discard(ws)
        async with lock: players.pop(pid, None)
        await broadcast({"type":"players","players":list(players.values()),
                         "players_online":len(players)})
