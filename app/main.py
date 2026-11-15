
import asyncio
import json
import os
import uuid
from datetime import datetime, timezone

import asyncpg
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Oxigen X3", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

RATES = {
    "wood": 3,
    "stone": 3,
    "metal_ore": 3,
    "sulfur_ore": 3,
    "scrap": 3,
    "crafting": 1,
}

players = {}
sockets = set()
lock = asyncio.Lock()
db_pool = None


@app.on_event("startup")
async def startup():
    global db_pool

    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        print("WARNING: DATABASE_URL is not configured. Using memory mode.")
        return

    db_pool = await asyncpg.create_pool(
        dsn=database_url,
        min_size=1,
        max_size=5,
        ssl="require",
        statement_cache_size=0,
    )

    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS player_profiles (
                id UUID PRIMARY KEY,
                nickname TEXT NOT NULL,
                inventory JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)

    print("Oxigen PostgreSQL connected successfully.")


@app.on_event("shutdown")
async def shutdown():
    global db_pool

    if db_pool:
        await db_pool.close()


@app.get("/")
async def root():
    return {
        "name": "Oxigen X3",
        "status": "online",
        "version": "0.2.0",
    }


@app.get("/health")
async def health():
    return {
        "ok": True,
        "players_online": len(players),
        "database": "connected" if db_pool else "memory_mode",
    }


@app.get("/config")
async def config():
    return {
        "server_name": "Oxigen X3",
        "rates": RATES,
        "database_enabled": db_pool is not None,
    }


async def get_or_create_profile(pid, nickname):
    if not db_pool:
        return {}

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO player_profiles (id, nickname)
            VALUES ($1, $2)
            ON CONFLICT (id)
            DO UPDATE SET
                nickname = EXCLUDED.nickname,
                updated_at = NOW()
            RETURNING inventory;
            """,
            uuid.UUID(pid),
            nickname,
        )

    inventory = row["inventory"]

    if isinstance(inventory, str):
        inventory = json.loads(inventory)

    return inventory or {}


async def save_profile(pid, inventory):
    if not db_pool:
        return

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE player_profiles
            SET inventory = $2::jsonb,
                updated_at = NOW()
            WHERE id = $1;
            """,
            uuid.UUID(pid),
            json.dumps(inventory),
        )


async def broadcast(data):
    dead = []
    payload = json.dumps(data)

    for ws in list(sockets):
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(ws)

    for ws in dead:
        sockets.discard(ws)


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()

    pid = str(uuid.uuid4())

    await ws.send_text(json.dumps({
        "type": "welcome",
        "player_id": pid,
        "rates": RATES,
    }))

    try:
        while True:
            try:
                data = json.loads(await ws.receive_text())
            except json.JSONDecodeError:
                await ws.send_text(json.dumps({
                    "type": "error",
                    "message": "Invalid JSON",
                }))
                continue

            if not isinstance(data, dict):
                continue

            event = data.get("type")

            if event == "join":
                nickname = (
                    str(data.get("nickname", "Survivor")).strip()[:20]
                    or "Survivor"
                )

                requested_id = data.get("player_id")

                if requested_id:
                    try:
                        pid = str(uuid.UUID(str(requested_id)))
                    except ValueError:
                        await ws.send_text(json.dumps({
                            "type": "error",
                            "message": "Invalid player_id",
                        }))
                        continue

                try:
                    inventory = await get_or_create_profile(pid, nickname)
                except Exception:
                    await ws.send_text(json.dumps({
                        "type": "error",
                        "message": "Database error",
                    }))
                    continue

                async with lock:
                    players[pid] = {
                        "id": pid,
                        "nickname": nickname,
                        "inventory": inventory,
                        "joined_at": datetime.now(
                            timezone.utc
                        ).isoformat(),
                    }
                    sockets.add(ws)

                await ws.send_text(json.dumps({
                    "type": "joined",
                    "player_id": pid,
                    "inventory": inventory,
                }))

                await broadcast({
                    "type": "players",
                    "players": [
                        {
                            "id": p["id"],
                            "nickname": p["nickname"],
                            "joined_at": p["joined_at"],
                        }
                        for p in players.values()
                    ],
                    "players_online": len(players),
                })

            elif event == "save_inventory":
                if pid not in players:
                    await ws.send_text(json.dumps({
                        "type": "error",
                        "message": "Join first",
                    }))
                    continue

                inventory = data.get("inventory")

                if not isinstance(inventory, dict) or len(inventory) > 100:
                    await ws.send_text(json.dumps({
                        "type": "error",
                        "message": "Invalid inventory",
                    }))
                    continue

                valid = all(
                    isinstance(key, str)
                    and isinstance(value, int)
                    and not isinstance(value, bool)
                    and 0 <= value <= 100000000
                    for key, value in inventory.items()
                )

                if not valid:
                    await ws.send_text(json.dumps({
                        "type": "error",
                        "message": "Invalid inventory values",
                    }))
                    continue

                try:
                    await save_profile(pid, inventory)
                    players[pid]["inventory"] = inventory

                    await ws.send_text(json.dumps({
                        "type": "inventory_saved",
                    }))
                except Exception:
                    await ws.send_text(json.dumps({
                        "type": "error",
                        "message": "Could not save inventory",
                    }))

            elif event == "get_players":
                await ws.send_text(json.dumps({
                    "type": "players",
                    "players": [
                        {
                            "id": p["id"],
                            "nickname": p["nickname"],
                            "joined_at": p["joined_at"],
                        }
                        for p in players.values()
                    ],
                    "players_online": len(players),
                }))

            elif event == "ping":
                await ws.send_text(json.dumps({"type": "pong"}))

            else:
                await ws.send_text(json.dumps({
                    "type": "error",
                    "message": (
                        "Use join, get_players, ping or save_inventory"
                    ),
                }))

    except WebSocketDisconnect:
        pass

    finally:
        sockets.discard(ws)

        player = players.pop(pid, None)

        if player:
            try:
                await save_profile(pid, player.get("inventory", {}))
            except Exception:
                print("Could not save player on disconnect:", pid)

        await broadcast({
            "type": "players",
            "players": [
                {
                    "id": p["id"],
                    "nickname": p["nickname"],
                    "joined_at": p["joined_at"],
                }
                for p in players.values()
            ],
            "players_online": len(players),
        })
