
import asyncio
import json
import os
import uuid
from datetime import datetime, timezone

import asyncpg
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI(title="Oxigen X3", version="0.3.0")

RATES = {
    "wood": 3,
    "stone": 3,
    "metal_ore": 3,
    "sulfur_ore": 3,
    "scrap": 3,
    "crafting": 1,
}

RECIPES = {
    "workbench": {
        "name": "Верстак I уровня",
        "cost": {"wood": 300, "stone": 100, "scrap": 50},
        "research": False,
    },
    "hatchet": {
        "name": "Топор",
        "cost": {"wood": 100, "stone": 50},
        "research": False,
    },
    "pickaxe": {
        "name": "Кирка",
        "cost": {"wood": 100, "stone": 80},
        "research": False,
    },
    "spear": {
        "name": "Копьё",
        "cost": {"wood": 150, "stone": 50},
        "research": True,
    },
    "bow": {
        "name": "Лук",
        "cost": {"wood": 200, "scrap": 30},
        "research": True,
    },
}

BUILDINGS = {
    "foundation": {"name": "Фундамент", "cost": {"wood": 100}},
    "wall": {"name": "Стена", "cost": {"wood": 75}},
    "door": {"name": "Дверь", "cost": {"wood": 90}},
    "window": {"name": "Оконная стена", "cost": {"wood": 85}},
    "roof": {"name": "Крыша", "cost": {"wood": 75}},
}

players = {}
sockets = set()
world_objects = {}
lock = asyncio.Lock()
db_pool = None


def starter_inventory():
    return {
        "wood": 0,
        "stone": 0,
        "metal_ore": 0,
        "sulfur_ore": 0,
        "scrap": 0,
        "workbench": 0,
        "hatchet": 0,
        "pickaxe": 0,
        "spear": 0,
        "bow": 0,
        "arrows": 0,
    }


@app.on_event("startup")
async def startup():
    global db_pool

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("DATABASE_URL missing: memory mode")
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
                research JSONB NOT NULL DEFAULT '[]'::jsonb,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS world_buildings (
                id TEXT PRIMARY KEY,
                owner_id UUID NOT NULL,
                kind TEXT NOT NULL,
                x DOUBLE PRECISION NOT NULL,
                y DOUBLE PRECISION NOT NULL,
                z DOUBLE PRECISION NOT NULL,
                rotation DOUBLE PRECISION NOT NULL DEFAULT 0,
                level INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        rows = await conn.fetch("SELECT * FROM world_buildings")
        for row in rows:
            world_objects[row["id"]] = {
                "id": row["id"],
                "owner_id": str(row["owner_id"]),
                "kind": row["kind"],
                "x": row["x"],
                "y": row["y"],
                "z": row["z"],
                "rotation": row["rotation"],
                "level": row["level"],
            }

    print("Oxigen X3 database ready")


@app.on_event("shutdown")
async def shutdown():
    if db_pool:
        await db_pool.close()


async def send(ws, data):
    await ws.send_text(json.dumps(data))


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


async def save_player(pid):
    if not db_pool or pid not in players:
        return
    p = players[pid]
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO player_profiles (id, nickname, inventory, research)
            VALUES ($1, $2, $3::jsonb, $4::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                nickname = EXCLUDED.nickname,
                inventory = EXCLUDED.inventory,
                research = EXCLUDED.research,
                updated_at = NOW()
        """, uuid.UUID(pid), p["nickname"],
             json.dumps(p["inventory"]), json.dumps(p["research"]))


async def load_player(pid, nickname):
    if not db_pool:
        return starter_inventory(), []

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT inventory, research FROM player_profiles WHERE id=$1",
            uuid.UUID(pid),
        )

    if not row:
        return starter_inventory(), []

    inv = row["inventory"]
    research = row["research"]
    if isinstance(inv, str):
        inv = json.loads(inv)
    if isinstance(research, str):
        research = json.loads(research)

    result = starter_inventory()
    result.update(inv or {})
    return result, research or []


def has_cost(inventory, cost):
    return all(inventory.get(k, 0) >= v for k, v in cost.items())


def pay(inventory, cost):
    for key, amount in cost.items():
        inventory[key] -= amount


async def require_player(ws, pid):
    if pid not in players:
        await send(ws, {"type": "error", "message": "Сначала войдите в игру"})
        return None
    return players[pid]


async def save_building(building):
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO world_buildings
            (id, owner_id, kind, x, y, z, rotation, level)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (id) DO UPDATE SET
                level=EXCLUDED.level
        """, building["id"], uuid.UUID(building["owner_id"]),
             building["kind"], building["x"], building["y"],
             building["z"], building["rotation"], building["level"])


@app.get("/")
async def root():
    return {"name": "Oxigen X3", "status": "online", "version": "0.3.0"}


@app.get("/health")
async def health():
    return {
        "ok": True,
        "players_online": len(players),
        "buildings": len(world_objects),
        "database": "connected" if db_pool else "memory_mode",
    }


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    pid = str(uuid.uuid4())
    joined = False

    await send(ws, {
        "type": "welcome",
        "player_id": pid,
        "rates": RATES,
        "recipes": RECIPES,
        "buildings": BUILDINGS,
    })

    try:
        while True:
            try:
                data = json.loads(await ws.receive_text())
            except (json.JSONDecodeError, TypeError):
                await send(ws, {"type": "error", "message": "Неверный JSON"})
                continue

            if not isinstance(data, dict):
                continue

            event = data.get("type")

            if event == "join":
                nickname = str(data.get("nickname", "Survivor")).strip()[:20] or "Survivor"
                requested = data.get("player_id")
                if requested:
                    try:
                        pid = str(uuid.UUID(str(requested)))
                    except ValueError:
                        await send(ws, {"type": "error", "message": "Неверный ID"})
                        continue

                inventory, research = await load_player(pid, nickname)
                players[pid] = {
                    "id": pid,
                    "nickname": nickname,
                    "inventory": inventory,
                    "research": research,
                }
                sockets.add(ws)
                joined = True

                await send(ws, {
                    "type": "joined",
                    "player_id": pid,
                    "inventory": inventory,
                    "research": research,
                    "buildings": list(world_objects.values()),
                })
                await broadcast({
                    "type": "players",
                    "players": [
                        {"id": p["id"], "nickname": p["nickname"]}
                        for p in players.values()
                    ],
                })

            elif event == "get_players":
                await send(ws, {
                    "type": "players",
                    "players": [
                        {"id": p["id"], "nickname": p["nickname"]}
                        for p in players.values()
                    ],
                })

            elif event == "harvest":
                p = await require_player(ws, pid)
                if not p:
                    continue

                resource = data.get("resource")
                if resource not in RATES:
                    await send(ws, {"type": "error", "message": "Неизвестный ресурс"})
                    continue

                amount = data.get("amount", 1)
                if not isinstance(amount, int) or isinstance(amount, bool):
                    continue
                amount = max(1, min(amount, 100))
                gained = amount * RATES[resource]
                p["inventory"][resource] = p["inventory"].get(resource, 0) + gained
                await save_player(pid)
                await send(ws, {
                    "type": "inventory",
                    "inventory": p["inventory"],
                    "message": f"+{gained} {resource}",
                })

            elif event == "craft":
                p = await require_player(ws, pid)
                if not p:
                    continue

                item = data.get("item")
                recipe = RECIPES.get(item)
                if not recipe:
                    await send(ws, {"type": "error", "message": "Нет такого рецепта"})
                    continue

                if recipe["research"] and item not in p["research"]:
                    await send(ws, {"type": "error", "message": "Сначала изучите чертёж"})
                    continue

                if item == "workbench" and p["inventory"].get("workbench", 0) >= 1:
                    await send(ws, {"type": "error", "message": "Верстак уже изготовлен"})
                    continue

                if not has_cost(p["inventory"], recipe["cost"]):
                    await send(ws, {"type": "error", "message": "Недостаточно ресурсов"})
                    continue

                pay(p["inventory"], recipe["cost"])
                p["inventory"][item] = p["inventory"].get(item, 0) + 1
                await save_player(pid)
                await send(ws, {
                    "type": "inventory",
                    "inventory": p["inventory"],
                    "message": f"Создано: {recipe['name']}",
                })

            elif event == "research":
                p = await require_player(ws, pid)
                if not p:
                    continue

                item = data.get("item")
                if p["inventory"].get("workbench", 0) < 1:
                    await send(ws, {"type": "error", "message": "Нужен верстак I уровня"})
                    continue
                if item not in RECIPES or not RECIPES[item]["research"]:
                    await send(ws, {"type": "error", "message": "Нельзя изучить этот предмет"})
                    continue
                if item in p["research"]:
                    await send(ws, {"type": "error", "message": "Уже изучено"})
                    continue
                if p["inventory"].get("scrap", 0) < 40:
                    await send(ws, {"type": "error", "message": "Нужно 40 скрапа"})
                    continue

                p["inventory"]["scrap"] -= 40
                p["research"].append(item)
                await save_player(pid)
                await send(ws, {
                    "type": "research",
                    "research": p["research"],
                    "inventory": p["inventory"],
                    "message": "Чертёж изучен",
                })

            elif event == "build":
                p = await require_player(ws, pid)
                if not p:
                    continue

                kind = data.get("kind")
                spec = BUILDINGS.get(kind)
                if not spec:
                    await send(ws, {"type": "error", "message": "Неизвестная постройка"})
                    continue

                pos = data.get("position", {})
                try:
                    x, y, z = float(pos["x"]), float(pos["y"]), float(pos["z"])
                    rotation = float(data.get("rotation", 0))
                except (KeyError, TypeError, ValueError):
                    await send(ws, {"type": "error", "message": "Неверная позиция"})
                    continue

                if any(abs(v) > 5000 for v in (x, y, z)):
                    await send(ws, {"type": "error", "message": "Позиция вне карты"})
                    continue
                if not has_cost(p["inventory"], spec["cost"]):
                    await send(ws, {"type": "error", "message": "Недостаточно дерева"})
                    continue

                pay(p["inventory"], spec["cost"])
                building = {
                    "id": str(uuid.uuid4()),
                    "owner_id": pid,
                    "kind": kind,
                    "x": x, "y": y, "z": z,
                    "rotation": rotation,
                    "level": 1,
                }
                world_objects[building["id"]] = building
                await save_building(building)
                await save_player(pid)
                await broadcast({"type": "building_added", "building": building})
                await send(ws, {"type": "inventory", "inventory": p["inventory"]})

            elif event == "upgrade":
                p = await require_player(ws, pid)
                if not p:
                    continue
                bid = data.get("id")
                building = world_objects.get(bid)
                if not building or building["owner_id"] != pid:
                    await send(ws, {"type": "error", "message": "Нельзя улучшить эту постройку"})
                    continue
                if building["level"] >= 3:
                    await send(ws, {"type": "error", "message": "Максимальный уровень"})
                    continue
                cost = {"stone": 100} if building["level"] == 1 else {"metal_ore": 150}
                if not has_cost(p["inventory"], cost):
                    await send(ws, {"type": "error", "message": "Недостаточно материалов"})
                    continue
                pay(p["inventory"], cost)
                building["level"] += 1
                await save_building(building)
                await save_player(pid)
                await broadcast({"type": "building_updated", "building": building})
                await send(ws, {"type": "inventory", "inventory": p["inventory"]})

            elif event == "demolish":
                p = await require_player(ws, pid)
                if not p:
                    continue
                bid = data.get("id")
                building = world_objects.get(bid)
                if not building or building["owner_id"] != pid:
                    await send(ws, {"type": "error", "message": "Можно убрать только свою постройку"})
                    continue
                world_objects.pop(bid, None)
                if db_pool:
                    async with db_pool.acquire() as conn:
                        await conn.execute("DELETE FROM world_buildings WHERE id=$1", bid)
                await broadcast({"type": "building_removed", "id": bid})

            elif event == "ping":
                await send(ws, {"type": "pong"})

            else:
                await send(ws, {"type": "error", "message": "Неизвестное действие"})

    except WebSocketDisconnect:
        pass
    finally:
        sockets.discard(ws)
        if joined:
            await save_player(pid)
            players.pop(pid, None)
            await broadcast({
                "type": "players",
                "players": [
                    {"id": p["id"], "nickname": p["nickname"]}
                    for p in players.values()
                ],
            })
