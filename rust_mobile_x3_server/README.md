# Rust Mobile X3 — backend prototype

## Included
- FastAPI HTTP service and WebSocket endpoint `/ws`
- Online player presence (`join`, `get_players`, `ping`)
- `/health` and `/config`
- Rates: wood, stone, metal ore, sulfur ore and scrap x3; crafting x1

## Deploy on Render
1. Upload this folder to a GitHub repository.
2. In Render choose New + → Blueprint and select the repository.
3. Render reads `render.yaml` and deploys the web service.
4. Check `/health` and `/config`.
5. WebSocket URL: `wss://YOUR-SERVICE.onrender.com/ws`

Send after connecting:
`{"type":"join","nickname":"PlayerOne"}`

## Limitations
This is only a networking/backend prototype, not a complete game server.
Player presence is stored in memory and resets on restart. No accounts, inventory,
world simulation, combat, database persistence, anti-cheat, or Unity client yet.
Render free services may sleep, so this is for early testing, not a persistent
public game server. Add authentication, persistent storage, validation and
server-authoritative gameplay before public release.
