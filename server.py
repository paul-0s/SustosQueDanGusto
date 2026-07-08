"""
Servidor relay de salas para ScreamerApp.

Qué hace: cada cliente se conecta acá por WebSocket. Uno crea una sala
("create") y recibe un código de 5 caracteres. Los demás se unen con
"join" + ese código. Cuando alguien manda "scream", el servidor lo
reenvía a todos los demás conectados en esa misma sala.

Cómo correrlo:
  pip install websockets
  python server.py

Cómo hostearlo gratis (recomendado, para no depender de tu PC):
  1. Subí este archivo a un repo de GitHub (junto con un requirements.txt
     que tenga "websockets").
  2. Andá a https://render.com -> New -> Web Service -> conectá el repo.
  3. Build command: pip install -r requirements.txt
     Start command: python server.py
  4. Render te va a dar una URL tipo "screamer-relay.onrender.com".
     La URL de WebSocket para el cliente es: wss://screamer-relay.onrender.com
  5. Pegá esa URL en config.py (server_url) antes de repartir la app.
"""

import asyncio
import http
import json
import os
import random
import string

import websockets


async def health_check(connection, request):
    """Responde OK a los pings de salud (HEAD/GET normales) que manda Render,
    en vez de dejar que la librería websockets tire una excepción por no ser
    una conexión WebSocket real."""
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return connection.respond(http.HTTPStatus.OK, "OK\n")
    return None

ROOMS = {}  # room_code (str) -> set de conexiones websocket


def gen_code():
    while True:
        code = "".join(random.choices(string.ascii_uppercase + string.digits, k=5))
        if code not in ROOMS:
            return code


async def broadcast(room_code, message, exclude=None):
    for peer in list(ROOMS.get(room_code, [])):
        if peer is exclude:
            continue
        try:
            await peer.send(json.dumps(message))
        except Exception:
            pass


async def handler(ws):
    room_code = None
    print("[server] Nueva conexión entrante", flush=True)
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            action = msg.get("action")

            if action == "create":
                room_code = gen_code()
                ROOMS[room_code] = {ws}
                print(f"[server] Sala creada: {room_code}", flush=True)
                await ws.send(json.dumps({"action": "created", "room": room_code}))

            elif action == "join":
                code = (msg.get("room") or "").strip().upper()
                if code in ROOMS:
                    room_code = code
                    ROOMS[code].add(ws)
                    print(f"[server] Alguien se unió a la sala {code} (total: {len(ROOMS[code])})", flush=True)
                    await ws.send(json.dumps({"action": "joined", "room": code}))
                    await broadcast(code, {"action": "peer_joined"}, exclude=ws)
                else:
                    print(f"[server] Intento de unirse a sala inexistente: {code}", flush=True)
                    await ws.send(json.dumps({
                        "action": "error",
                        "message": "Sala no encontrada. Revisa el código."
                    }))

            elif action == "scream":
                if room_code:
                    print(f"[server] Screamer disparado en sala {room_code}", flush=True)
                    await broadcast(room_code, {"action": "scream"}, exclude=ws)

    finally:
        print("[server] Conexión cerrada", flush=True)
        if room_code and room_code in ROOMS:
            ROOMS[room_code].discard(ws)
            if not ROOMS[room_code]:
                del ROOMS[room_code]


async def main():
    port = int(os.environ.get("PORT", 8765))  # Render inyecta PORT solo
    async with websockets.serve(handler, "0.0.0.0", port, process_request=health_check):
        print(f"Servidor relay corriendo en el puerto {port}", flush=True)
        await asyncio.Future()  # corre para siempre


if __name__ == "__main__":
    asyncio.run(main())
