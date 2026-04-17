import os
import asyncio
import sys
import time
import json
import hashlib
import uuid
import mimetypes
from datetime import datetime, timedelta
from aiohttp import web
import aiohttp
import asyncpg
from yoomoney import Quickpay
import aiofiles

# ---------- КОНФИГУРАЦИЯ ----------
YANDEX_CLIENT_ID = "102bb468a84f4d62a52520f715aea194"
YANDEX_CLIENT_SECRET = "a2e48310b6404262bb9d37c1a2405039"
BASE_URL = os.environ.get("BASE_URL", "https://luch-messenger-production.up.railway.app")
REDIRECT_URI = f"{BASE_URL}/auth/yandex/callback"

YOOMONEY_RECEIVER = "4100118812633088"
YOOMONEY_TOKEN = os.environ.get("YOOMONEY_TOKEN", "")

PRICES = {"month": 150, "quarter": 400, "year": 1500}

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("❌ DATABASE_URL не задан!")
    sys.exit(1)

MAX_FILE_SIZE = 50 * 1024 * 1024
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

connected_clients = {}

def json_serializable(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [json_serializable(i) for i in obj]
    return obj

async def init_db():
    print("🔧 Инициализация базы данных...")
    conn = await asyncpg.connect(DATABASE_URL)
    # ... (оставь свою реализацию init_db без изменений)
    await conn.close()
    print("✅ База данных готова")

# ---------- WebSocket ----------
async def ws_handler(request):
    print(f"[WS] Incoming request, Upgrade header: {request.headers.get('Upgrade')}")
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    print("[WS] WebSocket upgrade successful")

    if request.query.get("token") != "test":
        await ws.close()
        return ws

    msg = await ws.receive()
    data = json.loads(msg.data)
    if data.get("type") != "auth":
        await ws.close()
        return ws

    uid = data["user_id"]
    connected_clients[uid] = ws
    print(f"[WS] {uid} connected")

    try:
        async for msg in ws:
            data = json.loads(msg.data)
            if data["action"] == "send":
                text = data.get("text")
                recipient_id = data.get("recipient_id")
                group_id = data.get("group_id")
                file_info = data.get("file_info")
                if not text and not file_info:
                    continue

                conn = await asyncpg.connect(DATABASE_URL)
                try:
                    file_size_val = None
                    if file_info and file_info.get("file_size") is not None:
                        file_size_val = int(file_info["file_size"])

                    msg_id = await conn.fetchval(
                        "INSERT INTO messages (sender_id, recipient_id, group_id, text, file_url, file_name, file_size, file_type, file_hash) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id",
                        uid, recipient_id, group_id, text,
                        file_info.get("file_url") if file_info else None,
                        file_info.get("file_name") if file_info else None,
                        file_size_val,
                        file_info.get("file_type") if file_info else None,
                        file_info.get("file_hash") if file_info else None
                    )
                    sender = await conn.fetchrow(
                        "SELECT id, username, full_name, name_color, badge_url FROM users WHERE id=$1",
                        uid
                    )
                finally:
                    await conn.close()

                sender_dict = dict(sender) if sender else {}
                sender_dict = json_serializable(sender_dict)

                message_obj = {
                    "id": msg_id,
                    "text": text,
                    "sender": sender_dict,
                    "recipient_id": recipient_id,
                    "group_id": group_id,
                    "created_at": datetime.now().isoformat()
                }
                if file_info:
                    message_obj["file_info"] = file_info

                targets = connected_clients.keys() if group_id else ([uid, recipient_id] if recipient_id else connected_clients.keys())
                for client_id in targets:
                    client = connected_clients.get(client_id)
                    if client:
                        try:
                            await client.send_json({"type": "new_message", "message": message_obj})
                        except:
                            pass
            elif data["action"] == "delete":
                message_id = data.get("message_id")
                if message_id:
                    conn = await asyncpg.connect(DATABASE_URL)
                    try:
                        await conn.execute("UPDATE messages SET is_deleted=TRUE WHERE id=$1 AND sender_id=$2", message_id, uid)
                    finally:
                        await conn.close()
                    for client in connected_clients.values():
                        try:
                            await client.send_json({"type": "delete_message", "message_id": message_id})
                        except:
                            pass
    finally:
        connected_clients.pop(uid, None)
        print(f"[WS] {uid} disconnected")
    return ws

# ---------- Healthcheck ----------
async def health_check(request):
    return web.Response(text="OK")

# ---------- Остальные handler'ы оставь без изменений ----------
# (auth_handler, register_handler, messages_handler, upload_handler и т.д.)

async def init_app():
    await init_db()
    app = web.Application()
    # WebSocket
    app.router.add_get("/ws", ws_handler)
    # Healthcheck
    app.router.add_get("/health", health_check)
    # Остальные маршруты
    app.router.add_post("/auth/yandex", auth_handler)
    app.router.add_post("/auth/register", register_handler)
    # ... (все твои маршруты)
    app.router.add_static('/uploads', UPLOAD_FOLDER, name='uploads')
    return app

app = init_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    web.run_app(app, port=port)
