import os
import asyncio
import sys
import time
import json
import re
from datetime import datetime, timedelta
from aiohttp import web
import aiohttp
import asyncpg
from yoomoney import Quickpay

# ============= КОНФИГУРАЦИЯ =============
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
# ========================================

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
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            yandex_id TEXT UNIQUE NOT NULL,
            username TEXT UNIQUE NOT NULL,
            full_name TEXT NOT NULL,
            phone TEXT UNIQUE NOT NULL,
            name_color TEXT DEFAULT NULL,
            badge_url TEXT DEFAULT NULL,
            last_message_time TIMESTAMP DEFAULT NULL,
            is_admin INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW()
        )
    ''')
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            sender_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            text TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            is_deleted BOOLEAN DEFAULT FALSE
        )
    ''')
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS subscriptions (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
            plan_type TEXT NOT NULL,
            start_date TIMESTAMP DEFAULT NOW(),
            end_date TIMESTAMP NOT NULL,
            status TEXT DEFAULT 'active',
            auto_renew BOOLEAN DEFAULT FALSE
        )
    ''')
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS pending_payments (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
            label TEXT UNIQUE NOT NULL,
            plan_type TEXT NOT NULL,
            amount DECIMAL NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT NOW()
        )
    ''')
    await conn.close()
    print("✅ База данных готова")

async def check_subscription(user_id):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        result = await conn.fetchval(
            "SELECT 1 FROM subscriptions WHERE user_id=$1 AND status='active' AND end_date > NOW()",
            user_id
        )
        return result is not None
    finally:
        await conn.close()

async def is_admin(user_id):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        result = await conn.fetchval("SELECT is_admin FROM users WHERE id=$1", user_id)
        return result == 1
    finally:
        await conn.close()

# ---------- АВТОРИЗАЦИЯ ----------
async def auth_handler(request):
    data = await request.json()
    code = data.get("code")
    if not code:
        return web.json_response({"error": "No code"}, status=400)

    async with aiohttp.ClientSession() as session:
        token_resp = await session.post(
            "https://oauth.yandex.ru/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": YANDEX_CLIENT_ID,
                "client_secret": YANDEX_CLIENT_SECRET
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        token_data = await token_resp.json()
        access_token = token_data.get("access_token")
        if not access_token:
            return web.json_response({"error": "No token"}, status=400)

        user_resp = await session.get(
            "https://login.yandex.ru/info?format=json",
            headers={"Authorization": f"OAuth {access_token}"}
        )
        user_data = await user_resp.json()
        yandex_id = str(user_data["id"])

    conn = await asyncpg.connect(DATABASE_URL)
    user = await conn.fetchrow("SELECT * FROM users WHERE yandex_id=$1", yandex_id)
    await conn.close()

    if user:
        user_dict = dict(user)
        user_dict = json_serializable(user_dict)
        return web.json_response({"status": "ok", "jwt": "test", "user": user_dict})
    else:
        full_name = f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip()
        return web.json_response({
            "status": "need_registration",
            "yandex_id": yandex_id,
            "email": user_data.get("default_email"),
            "full_name": full_name
        })

async def register_handler(request):
    data = await request.json()
    yandex_id = data["yandex_id"]
    username = data["username"]
    full_name = data["full_name"]
    phone = data["phone"]

    # Валидация
    if not re.match(r'^[a-zA-Z0-9_]{3,20}$', username):
        return web.json_response({"error": "Username must be 3-20 letters, digits or underscore"}, status=400)
    if not full_name.strip():
        return web.json_response({"error": "Full name required"}, status=400)
    if not re.match(r'^\+?\d{5,15}$', phone):
        return web.json_response({"error": "Invalid phone number"}, status=400)

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        if await conn.fetchval("SELECT 1 FROM users WHERE username=$1", username):
            return web.json_response({"error": "Username taken"}, status=400)
        if await conn.fetchval("SELECT 1 FROM users WHERE phone=$1", phone):
            return web.json_response({"error": "Phone taken"}, status=400)
        is_admin_flag = 1 if username == "luxius" else 0
        user_id = await conn.fetchval(
            "INSERT INTO users (yandex_id, username, full_name, phone, is_admin) VALUES ($1,$2,$3,$4,$5) RETURNING id",
            yandex_id, username, full_name, phone, is_admin_flag
        )
    finally:
        await conn.close()

    return web.json_response({
        "status": "ok",
        "jwt": "test",
        "user": {
            "id": user_id,
            "username": username,
            "full_name": full_name,
            "phone": phone,
            "is_admin": is_admin_flag
        }
    })

# ---------- ПРОФИЛЬ ----------
async def update_profile_handler(request):
    data = await request.json()
    user_id = data.get("user_id")
    full_name = data.get("full_name")
    username = data.get("username")
    phone = data.get("phone")

    if not user_id:
        return web.json_response({"error": "Missing user_id"}, status=400)

    # Валидация
    if username and not re.match(r'^[a-zA-Z0-9_]{3,20}$', username):
        return web.json_response({"error": "Invalid username"}, status=400)
    if full_name is not None and not full_name.strip():
        return web.json_response({"error": "Full name required"}, status=400)
    if phone and not re.match(r'^\+?\d{5,15}$', phone):
        return web.json_response({"error": "Invalid phone number"}, status=400)

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        if username:
            # Проверка уникальности
            existing = await conn.fetchval("SELECT id FROM users WHERE username=$1 AND id!=$2", username, user_id)
            if existing:
                return web.json_response({"error": "Username already taken"}, status=400)
        if phone:
            existing = await conn.fetchval("SELECT id FROM users WHERE phone=$1 AND id!=$2", phone, user_id)
            if existing:
                return web.json_response({"error": "Phone already taken"}, status=400)

        # Формируем запрос
        updates = []
        params = []
        if full_name is not None:
            updates.append(f"full_name = ${len(params)+1}")
            params.append(full_name)
        if username is not None:
            updates.append(f"username = ${len(params)+1}")
            params.append(username)
        if phone is not None:
            updates.append(f"phone = ${len(params)+1}")
            params.append(phone)
        params.append(user_id)

        if updates:
            query = f"UPDATE users SET {', '.join(updates)} WHERE id = ${len(params)}"
            await conn.execute(query, *params)
    finally:
        await conn.close()

    return web.json_response({"status": "ok", "message": "Profile updated"})

# ---------- АДМИН: ПОЛУЧЕНИЕ СПИСКА ПОЛЬЗОВАТЕЛЕЙ ----------
async def admin_users_handler(request):
    admin_id = request.query.get("admin_id")
    if not admin_id or not await is_admin(int(admin_id)):
        return web.json_response({"error": "Forbidden"}, status=403)

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await conn.fetch("SELECT id, username, full_name, phone FROM users ORDER BY id")
        users = [dict(r) for r in rows]
    finally:
        await conn.close()
    return web.json_response({"users": users})

# ---------- АДМИН: РЕДАКТИРОВАНИЕ ПОЛЬЗОВАТЕЛЯ ----------
async def admin_edit_user_handler(request):
    data = await request.json()
    admin_id = data.get("admin_id")
    target_id = data.get("target_id")
    field = data.get("field")
    value = data.get("value")

    if not admin_id or not await is_admin(int(admin_id)):
        return web.json_response({"error": "Forbidden"}, status=403)

    if field not in ["phone", "full_name", "username"]:
        return web.json_response({"error": "Invalid field"}, status=400)

    # Валидация
    if field == "phone" and not re.match(r'^\+?\d{5,15}$', value):
        return web.json_response({"error": "Invalid phone number"}, status=400)
    if field == "username" and not re.match(r'^[a-zA-Z0-9_]{3,20}$', value):
        return web.json_response({"error": "Invalid username"}, status=400)
    if field == "full_name" and not value.strip():
        return web.json_response({"error": "Full name required"}, status=400)

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # Проверка уникальности для username/phone
        if field == "username":
            existing = await conn.fetchval("SELECT id FROM users WHERE username=$1 AND id!=$2", value, target_id)
            if existing:
                return web.json_response({"error": "Username already taken"}, status=400)
        if field == "phone":
            existing = await conn.fetchval("SELECT id FROM users WHERE phone=$1 AND id!=$2", value, target_id)
            if existing:
                return web.json_response({"error": "Phone already taken"}, status=400)

        await conn.execute(f"UPDATE users SET {field}=$1 WHERE id=$2", value, target_id)
    finally:
        await conn.close()

    return web.json_response({"status": "ok"})

# ---------- ПОДПИСКИ ----------
async def create_payment_handler(request):
    data = await request.json()
    user_id = data.get("user_id")
    plan_type = data.get("plan")
    if plan_type not in PRICES:
        return web.json_response({"error": "Invalid plan"}, status=400)
    amount = PRICES[plan_type]
    label = f"pay_{user_id}_{int(time.time())}"
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute("INSERT INTO pending_payments (user_id, label, plan_type, amount, status) VALUES ($1,$2,$3,$4,'pending')", user_id, label, plan_type, amount)
    finally:
        await conn.close()
    quickpay = Quickpay(receiver=YOOMONEY_RECEIVER, quickpay_form="shop", targets=f"Подписка {plan_type}", paymentType="AC", sum=amount, label=label)
    return web.json_response({"payment_url": quickpay.redirected_url, "label": label})

async def yoomoney_webhook(request):
    data = await request.post()
    if data.get("notification_type") in ("card-incoming", "p2p-incoming"):
        label = data.get("label")
        amount = float(data.get("amount"))
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            payment = await conn.fetchrow("SELECT * FROM pending_payments WHERE label=$1 AND status='pending'", label)
            if payment and abs(amount - payment['amount']) <= 0.01:
                user_id = payment['user_id']
                plan_type = payment['plan_type']
                durations = {"month":30, "quarter":90, "year":365}
                days = durations.get(plan_type,30)
                end_date = (datetime.now() + timedelta(days=days)).isoformat()
                await conn.execute("INSERT INTO subscriptions (user_id, plan_type, end_date, status) VALUES ($1,$2,$3,'active')", user_id, plan_type, end_date)
                await conn.execute("UPDATE pending_payments SET status='completed' WHERE label=$1", label)
        finally:
            await conn.close()
    return web.Response(status=200)

async def subscription_status_handler(request):
    user_id = request.query.get("user_id")
    if not user_id:
        return web.json_response({"error": "Missing user_id"}, status=400)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        sub = await conn.fetchrow("SELECT plan_type, end_date FROM subscriptions WHERE user_id=$1 AND status='active' AND end_date>NOW() ORDER BY end_date DESC LIMIT 1", int(user_id))
    finally:
        await conn.close()
    if sub:
        days_left = (sub['end_date'] - datetime.now()).days
        return web.json_response({
            "active": True,
            "plan": sub['plan_type'],
            "days_left": days_left,
            "expires": sub['end_date'].isoformat()
        })
    return web.json_response({"active": False})

# ---------- АДМИН: АКТИВАЦИЯ ПОДПИСКИ ----------
async def admin_activate_subscription(request):
    data = await request.json()
    admin_id = data.get("admin_id")
    if not await is_admin(admin_id):
        return web.json_response({"error": "Forbidden"}, status=403)
    target_username = data.get("username")
    plan_type = data.get("plan")
    if plan_type not in PRICES:
        return web.json_response({"error": "Invalid plan"}, status=400)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        user = await conn.fetchrow("SELECT id FROM users WHERE username=$1", target_username)
        if not user:
            return web.json_response({"error": "User not found"}, status=404)
        user_id = user["id"]
        durations = {"month":30, "quarter":90, "year":365}
        days = durations.get(plan_type,30)
        end_date = (datetime.now() + timedelta(days=days)).isoformat()
        await conn.execute("INSERT INTO subscriptions (user_id, plan_type, end_date, status) VALUES ($1,$2,$3,'active')", user_id, plan_type, end_date)
    finally:
        await conn.close()
    return web.json_response({"status": "ok", "message": f"Subscription activated for {target_username}"})

# ---------- КАСТОМИЗАЦИЯ ----------
async def set_color_handler(request):
    data = await request.json()
    user_id = data.get("user_id")
    color = data.get("color")
    if not await check_subscription(user_id):
        return web.json_response({"error": "Subscription required"}, status=403)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute("UPDATE users SET name_color=$1 WHERE id=$2", color, user_id)
    finally:
        await conn.close()
    return web.json_response({"status": "ok", "color": color})

async def get_profile_handler(request):
    user_id = request.query.get("user_id")
    if not user_id:
        return web.json_response({"error": "Missing user_id"}, status=400)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        user = await conn.fetchrow("SELECT id, username, full_name, name_color, badge_url, is_admin, phone FROM users WHERE id=$1", int(user_id))
    finally:
        await conn.close()
    if user:
        user_dict = dict(user)
        user_dict = json_serializable(user_dict)
        return web.json_response(user_dict)
    return web.json_response({"error": "User not found"}, status=404)

# ---------- ПОИСК ----------
async def search_users_handler(request):
    query = request.query.get("q", "")
    if len(query) < 2:
        return web.json_response({"users": []})
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await conn.fetch("SELECT id, username, full_name FROM users WHERE username ILIKE $1 OR full_name ILIKE $1 LIMIT 20", f"%{query}%")
        users = [dict(r) for r in rows]
    finally:
        await conn.close()
    return web.json_response({"users": users})

# ---------- WEBSOCKET ----------
async def ws_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
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
                if not text:
                    continue
                conn = await asyncpg.connect(DATABASE_URL)
                try:
                    msg_id = await conn.fetchval("INSERT INTO messages (sender_id, text) VALUES ($1,$2) RETURNING id", uid, text)
                    sender = await conn.fetchrow("SELECT id, username, full_name, name_color, badge_url FROM users WHERE id=$1", uid)
                finally:
                    await conn.close()
                sender_dict = dict(sender) if sender else {}
                sender_dict = json_serializable(sender_dict)
                for client in connected_clients.values():
                    try:
                        await client.send_json({"type": "new_message", "message": {"id": msg_id, "text": text, "sender": sender_dict}})
                    except:
                        pass
            elif data["action"] == "delete":
                message_id = data.get("message_id")
                if message_id:
                    conn = await asyncpg.connect(DATABASE_URL)
                    try:
                        result = await conn.execute(
                            "UPDATE messages SET is_deleted = TRUE WHERE id = $1 AND sender_id = $2",
                            message_id, uid
                        )
                        # result like "UPDATE 1" or "UPDATE 0"
                        if result == "UPDATE 1":
                            for client in connected_clients.values():
                                try:
                                    await client.send_json({"type": "delete_message", "message_id": message_id})
                                except:
                                    pass
                    finally:
                        await conn.close()
    finally:
        connected_clients.pop(uid, None)
        print(f"[WS] {uid} disconnected")
    return ws

async def messages_handler(request):
    if request.query.get("token") != "test":
        return web.json_response({"error": "Unauth"}, status=401)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await conn.fetch('''
            SELECT m.id, m.text, m.created_at,
                   u.id as uid, u.username, u.full_name, u.name_color, u.badge_url
            FROM messages m JOIN users u ON m.sender_id=u.id
            WHERE m.is_deleted=FALSE
            ORDER BY m.created_at DESC LIMIT 50
        ''')
        messages = []
        for r in reversed(rows):
            msg = {
                "id": r["id"],
                "text": r["text"],
                "created_at": r["created_at"].isoformat(),
                "sender": {
                    "id": r["uid"],
                    "username": r["username"],
                    "full_name": r["full_name"],
                    "name_color": r["name_color"],
                    "badge_url": r["badge_url"]
                }
            }
            messages.append(msg)
    finally:
        await conn.close()
    return web.json_response(messages)

# ---------- ЗАПУСК ----------
async def init_app():
    await init_db()
    app = web.Application()
    app.router.add_post("/auth/yandex", auth_handler)
    app.router.add_post("/auth/register", register_handler)
    app.router.add_post("/update-profile", update_profile_handler)
    app.router.add_get("/admin/users", admin_users_handler)
    app.router.add_post("/admin/edit-user", admin_edit_user_handler)
    app.router.add_post("/create-payment", create_payment_handler)
    app.router.add_post("/yoomoney-webhook", yoomoney_webhook)
    app.router.add_get("/subscription-status", subscription_status_handler)
    app.router.add_post("/admin/activate-subscription", admin_activate_subscription)
    app.router.add_post("/set-color", set_color_handler)
    app.router.add_get("/profile", get_profile_handler)
    app.router.add_get("/search-users", search_users_handler)
    app.router.add_get("/ws", ws_handler)
    app.router.add_get("/messages", messages_handler)
    return app

app = init_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    web.run_app(app, port=port)
