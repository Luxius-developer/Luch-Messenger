import os
import asyncio
import sys
import time
import json
from datetime import datetime, timedelta
from aiohttp import web
import aiohttp
import asyncpg
from yoomoney import Quickpay

# ============= КОНФИГУРАЦИЯ =============
# Данные Яндекс OAuth
YANDEX_CLIENT_ID = "102bb468a84f4d62a52520f715aea194"
YANDEX_CLIENT_SECRET = "a2e48310b6404262bb9d37c1a2405039"
# !!! ЗАМЕНИТЕ ЭТОТ URL НА АДРЕС ВАШЕГО СЕРВЕРА ПОСЛЕ ДЕПЛОЯ !!!
BASE_URL = "https://luch-messenger.onrender.com"
REDIRECT_URI = f"{BASE_URL}/auth/yandex/callback"

# Данные ЮMoney (ваши)
YOOMONEY_RECEIVER = "4100118812633088"
YOOMONEY_TOKEN = "4100118812633088.2E341BE9B7E250E2EBE11E8B3B61E9D6EB9A59D7E1DE2D0F18C915B68B8AF6CFCAEA05A49700D54E77C7CDD3A744BC41A06C6CDC3FF7F7D3521B82F381951BFC366C8894B12DB79F5CEBE330F70C96CBC4671C9DE9E6F88D4879737AEACACE3BDD2B4473CFE0949803734BA2C4A65B02E1FC9ABE44831CBE53735679C363105A"

PRICES = {"month": 150, "quarter": 400, "year": 1500}

# Читаем адрес базы данных из переменной окружения, которую создаст Render
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("❌ ОШИБКА: Переменная окружения DATABASE_URL не установлена!")
    sys.exit(1)
# ========================================

connected_clients = {}

# ---------- БАЗА ДАННЫХ ----------
async def init_db():
    print("🔧 Инициализация базы данных PostgreSQL...")
    conn = await asyncpg.connect(DATABASE_URL)

    # Таблица пользователей
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

    # Таблица сообщений
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            sender_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            text TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            is_deleted BOOLEAN DEFAULT FALSE
        )
    ''')

    # Таблица подписок
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

    # Таблица платежей
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS pending_payments (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
            label TEXT UNIQUE NOT NULL,
            plan_type TEXT NOT NULL,
            amount DECIMAL NOT NULL,
            status TEXT DEFAULT 'pending'
        )
    ''')

    await conn.close()
    print("✅ База данных готова")

# ---------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ----------
async def check_subscription(user_id):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        sub = await conn.fetchval('''
            SELECT 1 FROM subscriptions
            WHERE user_id = $1 AND status = 'active' AND end_date > NOW()
        ''', user_id)
        return sub is not None
    finally:
        await conn.close()

async def is_admin(user_id):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        result = await conn.fetchval('''
            SELECT is_admin FROM users WHERE id = $1
        ''', user_id)
        return result == 1
    finally:
        await conn.close()

# ---------- ОБРАБОТЧИКИ АВТОРИЗАЦИИ ----------
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
    user = await conn.fetchrow("SELECT * FROM users WHERE yandex_id = $1", yandex_id)
    await conn.close()

    if user:
        return web.json_response({
            "status": "ok",
            "jwt": "test",
            "user": dict(user)
        })
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

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # Проверки уникальности
        if await conn.fetchval("SELECT 1 FROM users WHERE username = $1", username):
            return web.json_response({"error": "Username already taken"}, status=400)
        if await conn.fetchval("SELECT 1 FROM users WHERE phone = $1", phone):
            return web.json_response({"error": "Phone already registered"}, status=400)

        # Если username == "luxius", делаем админом
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
        await conn.execute(
            "INSERT INTO pending_payments (user_id, label, plan_type, amount, status) VALUES ($1,$2,$3,$4,'pending')",
            user_id, label, plan_type, amount
        )
    finally:
        await conn.close()

    quickpay = Quickpay(
        receiver=YOOMONEY_RECEIVER,
        quickpay_form="shop",
        targets=f"Подписка {plan_type}",
        paymentType="AC",
        sum=amount,
        label=label
    )

    return web.json_response({
        "payment_url": quickpay.redirected_url,
        "label": label
    })

async def yoomoney_webhook(request):
    data = await request.post()
    if data.get("notification_type") in ("card-incoming", "p2p-incoming"):
        label = data.get("label")
        amount = float(data.get("amount"))

        conn = await asyncpg.connect(DATABASE_URL)
        try:
            payment = await conn.fetchrow(
                "SELECT * FROM pending_payments WHERE label = $1 AND status = 'pending'",
                label
            )
            if payment:
                if abs(amount - payment['amount']) > 0.01:
                    await conn.execute("UPDATE pending_payments SET status = 'amount_mismatch' WHERE label = $1", label)
                    return web.Response(status=400)

                user_id = payment['user_id']
                plan_type = payment['plan_type']
                durations = {"month": 30, "quarter": 90, "year": 365}
                days = durations.get(plan_type, 30)
                end_date = (datetime.now() + timedelta(days=days)).isoformat()

                await conn.execute(
                    "INSERT INTO subscriptions (user_id, plan_type, end_date, status) VALUES ($1,$2,$3,'active')",
                    user_id, plan_type, end_date
                )
                await conn.execute("UPDATE pending_payments SET status = 'completed' WHERE label = $1", label)
        finally:
            await conn.close()

    return web.Response(status=200)

async def subscription_status_handler(request):
    user_id = request.query.get("user_id")
    if not user_id:
        return web.json_response({"error": "Missing user_id"}, status=400)

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        sub = await conn.fetchrow('''
            SELECT plan_type, end_date FROM subscriptions
            WHERE user_id = $1 AND status = 'active' AND end_date > NOW()
            ORDER BY end_date DESC LIMIT 1
        ''', int(user_id))
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

# ---------- АДМИН-ЭНДПОИНТ для активации подписки ----------
async def admin_activate_subscription(request):
    data = await request.json()
    admin_id = data.get("admin_id")
    target_username = data.get("username")
    plan_type = data.get("plan")

    if not await is_admin(admin_id):
        return web.json_response({"error": "Forbidden"}, status=403)

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        user = await conn.fetchrow("SELECT id FROM users WHERE username = $1", target_username)
        if not user:
            return web.json_response({"error": "User not found"}, status=404)
        user_id = user["id"]

        if plan_type not in PRICES:
            return web.json_response({"error": "Invalid plan"}, status=400)

        durations = {"month": 30, "quarter": 90, "year": 365}
        days = durations.get(plan_type, 30)
        end_date = (datetime.now() + timedelta(days=days)).isoformat()

        await conn.execute(
            "INSERT INTO subscriptions (user_id, plan_type, end_date, status) VALUES ($1,$2,$3,'active')",
            user_id, plan_type, end_date
        )
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
        await conn.execute("UPDATE users SET name_color = $1 WHERE id = $2", color, user_id)
    finally:
        await conn.close()

    return web.json_response({"status": "ok", "color": color})

async def get_profile_handler(request):
    user_id = request.query.get("user_id")
    if not user_id:
        return web.json_response({"error": "Missing user_id"}, status=400)

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        user = await conn.fetchrow(
            "SELECT id, username, full_name, name_color, badge_url, is_admin FROM users WHERE id = $1",
            int(user_id)
        )
    finally:
        await conn.close()

    if user:
        return web.json_response(dict(user))
    else:
        return web.json_response({"error": "User not found"}, status=404)

# ---------- ПОИСК ПОЛЬЗОВАТЕЛЕЙ ----------
async def search_users_handler(request):
    query = request.query.get("q", "")
    if len(query) < 2:
        return web.json_response({"users": []})

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await conn.fetch(
            "SELECT id, username, full_name FROM users WHERE username ILIKE $1 OR full_name ILIKE $1 LIMIT 20",
            f"%{query}%"
        )
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
    print(f"[WS] Пользователь {uid} подключился")

    try:
        async for msg in ws:
            data = json.loads(msg.data)
            if data["action"] == "send":
                text = data.get("text")
                if not text:
                    continue

                conn = await asyncpg.connect(DATABASE_URL)
                try:
                    msg_id = await conn.fetchval(
                        "INSERT INTO messages (sender_id, text) VALUES ($1,$2) RETURNING id",
                        uid, text
                    )
                    sender = await conn.fetchrow(
                        "SELECT id, username, full_name, name_color, badge_url FROM users WHERE id=$1",
                        uid
                    )
                finally:
                    await conn.close()

                for client in connected_clients.values():
                    try:
                        await client.send_json({
                            "type": "new_message",
                            "message": {
                                "id": msg_id,
                                "text": text,
                                "sender": dict(sender)
                            }
                        })
                    except:
                        pass

            elif data["action"] == "delete":
                message_id = data.get("message_id")
                if message_id:
                    conn = await asyncpg.connect(DATABASE_URL)
                    try:
                        await conn.execute(
                            "UPDATE messages SET is_deleted = TRUE WHERE id = $1 AND sender_id = $2",
                            message_id, uid
                        )
                    finally:
                        await conn.close()

                    for client in connected_clients.values():
                        try:
                            await client.send_json({
                                "type": "delete_message",
                                "message_id": message_id
                            })
                        except:
                            pass
    finally:
        connected_clients.pop(uid, None)
        print(f"[WS] Пользователь {uid} отключился")
    return ws

async def messages_handler(request):
    if request.query.get("token") != "test":
        return web.json_response({"error": "Unauth"}, status=401)

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await conn.fetch('''
            SELECT m.id, m.text, m.created_at,
                   u.id as uid, u.username, u.full_name, u.name_color, u.badge_url
            FROM messages m JOIN users u ON m.sender_id = u.id
            WHERE m.is_deleted = FALSE
            ORDER BY m.created_at DESC LIMIT 50
        ''')
        messages = [{
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
        } for r in reversed(rows)]
    finally:
        await conn.close()
    return web.json_response(messages)

# ---------- ЗАПУСК ----------
async def init_app():
    await init_db()
    app = web.Application()
    app.router.add_post("/auth/yandex", auth_handler)
    app.router.add_post("/auth/register", register_handler)
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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(init_app())
    web.run_app(app, port=port)
