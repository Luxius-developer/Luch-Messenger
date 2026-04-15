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
            phone TEXT,
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
            recipient_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
            text TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            is_deleted BOOLEAN DEFAULT FALSE
        )
    ''')
    try:
        await conn.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS recipient_id INTEGER REFERENCES users(id) ON DELETE CASCADE")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_recipient ON messages(recipient_id)")
    except Exception as e:
        print(f"⚠️ Колонка recipient_id уже есть: {e}")
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
    # Таблица для версий приложения
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS app_version (
            id SERIAL PRIMARY KEY,
            stable_version TEXT NOT NULL,
            beta_version TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT NOW()
        )
    ''')
    # Вставляем начальные версии, если таблица пуста
    row = await conn.fetchval("SELECT COUNT(*) FROM app_version")
    if row == 0:
        await conn.execute("INSERT INTO app_version (stable_version, beta_version) VALUES ('1.0.0', '1.1.0-beta')")
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
    phone = data.get("phone", None)

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        if await conn.fetchval("SELECT 1 FROM users WHERE username=$1", username):
            return web.json_response({"error": "Username taken"}, status=400)
        if phone:
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

# ---------- ПОИСК ПОЛЬЗОВАТЕЛЕЙ ----------
async def search_users_handler(request):
    query = request.query.get("q", "").strip()
    if len(query) < 1:
        return web.json_response({"users": []})
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        if query.isdigit():
            rows = await conn.fetch(
                "SELECT id, username, full_name FROM users WHERE id = $1 OR username ILIKE $2 OR full_name ILIKE $2 LIMIT 20",
                int(query), f"%{query}%"
            )
        else:
            rows = await conn.fetch(
                "SELECT id, username, full_name FROM users WHERE username ILIKE $1 OR full_name ILIKE $1 LIMIT 20",
                f"%{query}%"
            )
        users = [dict(r) for r in rows]
    finally:
        await conn.close()
    return web.json_response({"users": users})

# ---------- СПИСОК ЧАТОВ ----------
async def chats_handler(request):
    user_id = request.query.get("user_id")
    if not user_id:
        return web.json_response({"error": "No user_id"}, status=400)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await conn.fetch("""
            SELECT DISTINCT u.id, u.username, u.full_name
            FROM messages m
            JOIN users u ON (u.id = m.sender_id OR u.id = m.recipient_id)
            WHERE (m.sender_id = $1 OR m.recipient_id = $1) AND u.id != $1
        """, int(user_id))
        chats = [dict(r) for r in rows]
    finally:
        await conn.close()
    return web.json_response({"chats": chats})

# ---------- ИСТОРИЯ СООБЩЕНИЙ ----------
async def messages_handler(request):
    token = request.query.get("token")
    if token != "test":
        return web.json_response({"error": "Unauth"}, status=401)
    user_id = request.query.get("user_id")
    recipient_id = request.query.get("recipient_id")
    if not user_id:
        return web.json_response({"error": "No user_id"}, status=400)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        if recipient_id:
            rows = await conn.fetch("""
                SELECT m.id, m.text, m.created_at,
                       u.id as uid, u.username, u.full_name, u.name_color, u.badge_url
                FROM messages m
                JOIN users u ON m.sender_id = u.id
                WHERE ((m.sender_id = $1 AND m.recipient_id = $2) OR (m.sender_id = $2 AND m.recipient_id = $1))
                  AND m.is_deleted = FALSE
                ORDER BY m.created_at DESC LIMIT 50
            """, int(user_id), int(recipient_id))
        else:
            rows = await conn.fetch("""
                SELECT m.id, m.text, m.created_at,
                       u.id as uid, u.username, u.full_name, u.name_color, u.badge_url
                FROM messages m
                JOIN users u ON m.sender_id = u.id
                WHERE m.recipient_id IS NULL AND m.is_deleted = FALSE
                ORDER BY m.created_at DESC LIMIT 50
            """)
        messages = []
        for r in reversed(rows):
            messages.append({
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
            })
    finally:
        await conn.close()
    return web.json_response(messages)

# ---------- ПРОФИЛЬ И РЕДАКТИРОВАНИЕ ----------
async def profile_handler(request):
    user_id = request.query.get("user_id")
    if not user_id:
        return web.json_response({"error": "Missing user_id"}, status=400)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        user = await conn.fetchrow(
            "SELECT id, username, full_name, phone, name_color, badge_url, is_admin, created_at FROM users WHERE id=$1",
            int(user_id)
        )
        if not user:
            return web.json_response({"error": "User not found"}, status=404)
        user_dict = dict(user)
        user_dict = json_serializable(user_dict)
    finally:
        await conn.close()
    return web.json_response(user_dict)

async def update_profile_handler(request):
    data = await request.json()
    user_id = data.get("user_id")
    new_username = data.get("username")
    new_full_name = data.get("full_name")
    if not user_id:
        return web.json_response({"error": "Missing user_id"}, status=400)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        if new_username:
            existing = await conn.fetchval("SELECT 1 FROM users WHERE username=$1 AND id!=$2", new_username, user_id)
            if existing:
                return web.json_response({"error": "Username already taken"}, status=400)
            await conn.execute("UPDATE users SET username=$1 WHERE id=$2", new_username, user_id)
        if new_full_name:
            await conn.execute("UPDATE users SET full_name=$1 WHERE id=$2", new_full_name, user_id)
    finally:
        await conn.close()
    return web.json_response({"status": "ok"})

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
        return web.json_response({"active": True, "plan": sub['plan_type'], "days_left": days_left, "expires": sub['end_date'].isoformat()})
    return web.json_response({"active": False})

# ---------- АДМИНКА ----------
async def admin_users_handler(request):
    admin_id = request.query.get("admin_id")
    if not await is_admin(int(admin_id)):
        return web.json_response({"error": "Forbidden"}, status=403)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await conn.fetch("SELECT id, username, full_name, phone, is_admin FROM users ORDER BY id")
        users = [dict(r) for r in rows]
    finally:
        await conn.close()
    return web.json_response({"users": users})

async def admin_update_user_handler(request):
    data = await request.json()
    admin_id = data.get("admin_id")
    if not await is_admin(int(admin_id)):
        return web.json_response({"error": "Forbidden"}, status=403)
    user_id = data.get("user_id")
    field = data.get("field")
    value = data.get("value")
    if field not in ["username", "full_name", "phone", "is_admin"]:
        return web.json_response({"error": "Invalid field"}, status=400)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute(f"UPDATE users SET {field}=$1 WHERE id=$2", value, user_id)
    finally:
        await conn.close()
    return web.json_response({"status": "ok"})

async def admin_set_subscription_handler(request):
    data = await request.json()
    admin_id = data.get("admin_id")
    if not await is_admin(int(admin_id)):
        return web.json_response({"error": "Forbidden"}, status=403)
    target_user_id = data.get("user_id")
    days = data.get("days")
    if not target_user_id or days is None:
        return web.json_response({"error": "Missing user_id or days"}, status=400)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        sub = await conn.fetchrow(
            "SELECT id, end_date FROM subscriptions WHERE user_id=$1 AND status='active' AND end_date > NOW() ORDER BY end_date DESC LIMIT 1",
            target_user_id
        )
        if sub:
            new_end = sub['end_date'] + timedelta(days=days)
            await conn.execute("UPDATE subscriptions SET end_date=$1 WHERE id=$2", new_end, sub['id'])
        else:
            new_end = datetime.now() + timedelta(days=days)
            await conn.execute(
                "INSERT INTO subscriptions (user_id, plan_type, end_date, status) VALUES ($1, 'admin', $2, 'active')",
                target_user_id, new_end
            )
    finally:
        await conn.close()
    return web.json_response({"status": "ok", "new_end_date": new_end.isoformat()})

# ---------- УПРАВЛЕНИЕ ВЕРСИЯМИ ----------
async def get_version_handler(request):
    # Получить версии из БД
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        row = await conn.fetchrow("SELECT stable_version, beta_version FROM app_version ORDER BY id DESC LIMIT 1")
        if row:
            return web.json_response({"stable": row["stable_version"], "beta": row["beta_version"]})
        else:
            return web.json_response({"stable": "1.0.0", "beta": "1.1.0-beta"})
    finally:
        await conn.close()

async def admin_set_version_handler(request):
    data = await request.json()
    admin_id = data.get("admin_id")
    if not await is_admin(int(admin_id)):
        return web.json_response({"error": "Forbidden"}, status=403)
    stable = data.get("stable")
    beta = data.get("beta")
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute("UPDATE app_version SET stable_version=$1, beta_version=$2, updated_at=NOW()", stable, beta)
    finally:
        await conn.close()
    return web.json_response({"status": "ok"})

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
                recipient_id = data.get("recipient_id")
                if not text:
                    continue
                conn = await asyncpg.connect(DATABASE_URL)
                try:
                    msg_id = await conn.fetchval(
                        "INSERT INTO messages (sender_id, recipient_id, text) VALUES ($1,$2,$3) RETURNING id",
                        uid, recipient_id, text
                    )
                    sender = await conn.fetchrow(
                        "SELECT id, username, full_name, name_color, badge_url FROM users WHERE id=$1",
                        uid
                    )
                finally:
                    await conn.close()
                sender_dict = dict(sender) if sender else {}
                sender_dict = json_serializable(sender_dict)
                if recipient_id:
                    targets = [uid, recipient_id]
                else:
                    targets = connected_clients.keys()
                for client_id in targets:
                    client = connected_clients.get(client_id)
                    if client:
                        try:
                            await client.send_json({
                                "type": "new_message",
                                "message": {
                                    "id": msg_id,
                                    "text": text,
                                    "sender": sender_dict
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
                            "UPDATE messages SET is_deleted=TRUE WHERE id=$1 AND sender_id=$2",
                            message_id, uid
                        )
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

# ---------- ЗАПУСК ----------
async def init_app():
    await init_db()
    app = web.Application()
    app.router.add_post("/auth/yandex", auth_handler)
    app.router.add_post("/auth/register", register_handler)
    app.router.add_get("/search-users", search_users_handler)
    app.router.add_get("/chats", chats_handler)
    app.router.add_get("/messages", messages_handler)
    app.router.add_get("/profile", profile_handler)
    app.router.add_post("/update-profile", update_profile_handler)
    app.router.add_post("/set-color", set_color_handler)
    app.router.add_post("/create-payment", create_payment_handler)
    app.router.add_post("/yoomoney-webhook", yoomoney_webhook)
    app.router.add_get("/subscription-status", subscription_status_handler)
    app.router.add_get("/admin/users", admin_users_handler)
    app.router.add_post("/admin/update-user", admin_update_user_handler)
    app.router.add_post("/admin/set-subscription-days", admin_set_subscription_handler)
    app.router.add_get("/app-version", get_version_handler)
    app.router.add_post("/admin/set-version", admin_set_version_handler)
    app.router.add_get("/ws", ws_handler)
    return app

app = init_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    web.run_app(app, port=port)
