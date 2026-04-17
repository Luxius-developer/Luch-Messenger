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

MAX_FILE_SIZE = 50 * 1024 * 1024
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
FILE_RETENTION_DAYS = 30
REPUTATION_THRESHOLD = 3
VIRUSTOTAL_API_KEY = os.environ.get("VIRUSTOTAL_API_KEY", "")
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

    # Таблица users
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
            bio TEXT DEFAULT NULL,
            hide_phone BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW()
        )
    ''')

    # Таблица messages (с групповыми чатами и файлами)
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            sender_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            recipient_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
            group_id INTEGER,
            text TEXT,
            file_url TEXT,
            file_name TEXT,
            file_size INTEGER,
            file_type TEXT,
            file_hash TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            is_deleted BOOLEAN DEFAULT FALSE
        )
    ''')

    # Добавляем недостающие колонки, если таблица уже существовала
    for col in ['group_id', 'file_hash', 'file_url', 'file_name', 'file_size', 'file_type']:
        try:
            await conn.execute(f"ALTER TABLE messages ADD COLUMN IF NOT EXISTS {col} TEXT")
        except:
            pass

    # Индексы
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_group ON messages(group_id)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_recipient ON messages(recipient_id)")

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

    # Таблица временных платежей
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

    # Таблица версий приложения
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS app_version (
            id SERIAL PRIMARY KEY,
            stable_version TEXT NOT NULL,
            beta_version TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT NOW()
        )
    ''')
    row = await conn.fetchval("SELECT COUNT(*) FROM app_version")
    if row == 0:
        await conn.execute("INSERT INTO app_version (stable_version, beta_version) VALUES ('1.0.0', '1.1.0-beta')")

    # Таблица репутации файлов
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS file_reputation (
            id SERIAL PRIMARY KEY,
            file_hash TEXT UNIQUE NOT NULL,
            status TEXT DEFAULT 'unknown',
            vt_report TEXT DEFAULT NULL,
            complaints INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    ''')

    # Таблица жалоб на файлы
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS file_complaints (
            id SERIAL PRIMARY KEY,
            file_hash TEXT NOT NULL,
            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
            reason TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    ''')

    # Таблица событий безопасности (лента)
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS security_events (
            id SERIAL PRIMARY KEY,
            event_type TEXT NOT NULL,
            file_hash TEXT,
            file_name TEXT,
            message TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    ''')

    await conn.close()
    print("✅ База данных готова")

# ---------- Вспомогательные функции ----------
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

async def get_file_reputation(file_hash):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        row = await conn.fetchrow("SELECT status, complaints FROM file_reputation WHERE file_hash=$1", file_hash)
        if row:
            return {"status": row["status"], "complaints": row["complaints"]}
        return None
    finally:
        await conn.close()

async def update_file_reputation(file_hash, status, complaints=None):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        if complaints is not None:
            await conn.execute(
                "INSERT INTO file_reputation (file_hash, status, complaints, updated_at) VALUES ($1,$2,$3,NOW()) ON CONFLICT (file_hash) DO UPDATE SET status=$2, complaints=$3, updated_at=NOW()",
                file_hash, status, complaints
            )
        else:
            await conn.execute(
                "INSERT INTO file_reputation (file_hash, status, updated_at) VALUES ($1,$2,NOW()) ON CONFLICT (file_hash) DO UPDATE SET status=$2, updated_at=NOW()",
                file_hash, status
            )
    finally:
        await conn.close()

async def add_complaint(file_hash, user_id, reason):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute("INSERT INTO file_complaints (file_hash, user_id, reason) VALUES ($1,$2,$3)", file_hash, user_id, reason)
        await conn.execute("UPDATE file_reputation SET complaints = complaints + 1, updated_at=NOW() WHERE file_hash=$1", file_hash)
        row = await conn.fetchrow("SELECT complaints FROM file_reputation WHERE file_hash=$1", file_hash)
        if row and row["complaints"] >= REPUTATION_THRESHOLD:
            await conn.execute("UPDATE file_reputation SET status='dangerous' WHERE file_hash=$1", file_hash)
            await conn.execute("INSERT INTO security_events (event_type, file_hash, message) VALUES ('auto_blocked', $1, $2)", file_hash, f"Файл заблокирован автоматически после {REPUTATION_THRESHOLD} жалоб")
    finally:
        await conn.close()

async def add_security_event(event_type, file_hash, file_name, message):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute(
            "INSERT INTO security_events (event_type, file_hash, file_name, message) VALUES ($1,$2,$3,$4)",
            event_type, file_hash, file_name, message
        )
    finally:
        await conn.close()

async def clean_old_files():
    while True:
        await asyncio.sleep(86400)
        try:
            conn = await asyncpg.connect(DATABASE_URL)
            rows = await conn.fetch("SELECT id, file_url FROM messages WHERE file_url IS NOT NULL AND created_at < NOW() - INTERVAL '30 days'")
            for row in rows:
                file_path = row["file_url"].lstrip("/")
                if os.path.exists(file_path):
                    os.remove(file_path)
                await conn.execute("UPDATE messages SET file_url=NULL, file_name=NULL, file_size=NULL, file_type=NULL WHERE id=$1", row["id"])
            await conn.close()
        except Exception as e:
            print(f"Ошибка очистки файлов: {e}")

# ---------- HTTP handlers ----------
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

async def messages_handler(request):
    token = request.query.get("token")
    if token != "test":
        return web.json_response({"error": "Unauth"}, status=401)
    user_id = request.query.get("user_id")
    recipient_id = request.query.get("recipient_id")
    group_id = request.query.get("group_id")
    if not user_id:
        return web.json_response({"error": "No user_id"}, status=400)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        if recipient_id:
            rows = await conn.fetch("""
                SELECT m.id, m.text, m.created_at,
                       u.id as uid, u.username, u.full_name, u.name_color, u.badge_url,
                       m.file_url, m.file_name, m.file_size, m.file_type, m.file_hash
                FROM messages m
                JOIN users u ON m.sender_id = u.id
                WHERE ((m.sender_id = $1 AND m.recipient_id = $2) OR (m.sender_id = $2 AND m.recipient_id = $1))
                  AND m.is_deleted = FALSE
                ORDER BY m.created_at DESC LIMIT 50
            """, int(user_id), int(recipient_id))
        elif group_id:
            rows = await conn.fetch("""
                SELECT m.id, m.text, m.created_at,
                       u.id as uid, u.username, u.full_name, u.name_color, u.badge_url,
                       m.file_url, m.file_name, m.file_size, m.file_type, m.file_hash
                FROM messages m
                JOIN users u ON m.sender_id = u.id
                WHERE m.group_id = $1 AND m.is_deleted = FALSE
                ORDER BY m.created_at DESC LIMIT 50
            """, int(group_id))
        else:
            rows = await conn.fetch("""
                SELECT m.id, m.text, m.created_at,
                       u.id as uid, u.username, u.full_name, u.name_color, u.badge_url,
                       m.file_url, m.file_name, m.file_size, m.file_type, m.file_hash
                FROM messages m
                JOIN users u ON m.sender_id = u.id
                WHERE m.recipient_id IS NULL AND m.group_id IS NULL AND m.is_deleted = FALSE
                ORDER BY m.created_at DESC LIMIT 50
            """)
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
            if r["file_url"]:
                msg["file_info"] = {
                    "file_url": r["file_url"],
                    "file_name": r["file_name"],
                    "file_size": r["file_size"],
                    "file_type": r["file_type"],
                    "file_hash": r["file_hash"]
                }
            messages.append(msg)
    finally:
        await conn.close()
    return web.json_response(messages)

async def profile_handler(request):
    user_id = request.query.get("user_id")
    if not user_id:
        return web.json_response({"error": "Missing user_id"}, status=400)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        user = await conn.fetchrow(
            "SELECT id, username, full_name, phone, name_color, badge_url, is_admin, bio, hide_phone, created_at FROM users WHERE id=$1",
            int(user_id)
        )
        if not user:
            return web.json_response({"error": "User not found"}, status=404)
        user_dict = dict(user)
        if user_dict.get("hide_phone"):
            user_dict["phone"] = None
        user_dict = json_serializable(user_dict)
    finally:
        await conn.close()
    return web.json_response(user_dict)

async def update_profile_handler(request):
    data = await request.json()
    user_id = data.get("user_id")
    new_username = data.get("username")
    new_full_name = data.get("full_name")
    new_bio = data.get("bio")
    hide_phone = data.get("hide_phone")
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
        if new_bio is not None:
            await conn.execute("UPDATE users SET bio=$1 WHERE id=$2", new_bio, user_id)
        if hide_phone is not None:
            await conn.execute("UPDATE users SET hide_phone=$1 WHERE id=$2", hide_phone, user_id)
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

# ---------- Файлы ----------
async def upload_handler(request):
    data = await request.post()
    user_id = int(data.get("user_id", 0))
    token = data.get("token")
    if token != "test":
        return web.json_response({"error": "Unauthorized"}, status=401)
    conn = await asyncpg.connect(DATABASE_URL)
    user = await conn.fetchrow("SELECT id FROM users WHERE id=$1", user_id)
    await conn.close()
    if not user:
        return web.json_response({"error": "Invalid user"}, status=403)

    if "file" not in data:
        return web.json_response({"error": "No file"}, status=400)
    file = data["file"]
    file_data = file.file
    original_filename = file.filename
    file_size = len(file_data.read())
    file_data.seek(0)
    if file_size > MAX_FILE_SIZE:
        return web.json_response({"error": "File too large"}, status=400)

    file_hash = hashlib.sha256(file_data.read()).hexdigest()
    file_data.seek(0)

    rep = await get_file_reputation(file_hash)
    if rep and rep["status"] == "dangerous":
        return web.json_response({"error": "This file is blocked due to security reasons"}, status=403)

    ext = os.path.splitext(original_filename)[1]
    unique_name = f"{uuid.uuid4().hex}{ext}"
    save_path = os.path.join(UPLOAD_FOLDER, unique_name)
    async with aiofiles.open(save_path, "wb") as f:
        await f.write(file_data.read())
    print(f"[Upload] Файл сохранён: {save_path}, размер {file_size}")

    mime_type, _ = mimetypes.guess_type(original_filename)
    if not mime_type:
        mime_type = "application/octet-stream"

    file_url = f"/uploads/{unique_name}"
    return web.json_response({
        "file_url": file_url,
        "file_name": original_filename,
        "file_size": file_size,
        "file_type": mime_type,
        "file_hash": file_hash
    })

async def download_handler(request):
    filename = request.match_info['filename']
    token = request.query.get("token")
    if token != "test":
        return web.json_response({"error": "Unauthorized"}, status=401)
    user_id = request.query.get("user_id")
    if not user_id:
        return web.json_response({"error": "Missing user_id"}, status=400)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        file_url = f"/uploads/{filename}"
        msg = await conn.fetchrow("SELECT sender_id, recipient_id, group_id FROM messages WHERE file_url=$1", file_url)
        if not msg:
            return web.json_response({"error": "File not found"}, status=404)
        if msg["group_id"] is not None:
            member = await conn.fetchval("SELECT 1 FROM group_members WHERE group_id=$1 AND user_id=$2", msg["group_id"], int(user_id))
            if not member:
                return web.json_response({"error": "Access denied"}, status=403)
        else:
            if msg["recipient_id"] is not None:
                if int(user_id) not in (msg["sender_id"], msg["recipient_id"]):
                    return web.json_response({"error": "Access denied"}, status=403)
    finally:
        await conn.close()

    file_path = os.path.join(UPLOAD_FOLDER, filename)
    if not os.path.exists(file_path):
        return web.json_response({"error": "File not found on disk"}, status=404)
    return web.FileResponse(file_path)

async def check_file_reputation_handler(request):
    data = await request.json()
    file_hash = data.get("file_hash")
    if not file_hash:
        return web.json_response({"error": "No file_hash"}, status=400)
    rep = await get_file_reputation(file_hash)
    if rep:
        return web.json_response(rep)
    else:
        return web.json_response({"status": "unknown", "complaints": 0})

async def complain_file_handler(request):
    data = await request.json()
    user_id = data.get("user_id")
    file_hash = data.get("file_hash")
    reason = data.get("reason", "")
    token = data.get("token")
    if token != "test":
        return web.json_response({"error": "Unauthorized"}, status=401)
    if not user_id or not file_hash:
        return web.json_response({"error": "Missing parameters"}, status=400)
    await add_complaint(file_hash, user_id, reason)
    return web.json_response({"status": "ok"})

async def security_events_handler(request):
    token = request.query.get("token")
    if token != "test":
        return web.json_response({"error": "Unauthorized"}, status=401)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await conn.fetch("SELECT event_type, file_hash, file_name, message, created_at FROM security_events ORDER BY created_at DESC LIMIT 50")
        events = [dict(r) for r in rows]
    finally:
        await conn.close()
    return web.json_response({"events": events})

# ---------- Админка ----------
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
        if field == "is_admin":
            value = 1 if value in (1, "1", True, "true") else 0
        await conn.execute(f"UPDATE users SET {field}=$1 WHERE id=$2", value, user_id)
        print(f"[Admin] Обновлено поле {field} для user {user_id} на {value}")
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
        sub = await conn.fetchrow("SELECT id, end_date FROM subscriptions WHERE user_id=$1 AND status='active' AND end_date > NOW() ORDER BY end_date DESC LIMIT 1", target_user_id)
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

async def admin_complaints_handler(request):
    admin_id = request.query.get("admin_id")
    if not await is_admin(int(admin_id)):
        return web.json_response({"error": "Forbidden"}, status=403)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await conn.fetch("""
            SELECT fr.file_hash, fr.status, fr.complaints, fc.user_id, fc.reason, fc.created_at
            FROM file_reputation fr
            JOIN file_complaints fc ON fr.file_hash = fc.file_hash
            ORDER BY fc.created_at DESC
        """)
        complaints = [dict(r) for r in rows]
    finally:
        await conn.close()
    return web.json_response({"complaints": complaints})

async def admin_unblock_file_handler(request):
    data = await request.json()
    admin_id = data.get("admin_id")
    if not await is_admin(int(admin_id)):
        return web.json_response({"error": "Forbidden"}, status=403)
    file_hash = data.get("file_hash")
    if not file_hash:
        return web.json_response({"error": "Missing file_hash"}, status=400)
    await update_file_reputation(file_hash, "safe", 0)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute("DELETE FROM file_complaints WHERE file_hash=$1", file_hash)
        await conn.execute("INSERT INTO security_events (event_type, file_hash, message) VALUES ('admin_unblocked', $1, $2)", file_hash, "Файл разблокирован администратором")
    finally:
        await conn.close()
    return web.json_response({"status": "ok"})

async def admin_confirm_dangerous_handler(request):
    data = await request.json()
    admin_id = data.get("admin_id")
    if not await is_admin(int(admin_id)):
        return web.json_response({"error": "Forbidden"}, status=403)
    file_hash = data.get("file_hash")
    if not file_hash:
        return web.json_response({"error": "Missing file_hash"}, status=400)
    await update_file_reputation(file_hash, "dangerous")
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute("INSERT INTO security_events (event_type, file_hash, message) VALUES ('admin_confirmed', $1, $2)", file_hash, "Файл подтверждён как опасный администратором")
    finally:
        await conn.close()
    return web.json_response({"status": "ok"})

async def get_version_handler(request):
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
    if not stable or not beta:
        return web.json_response({"error": "Missing version"}, status=400)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute("UPDATE app_version SET stable_version=$1, beta_version=$2, updated_at=NOW()", stable, beta)
    finally:
        await conn.close()
    return web.json_response({"status": "ok"})

async def admin_broadcast_handler(request):
    data = await request.json()
    admin_id = data.get("admin_id")
    if not await is_admin(int(admin_id)):
        return web.json_response({"error": "Forbidden"}, status=403)
    message = data.get("message")
    if not message:
        return web.json_response({"error": "No message"}, status=400)
    for client in connected_clients.values():
        try:
            await client.send_json({"type": "broadcast", "message": message})
        except:
            pass
    return web.json_response({"status": "ok"})

# ---------- WebSocket ----------
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
                group_id = data.get("group_id")
                file_info = data.get("file_info")
                if not text and not file_info:
                    continue
                conn = await asyncpg.connect(DATABASE_URL)
                try:
                    msg_id = await conn.fetchval(
                        "INSERT INTO messages (sender_id, recipient_id, group_id, text, file_url, file_name, file_size, file_type, file_hash) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id",
                        uid, recipient_id, group_id, text,
                        file_info.get("file_url") if file_info else None,
                        file_info.get("file_name") if file_info else None,
                        file_info.get("file_size") if file_info else None,
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
                    "sender": sender_dict
                }
                if file_info:
                    message_obj["file_info"] = file_info
                if group_id:
                    targets = connected_clients.keys()
                elif recipient_id:
                    targets = [uid, recipient_id]
                else:
                    targets = connected_clients.keys()
                for client_id in targets:
                    client = connected_clients.get(client_id)
                    if client:
                        try:
                            await client.send_json({
                                "type": "new_message",
                                "message": message_obj
                            })
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

# ---------- Запуск ----------
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
    app.router.add_post("/upload", upload_handler)
    app.router.add_get("/uploads/{filename}", download_handler)
    app.router.add_post("/check-file-reputation", check_file_reputation_handler)
    app.router.add_post("/complain-file", complain_file_handler)
    app.router.add_get("/security-events", security_events_handler)
    app.router.add_get("/admin/users", admin_users_handler)
    app.router.add_post("/admin/update-user", admin_update_user_handler)
    app.router.add_post("/admin/set-subscription-days", admin_set_subscription_handler)
    app.router.add_get("/admin/complaints", admin_complaints_handler)
    app.router.add_post("/admin/unblock-file", admin_unblock_file_handler)
    app.router.add_post("/admin/confirm-dangerous", admin_confirm_dangerous_handler)
    app.router.add_get("/app-version", get_version_handler)
    app.router.add_post("/admin/set-version", admin_set_version_handler)
    app.router.add_post("/admin/broadcast", admin_broadcast_handler)
    app.router.add_get("/ws", ws_handler)
    app.router.add_static('/uploads', UPLOAD_FOLDER, name='uploads')
    return app

app = init_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    web.run_app(app, port=port)
