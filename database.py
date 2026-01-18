import aiosqlite
import time
import re
from datetime import datetime, timedelta

DB_NAME = "nullprotocol.db"

# Helper function to parse time
def parse_time_string(time_str):
    """
    Parse time string like: 
    "30m" = 30 minutes
    "2h" = 2 hours (120 minutes)
    "1h30m" = 90 minutes
    "24h" = 1440 minutes
    """
    if not time_str or str(time_str).lower() == 'none':
        return None
    
    time_str = str(time_str).lower()
    total_minutes = 0
    
    # Extract hours
    hour_match = re.search(r'(\d+)h', time_str)
    if hour_match:
        total_minutes += int(hour_match.group(1)) * 60
    
    # Extract minutes
    minute_match = re.search(r'(\d+)m', time_str)
    if minute_match:
        total_minutes += int(minute_match.group(1))
    
    # If no h/m specified, assume minutes if it's a number
    if not hour_match and not minute_match and time_str.isdigit():
        total_minutes = int(time_str)
    
    return total_minutes if total_minutes > 0 else None

async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        # Users Table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                credits INTEGER DEFAULT 5,
                joined_date TEXT,
                referrer_id INTEGER,
                is_banned INTEGER DEFAULT 0,
                total_earned INTEGER DEFAULT 0,
                last_active TEXT
            )
        """)
        
        # Admins Table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY,
                level TEXT DEFAULT 'admin',
                added_by INTEGER,
                added_date TEXT
            )
        """)
        
        # Redeem Codes Table with expiry in MINUTES
        await db.execute("""
            CREATE TABLE IF NOT EXISTS redeem_codes (
                code TEXT PRIMARY KEY,
                amount INTEGER,
                max_uses INTEGER,
                current_uses INTEGER DEFAULT 0,
                expiry_minutes INTEGER,
                created_date TEXT,
                is_active INTEGER DEFAULT 1
            )
        """)
        
        # Redeem logs to track who used which code
        await db.execute("""
            CREATE TABLE IF NOT EXISTS redeem_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                code TEXT,
                claimed_date TEXT,
                UNIQUE(user_id, code)
            )
        """)
        
        # Statistics Table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                date TEXT PRIMARY KEY,
                total_users INTEGER DEFAULT 0,
                active_users INTEGER DEFAULT 0,
                total_lookups INTEGER DEFAULT 0,
                credits_used INTEGER DEFAULT 0
            )
        """)
        
        # Lookup Logs Table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS lookup_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                api_type TEXT,
                input_data TEXT,
                result TEXT,
                lookup_date TEXT
            )
        """)
        
        await db.commit()

async def get_user(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def add_user(user_id, username, referrer_id=None):
    async with aiosqlite.connect(DB_NAME) as db:
        # Check if user exists
        async with db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,)) as cursor:
            if await cursor.fetchone():
                return
        
        credits = 5
        current_time = str(time.time())
        await db.execute("""
            INSERT INTO users (user_id, username, credits, joined_date, referrer_id, is_banned, total_earned, last_active) 
            VALUES (?, ?, ?, ?, ?, 0, 0, ?)
        """, (user_id, username, credits, current_time, referrer_id, current_time))
        await db.commit()

async def update_credits(user_id, amount):
    async with aiosqlite.connect(DB_NAME) as db:
        if amount > 0:
            await db.execute("UPDATE users SET credits = credits + ?, total_earned = total_earned + ? WHERE user_id = ?", 
                           (amount, amount, user_id))
        else:
            await db.execute("UPDATE users SET credits = credits + ? WHERE user_id = ?", (amount, user_id))
        await db.commit()

async def set_ban_status(user_id, status):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET is_banned = ? WHERE user_id = ?", (status, user_id))
        await db.commit()

async def create_redeem_code(code, amount, max_uses, expiry_minutes=None):
    """
    Create redeem code with expiry in minutes
    """
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            INSERT OR REPLACE INTO redeem_codes 
            (code, amount, max_uses, expiry_minutes, created_date, is_active)
            VALUES (?, ?, ?, ?, ?, 1)
        """, (code, amount, max_uses, expiry_minutes, datetime.now().isoformat()))
        await db.commit()

async def redeem_code_db(user_id, code):
    """
    Redeem code with multiple checks - FIXED: One user can use code only once
    """
    async with aiosqlite.connect(DB_NAME) as db:
        # Check if user already claimed this code
        async with db.execute("""
            SELECT 1 FROM redeem_logs WHERE user_id = ? AND code = ?
        """, (user_id, code)) as cursor:
            if await cursor.fetchone():
                return "already_claimed"
        
        # Get code details
        async with db.execute("""
            SELECT amount, max_uses, current_uses, expiry_minutes, created_date, is_active
            FROM redeem_codes WHERE code = ?
        """, (code,)) as cursor:
            data = await cursor.fetchone()
            
        if not data:
            return "invalid"
        
        amount, max_uses, current_uses, expiry_minutes, created_date, is_active = data
        
        # Check if code is active
        if not is_active:
            return "inactive"
        
        # Check max uses
        if current_uses >= max_uses:
            return "limit_reached"
        
        # Check expiry
        if expiry_minutes is not None and expiry_minutes > 0:
            created_dt = datetime.fromisoformat(created_date)
            expiry_dt = created_dt + timedelta(minutes=expiry_minutes)
            if datetime.now() > expiry_dt:
                return "expired"
        
        # All checks passed, process redeem
        try:
            await db.execute("BEGIN TRANSACTION")
            
            # Update current uses
            await db.execute("""
                UPDATE redeem_codes SET current_uses = current_uses + 1 WHERE code = ?
            """, (code,))
            
            # Add credits to user
            await db.execute("""
                UPDATE users SET credits = credits + ? WHERE user_id = ?
            """, (amount, user_id))
            
            # Update total earned
            await db.execute("""
                UPDATE users SET total_earned = total_earned + ? WHERE user_id = ?
            """, (amount, user_id))
            
            # Log the claim
            await db.execute("""
                INSERT OR IGNORE INTO redeem_logs (user_id, code, claimed_date)
                VALUES (?, ?, ?)
            """, (user_id, code, datetime.now().isoformat()))
            
            await db.execute("COMMIT")
            return amount
            
        except Exception as e:
            await db.execute("ROLLBACK")
            return f"error: {str(e)}"

async def get_all_users():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM users") as cursor:
            rows = await cursor.fetchall()
            return [row[0] for row in rows]

async def get_user_by_username(username):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM users WHERE username = ?", (username,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

async def get_top_referrers(limit=10):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT referrer_id, COUNT(*) as referrals 
            FROM users 
            WHERE referrer_id IS NOT NULL 
            GROUP BY referrer_id 
            ORDER BY referrals DESC 
            LIMIT ?
        """, (limit,)) as cursor:
            return await cursor.fetchall()

async def get_bot_stats():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as cursor:
            total_users = (await cursor.fetchone())[0]
        
        async with db.execute("SELECT COUNT(*) FROM users WHERE credits > 0") as cursor:
            active_users = (await cursor.fetchone())[0]
        
        async with db.execute("SELECT SUM(credits) FROM users") as cursor:
            total_credits = (await cursor.fetchone())[0] or 0
        
        async with db.execute("SELECT SUM(total_earned) FROM users") as cursor:
            credits_distributed = (await cursor.fetchone())[0] or 0
        
        return {
            'total_users': total_users,
            'active_users': active_users,
            'total_credits': total_credits,
            'credits_distributed': credits_distributed
        }

async def get_users_in_range(start_date, end_date):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT user_id, username, credits, joined_date 
            FROM users 
            WHERE joined_date BETWEEN ? AND ?
        """, (start_date, end_date)) as cursor:
            return await cursor.fetchall()

# Admin management functions
async def add_admin(user_id, level='admin'):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR REPLACE INTO admins (user_id, level) VALUES (?, ?)", (user_id, level))
        await db.commit()

async def remove_admin(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
        await db.commit()

async def get_all_admins():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id, level FROM admins") as cursor:
            return await cursor.fetchall()

async def is_admin(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT level FROM admins WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

async def get_expired_codes():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT code, amount, current_uses, max_uses, expiry_minutes, created_date 
            FROM redeem_codes 
            WHERE is_active = 1 
            AND expiry_minutes IS NOT NULL 
            AND expiry_minutes > 0
            AND datetime(created_date, '+' || expiry_minutes || ' minutes') < datetime('now')
        """) as cursor:
            return await cursor.fetchall()

async def delete_redeem_code(code):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM redeem_codes WHERE code = ?", (code,))
        await db.commit()

async def deactivate_code(code):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE redeem_codes SET is_active = 0 WHERE code = ?", (code,))
        await db.commit()

async def get_all_codes():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT code, amount, max_uses, current_uses, 
                   expiry_minutes, created_date, is_active
            FROM redeem_codes
            ORDER BY created_date DESC
        """) as cursor:
            return await cursor.fetchall()

async def get_user_redeem_history(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT code, claimed_date FROM redeem_logs WHERE user_id = ?
        """, (user_id,)) as cursor:
            return await cursor.fetchall()

async def update_username(user_id, username):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET username = ? WHERE user_id = ?", (username, user_id))
        await db.commit()

async def get_user_stats(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT 
                (SELECT COUNT(*) FROM users WHERE referrer_id = ?) as referrals,
                (SELECT COUNT(*) FROM redeem_logs WHERE user_id = ?) as codes_claimed,
                (SELECT SUM(amount) FROM redeem_logs rl 
                 JOIN redeem_codes rc ON rl.code = rc.code 
                 WHERE rl.user_id = ?) as total_from_codes
            FROM users WHERE user_id = ?
        """, (user_id, user_id, user_id, user_id)) as cursor:
            return await cursor.fetchone()

async def get_recent_users(limit=20):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT user_id, username, joined_date 
            FROM users 
            ORDER BY joined_date DESC 
            LIMIT ?
        """, (limit,)) as cursor:
            return await cursor.fetchall()

async def get_active_codes():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT code, amount, max_uses, current_uses
            FROM redeem_codes
            WHERE is_active = 1
            ORDER BY created_date DESC
        """) as cursor:
            return await cursor.fetchall()

async def get_inactive_codes():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT code, amount, max_uses, current_uses
            FROM redeem_codes
            WHERE is_active = 0
            ORDER BY created_date DESC
        """) as cursor:
            return await cursor.fetchall()

async def delete_user(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        # Delete user from users table
        await db.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
        # Delete user's redeem logs
        await db.execute("DELETE FROM redeem_logs WHERE user_id = ?", (user_id,))
        # Update referrer_id for users referred by this user
        await db.execute("UPDATE users SET referrer_id = NULL WHERE referrer_id = ?", (user_id,))
        await db.commit()

async def reset_user_credits(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET credits = 0 WHERE user_id = ?", (user_id,))
        await db.commit()

async def get_user_by_id(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def search_users(query):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT user_id, username, credits 
            FROM users 
            WHERE username LIKE ? OR user_id = ?
            LIMIT 20
        """, (f"%{query}%", query if query.isdigit() else 0)) as cursor:
            return await cursor.fetchall()

async def get_daily_stats(days=7):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT 
                date(joined_date, 'unixepoch') as join_date,
                COUNT(*) as new_users,
                (SELECT COUNT(*) FROM redeem_logs 
                 WHERE date(claimed_date) = date(joined_date, 'unixepoch')) as claims
            FROM users 
            WHERE date(joined_date, 'unixepoch') >= date('now', ? || ' days')
            GROUP BY join_date
            ORDER BY join_date DESC
        """, (f"-{days}",)) as cursor:
            return await cursor.fetchall()

async def log_lookup(user_id, api_type, input_data, result):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            INSERT INTO lookup_logs (user_id, api_type, input_data, result, lookup_date)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, api_type, input_data[:500], str(result)[:1000], datetime.now().isoformat()))
        await db.commit()

async def get_lookup_stats(user_id=None):
    async with aiosqlite.connect(DB_NAME) as db:
        if user_id:
            async with db.execute("""
                SELECT api_type, COUNT(*) as count 
                FROM lookup_logs 
                WHERE user_id = ?
                GROUP BY api_type
            """, (user_id,)) as cursor:
                return await cursor.fetchall()
        else:
            async with db.execute("""
                SELECT api_type, COUNT(*) as count 
                FROM lookup_logs 
                GROUP BY api_type
            """) as cursor:
                return await cursor.fetchall()

async def get_total_lookups():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM lookup_logs") as cursor:
            return (await cursor.fetchone())[0]

async def get_user_lookups(user_id, limit=50):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT api_type, input_data, lookup_date 
            FROM lookup_logs 
            WHERE user_id = ?
            ORDER BY lookup_date DESC
            LIMIT ?
        """, (user_id, limit)) as cursor:
            return await cursor.fetchall()

async def get_premium_users():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT user_id, username, credits 
            FROM users 
            WHERE credits >= 100
            ORDER BY credits DESC
        """) as cursor:
            return await cursor.fetchall()

async def get_low_credit_users():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT user_id, username, credits 
            FROM users 
            WHERE credits <= 5
            ORDER BY credits ASC
        """) as cursor:
            return await cursor.fetchall()

async def get_inactive_users(days=30):
    async with aiosqlite.connect(DB_NAME) as db:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        async with db.execute("""
            SELECT user_id, username, last_active 
            FROM users 
            WHERE last_active < ? 
            AND is_banned = 0
            ORDER BY last_active ASC
        """, (cutoff,)) as cursor:
            return await cursor.fetchall()

async def update_last_active(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET last_active = ? WHERE user_id = ?", 
                       (datetime.now().isoformat(), user_id))
        await db.commit()

async def get_user_activity(user_id, days=7):
    async with aiosqlite.connect(DB_NAME) as db:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        async with db.execute("""
            SELECT COUNT(*) 
            FROM lookup_logs 
            WHERE user_id = ? AND lookup_date > ?
        """, (user_id, cutoff)) as cursor:
            return (await cursor.fetchone())[0]

async def get_leaderboard(limit=10):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT user_id, username, credits 
            FROM users 
            WHERE is_banned = 0
            ORDER BY credits DESC 
            LIMIT ?
        """, (limit,)) as cursor:
            return await cursor.fetchall()

async def bulk_update_credits(user_ids, amount):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN TRANSACTION")
        for user_id in user_ids:
            if amount > 0:
                await db.execute("UPDATE users SET credits = credits + ?, total_earned = total_earned + ? WHERE user_id = ?", 
                               (amount, amount, user_id))
            else:
                await db.execute("UPDATE users SET credits = credits + ? WHERE user_id = ?", (amount, user_id))
        await db.execute("COMMIT")

async def get_code_usage_stats(code):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT 
                rc.amount, rc.max_uses, rc.current_uses,
                COUNT(DISTINCT rl.user_id) as unique_users,
                GROUP_CONCAT(DISTINCT rl.user_id) as user_ids
            FROM redeem_codes rc
            LEFT JOIN redeem_logs rl ON rc.code = rl.code
            WHERE rc.code = ?
            GROUP BY rc.code
        """, (code,)) as cursor:
            return await cursor.fetchone()