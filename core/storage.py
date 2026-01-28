"""
Storage abstraction supporting file and PostgreSQL backends.

If DATABASE_URL is set, PostgreSQL is used.
"""

import asyncio
import json
import logging
import os
import threading
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

_db_pool = None
_db_pool_lock = None
_db_loop = None
_db_thread = None
_db_loop_lock = threading.Lock()


def _get_database_url() -> str:
    return os.environ.get("DATABASE_URL", "").strip()


def is_database_enabled() -> bool:
    """Return True when DATABASE_URL is configured."""
    return bool(_get_database_url())


def _ensure_db_loop() -> asyncio.AbstractEventLoop:
    global _db_loop, _db_thread
    if _db_loop and _db_thread and _db_thread.is_alive():
        return _db_loop
    with _db_loop_lock:
        if _db_loop and _db_thread and _db_thread.is_alive():
            return _db_loop
        loop = asyncio.new_event_loop()

        def _runner() -> None:
            asyncio.set_event_loop(loop)
            loop.run_forever()

        thread = threading.Thread(target=_runner, name="storage-db-loop", daemon=True)
        thread.start()
        _db_loop = loop
        _db_thread = thread
        return _db_loop


def _run_in_db_loop(coro):
    loop = _ensure_db_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result()


async def _get_pool():
    """Get (or create) the asyncpg connection pool."""
    global _db_pool, _db_pool_lock
    if _db_pool is not None:
        return _db_pool
    if _db_pool_lock is None:
        _db_pool_lock = asyncio.Lock()
    async with _db_pool_lock:
        if _db_pool is not None:
            return _db_pool
        db_url = _get_database_url()
        if not db_url:
            raise ValueError("DATABASE_URL is not set")
        try:
            import asyncpg
            _db_pool = await asyncpg.create_pool(
                db_url,
                min_size=1,
                max_size=10,
                command_timeout=30,
            )
            await _init_tables(_db_pool)
            logger.info("[STORAGE] PostgreSQL pool initialized")
        except ImportError:
            logger.error("[STORAGE] asyncpg is required for database storage")
            raise
        except Exception as e:
            logger.error(f"[STORAGE] Database connection failed: {e}")
            raise
    return _db_pool


async def _init_tables(pool) -> None:
    """Initialize database tables."""
    async with pool.acquire() as conn:
        # 创建 kv_store 表（用于设置和统计）
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value JSONB NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        # 创建独立的 accounts 表
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                id TEXT PRIMARY KEY,
                secure_c_ses TEXT NOT NULL,
                host_c_oses TEXT,
                csesidx TEXT NOT NULL,
                config_id TEXT NOT NULL,
                expires_at TEXT,
                disabled BOOLEAN DEFAULT FALSE,
                mail_provider TEXT,
                mail_address TEXT,
                mail_password TEXT,
                mail_client_id TEXT,
                mail_refresh_token TEXT,
                mail_tenant TEXT,
                mail_base_url TEXT,
                mail_jwt_token TEXT,
                mail_verify_ssl BOOLEAN,
                mail_domain TEXT,
                mail_api_key TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        # 创建索引
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_accounts_disabled ON accounts(disabled)
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_accounts_expires_at ON accounts(expires_at)
            """
        )
        logger.info("[STORAGE] Database tables initialized")


async def db_get(key: str) -> Optional[dict]:
    """Fetch a value from the database."""
    pool = await _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT value FROM kv_store WHERE key = $1", key
        )
        if not row:
            return None
        value = row["value"]
        if isinstance(value, str):
            return json.loads(value)
        return value


async def db_set(key: str, value: dict) -> None:
    """Persist a value to the database."""
    pool = await _get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO kv_store (key, value, updated_at)
            VALUES ($1, $2, CURRENT_TIMESTAMP)
            ON CONFLICT (key) DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = CURRENT_TIMESTAMP
            """,
            key,
            json.dumps(value, ensure_ascii=False),
        )


# ==================== Accounts storage (独立表) ====================

# 账户表字段列表
ACCOUNT_FIELDS = [
    "id", "secure_c_ses", "host_c_oses", "csesidx", "config_id",
    "expires_at", "disabled", "mail_provider", "mail_address", "mail_password",
    "mail_client_id", "mail_refresh_token", "mail_tenant", "mail_base_url",
    "mail_jwt_token", "mail_verify_ssl", "mail_domain", "mail_api_key"
]


def _account_row_to_dict(row) -> dict:
    """将数据库行转换为账户字典"""
    return {field: row[field] for field in ACCOUNT_FIELDS if field in row.keys()}


def _account_dict_to_values(account: dict) -> tuple:
    """将账户字典转换为数据库值元组"""
    return tuple(account.get(field) for field in ACCOUNT_FIELDS)


async def load_accounts() -> Optional[list]:
    """
    Load account configuration from database when enabled.
    Return None to indicate file-based fallback.
    """
    if not is_database_enabled():
        return None
    try:
        pool = await _get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT {', '.join(ACCOUNT_FIELDS)} FROM accounts ORDER BY created_at"
            )
            accounts = [_account_row_to_dict(row) for row in rows]
            logger.info(f"[STORAGE] Loaded {len(accounts)} accounts from database")
            return accounts
    except Exception as e:
        logger.error(f"[STORAGE] Database read failed: {e}")
    return None


async def get_accounts_updated_at() -> Optional[float]:
    """
    Get the latest accounts updated_at timestamp (epoch seconds).
    Return None if database is not enabled or failed.
    """
    if not is_database_enabled():
        return None
    try:
        pool = await _get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT EXTRACT(EPOCH FROM MAX(updated_at)) AS ts FROM accounts"
            )
            if not row or row["ts"] is None:
                return None
            return float(row["ts"])
    except Exception as e:
        logger.error(f"[STORAGE] Database accounts updated_at failed: {e}")
    return None


def get_accounts_updated_at_sync() -> Optional[float]:
    """Sync wrapper for get_accounts_updated_at."""
    return _run_in_db_loop(get_accounts_updated_at())


async def save_accounts(accounts: list) -> bool:
    """Save account configuration to database when enabled (全量更新)."""
    if not is_database_enabled():
        return False
    try:
        pool = await _get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                # 获取现有账户 ID
                existing_rows = await conn.fetch("SELECT id FROM accounts")
                existing_ids = {row["id"] for row in existing_rows}

                # 计算需要删除的账户
                new_ids = {acc.get("id") for acc in accounts if acc.get("id")}
                ids_to_delete = existing_ids - new_ids

                # 删除不再存在的账户
                if ids_to_delete:
                    await conn.execute(
                        "DELETE FROM accounts WHERE id = ANY($1)",
                        list(ids_to_delete)
                    )

                # 插入或更新账户
                for account in accounts:
                    if not account.get("id"):
                        continue

                    # 构建 UPSERT 语句
                    fields_str = ", ".join(ACCOUNT_FIELDS)
                    placeholders = ", ".join(f"${i+1}" for i in range(len(ACCOUNT_FIELDS)))
                    update_str = ", ".join(
                        f"{field} = EXCLUDED.{field}"
                        for field in ACCOUNT_FIELDS if field != "id"
                    )

                    await conn.execute(
                        f"""
                        INSERT INTO accounts ({fields_str})
                        VALUES ({placeholders})
                        ON CONFLICT (id) DO UPDATE SET
                            {update_str},
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        *_account_dict_to_values(account)
                    )

            logger.info(f"[STORAGE] Saved {len(accounts)} accounts to database")
            return True
    except Exception as e:
        logger.error(f"[STORAGE] Database write failed: {e}")
    return False


async def save_account(account: dict) -> bool:
    """Save a single account to database (单个账户更新)."""
    if not is_database_enabled():
        return False
    if not account.get("id"):
        return False
    try:
        pool = await _get_pool()
        async with pool.acquire() as conn:
            fields_str = ", ".join(ACCOUNT_FIELDS)
            placeholders = ", ".join(f"${i+1}" for i in range(len(ACCOUNT_FIELDS)))
            update_str = ", ".join(
                f"{field} = EXCLUDED.{field}"
                for field in ACCOUNT_FIELDS if field != "id"
            )

            await conn.execute(
                f"""
                INSERT INTO accounts ({fields_str})
                VALUES ({placeholders})
                ON CONFLICT (id) DO UPDATE SET
                    {update_str},
                    updated_at = CURRENT_TIMESTAMP
                """,
                *_account_dict_to_values(account)
            )
            logger.info(f"[STORAGE] Saved account {account.get('id')} to database")
            return True
    except Exception as e:
        logger.error(f"[STORAGE] Database write failed: {e}")
    return False


async def delete_account(account_id: str) -> bool:
    """Delete a single account from database."""
    if not is_database_enabled():
        return False
    try:
        pool = await _get_pool()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM accounts WHERE id = $1", account_id)
            logger.info(f"[STORAGE] Deleted account {account_id} from database")
            return True
    except Exception as e:
        logger.error(f"[STORAGE] Database delete failed: {e}")
    return False


async def delete_accounts(account_ids: list) -> bool:
    """Delete multiple accounts from database."""
    if not is_database_enabled():
        return False
    if not account_ids:
        return True
    try:
        pool = await _get_pool()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM accounts WHERE id = ANY($1)", account_ids)
            logger.info(f"[STORAGE] Deleted {len(account_ids)} accounts from database")
            return True
    except Exception as e:
        logger.error(f"[STORAGE] Database delete failed: {e}")
    return False


def load_accounts_sync() -> Optional[list]:
    """Sync wrapper for load_accounts (safe in sync/async call sites)."""
    return _run_in_db_loop(load_accounts())


def save_accounts_sync(accounts: list) -> bool:
    """Sync wrapper for save_accounts (safe in sync/async call sites)."""
    return _run_in_db_loop(save_accounts(accounts))


def save_account_sync(account: dict) -> bool:
    """Sync wrapper for save_account (safe in sync/async call sites)."""
    return _run_in_db_loop(save_account(account))


def delete_account_sync(account_id: str) -> bool:
    """Sync wrapper for delete_account (safe in sync/async call sites)."""
    return _run_in_db_loop(delete_account(account_id))


def delete_accounts_sync(account_ids: list) -> bool:
    """Sync wrapper for delete_accounts (safe in sync/async call sites)."""
    return _run_in_db_loop(delete_accounts(account_ids))


# ==================== Settings storage ====================

async def load_settings() -> Optional[dict]:
    if not is_database_enabled():
        return None
    try:
        return await db_get("settings")
    except Exception as e:
        logger.error(f"[STORAGE] Settings read failed: {e}")
    return None


async def save_settings(settings: dict) -> bool:
    if not is_database_enabled():
        return False
    try:
        await db_set("settings", settings)
        logger.info("[STORAGE] Settings saved to database")
        return True
    except Exception as e:
        logger.error(f"[STORAGE] Settings write failed: {e}")
    return False


# ==================== Stats storage ====================

async def load_stats() -> Optional[dict]:
    if not is_database_enabled():
        return None
    try:
        return await db_get("stats")
    except Exception as e:
        logger.error(f"[STORAGE] Stats read failed: {e}")
    return None


async def save_stats(stats: dict) -> bool:
    if not is_database_enabled():
        return False
    try:
        await db_set("stats", stats)
        return True
    except Exception as e:
        logger.error(f"[STORAGE] Stats write failed: {e}")
    return False


def load_settings_sync() -> Optional[dict]:
    return _run_in_db_loop(load_settings())


def save_settings_sync(settings: dict) -> bool:
    return _run_in_db_loop(save_settings(settings))


def load_stats_sync() -> Optional[dict]:
    return _run_in_db_loop(load_stats())


def save_stats_sync(stats: dict) -> bool:
    return _run_in_db_loop(save_stats(stats))
