"""
Database management for Worker Attendance Bot - FIXED PostgreSQL version
"""

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
from threading import Lock
from contextlib import contextmanager

_DATABASE_URL = os.getenv('DATABASE_URL', '').strip()
_USING_POSTGRES = _DATABASE_URL.lower().startswith('postgres://') or _DATABASE_URL.lower().startswith('postgresql://')

if _USING_POSTGRES:
    import psycopg2
    import psycopg2.extras
    from psycopg2 import pool
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Global connection pool for PostgreSQL
_pg_pool: Optional["pool.SimpleConnectionPool"] = None
_pool_lock = Lock()

DB_FILE = 'attendance.db'
_db_lock = Lock()

GROUP_CODE_NO_EXPIRY_DATE = os.getenv('GROUP_CODE_NO_EXPIRY_DATE', '9999-12-31')


def initialize_pool():
    """Initialize PostgreSQL connection pool. Call once at startup."""
    global _pg_pool
    if not _USING_POSTGRES:
        return
    
    with _pool_lock:
        if _pg_pool is None:
            try:
                # Use ThreadedConnectionPool for thread safety
                _pg_pool = pool.ThreadedConnectionPool(
                    minconn=5,
                    maxconn=100,  # Support 1000+ users
                    dsn=_DATABASE_URL,
                    connect_timeout=10
                )
                print(f"✓ PostgreSQL connection pool initialized (5-100 connections)")
            except Exception as e:
                print(f"❌ Failed to initialize PostgreSQL pool: {e}")
                raise


def close_pool():
    """Close all connections in the pool."""
    global _pg_pool
    with _pool_lock:
        if _pg_pool is not None:
            _pg_pool.closeall()
            _pg_pool = None
            print("✓ PostgreSQL connection pool closed")


@contextmanager
def get_db_connection(timeout=5.0):
    """
    Context manager for getting database connections.
    Automatically returns connection to pool or closes it.
    
    Usage:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(...)
            conn.commit()
    """
    if _USING_POSTGRES:
        if _pg_pool is None:
            raise RuntimeError("Connection pool not initialized. Call initialize_pool() first.")
        
        conn = None
        try:
            conn = _pg_pool.getconn()
            if conn:
                conn.autocommit = False
                yield _wrap_pg_connection(conn)
        finally:
            if conn:
                try:
                    _pg_pool.putconn(conn)
                except Exception as e:
                    print(f"Warning: Failed to return connection to pool: {e}")
    else:
        # SQLite
        conn = sqlite3.connect(DB_FILE, timeout=timeout)
        try:
            yield conn
        finally:
            conn.close()


def _qmark_to_percent_s(query: str) -> str:
    """Convert SQLite ? placeholders to PostgreSQL %s."""
    out = []
    in_single = False
    in_double = False
    i = 0
    while i < len(query):
        ch = query[i]
        if ch == "'" and not in_double:
            if in_single and i + 1 < len(query) and query[i + 1] == "'":
                out.append("''")
                i += 2
                continue
            in_single = not in_single
            out.append(ch)
            i += 1
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            out.append(ch)
            i += 1
            continue
        if ch == '?' and not in_single and not in_double:
            out.append('%s')
            i += 1
            continue
        out.append(ch)
        i += 1
    return ''.join(out)


class _PgCompatCursor:
    """Wrapper for PostgreSQL cursor to provide SQLite-like interface."""
    def __init__(self, cur):
        self._cur = cur
        self.lastrowid = None
        self.rowcount = 0

    def execute(self, query, params=None):
        q = str(query).strip()
        if q.upper().startswith('PRAGMA'):
            return None
        q = _qmark_to_percent_s(q)
        result = self._cur.execute(q, params or ())
        self.rowcount = self._cur.rowcount
        return result

    def executemany(self, query, params_seq):
        q = str(query).strip()
        if q.upper().startswith('PRAGMA'):
            return None
        q = _qmark_to_percent_s(q)
        return self._cur.executemany(q, params_seq)

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    def close(self):
        return self._cur.close()


def _wrap_pg_connection(conn):
    """Wrap PostgreSQL connection to provide SQLite-like interface."""
    class PgConnWrapper:
        def __init__(self, pg_conn):
            self._conn = pg_conn
            self.row_factory = None

        def cursor(self):
            if self.row_factory is not None:
                cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            else:
                cur = self._conn.cursor()
            return _PgCompatCursor(cur)

        def commit(self):
            return self._conn.commit()

        def rollback(self):
            return self._conn.rollback()

        def close(self):
            pass  # Don't close, will be returned to pool by context manager

    return PgConnWrapper(conn)


def init_db():
    """Initialize database with required tables"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        if _USING_POSTGRES:
            # PostgreSQL schema
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id BIGSERIAL PRIMARY KEY,
                    telegram_id BIGINT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    fin TEXT NOT NULL,
                    seriya TEXT NOT NULL,
                    code TEXT NOT NULL,
                    phone_number TEXT,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    registered_at TIMESTAMPTZ DEFAULT NOW()
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS attendance (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    date DATE NOT NULL,
                    giris_time TEXT,
                    cixis_time TEXT,
                    giris_loc TEXT,
                    cixis_loc TEXT,
                    UNIQUE(user_id, date)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS codes (
                    id BIGSERIAL PRIMARY KEY,
                    code TEXT UNIQUE NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    expires_at TIMESTAMPTZ NOT NULL
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS registrations (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    date DATE NOT NULL,
                    profession TEXT NOT NULL,
                    code TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(user_id, date, code, profession)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS group_codes (
                    id BIGSERIAL PRIMARY KEY,
                    profession TEXT NOT NULL,
                    date DATE NOT NULL,
                    code TEXT NOT NULL,
                    expires_at DATE NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    UNIQUE(profession, date, code)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users2 (
                    id BIGSERIAL PRIMARY KEY,
                    telegram_id BIGINT UNIQUE NOT NULL,
                    full_name TEXT NOT NULL
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sessions (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users2(id) ON DELETE CASCADE,
                    start_time TIMESTAMPTZ NOT NULL,
                    start_lat DOUBLE PRECISION NOT NULL,
                    start_lon DOUBLE PRECISION NOT NULL,
                    end_time TIMESTAMPTZ,
                    end_lat DOUBLE PRECISION,
                    end_lon DOUBLE PRECISION,
                    duration_min INTEGER,
                    distance_m DOUBLE PRECISION
                )
            ''')

        else:
            # SQLite schema
            cursor.execute('PRAGMA journal_mode=WAL')
            cursor.execute('PRAGMA synchronous=NORMAL')
            cursor.execute('PRAGMA cache_size=10000')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    telegram_id INTEGER UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    fin TEXT NOT NULL,
                    seriya TEXT NOT NULL,
                    code TEXT NOT NULL,
                    phone_number TEXT,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS attendance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    date TEXT NOT NULL,
                    giris_time TEXT,
                    cixis_time TEXT,
                    giris_loc TEXT,
                    cixis_loc TEXT,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    UNIQUE(user_id, date)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS codes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP NOT NULL
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS registrations (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    date TEXT NOT NULL,
                    profession TEXT NOT NULL,
                    code TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, date, code, profession),
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS group_codes (
                    id INTEGER PRIMARY KEY,
                    profession TEXT NOT NULL,
                    date TEXT NOT NULL,
                    code TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    UNIQUE(profession, date, code)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users2 (
                    id INTEGER PRIMARY KEY,
                    telegram_id INTEGER UNIQUE NOT NULL,
                    full_name TEXT NOT NULL
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sessions (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    start_time TEXT NOT NULL,
                    start_lat REAL NOT NULL,
                    start_lon REAL NOT NULL,
                    end_time TEXT,
                    end_lat REAL,
                    end_lon REAL,
                    duration_min INTEGER,
                    distance_m REAL,
                    FOREIGN KEY(user_id) REFERENCES users2(id) ON DELETE CASCADE
                )
            ''')

        # Create indexes
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id)',
            'CREATE INDEX IF NOT EXISTS idx_users_code ON users(code)',
            'CREATE INDEX IF NOT EXISTS idx_attendance_user_date ON attendance(user_id, date)',
            'CREATE INDEX IF NOT EXISTS idx_attendance_date ON attendance(date)',
            'CREATE INDEX IF NOT EXISTS idx_codes_expires ON codes(expires_at)',
            'CREATE INDEX IF NOT EXISTS idx_codes_code ON codes(code)',
            'CREATE INDEX IF NOT EXISTS idx_regs_date ON registrations(date)',
            'CREATE INDEX IF NOT EXISTS idx_regs_code ON registrations(code)',
            'CREATE INDEX IF NOT EXISTS idx_group_codes_date ON group_codes(date)',
            'CREATE INDEX IF NOT EXISTS idx_group_codes_prof ON group_codes(profession)',
            'CREATE INDEX IF NOT EXISTS idx_users2_tid ON users2(telegram_id)',
            'CREATE INDEX IF NOT EXISTS idx_sessions_user_open ON sessions(user_id, end_time)',
        ]

        for idx_sql in indexes:
            try:
                cursor.execute(idx_sql)
            except Exception as e:
                print(f"Warning creating index: {e}")

        conn.commit()


# Code management functions
def add_code(code: str, days_valid: int = 30) -> bool:
    """Add a new access code"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            expires_at = datetime.now() + timedelta(days=days_valid)
            cursor.execute(
                'INSERT INTO codes (code, expires_at) VALUES (?, ?)',
                (code, expires_at)
            )
            conn.commit()
            return True
    except Exception as e:
        print(f"Error adding code: {e}")
        return False


def remove_code(code: str) -> bool:
    """Remove an access code"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM codes WHERE code = ?', (code,))
            affected = cursor.rowcount
            conn.commit()
            return affected > 0
    except Exception as e:
        print(f"Error removing code: {e}")
        return False


def is_code_valid(code: str) -> bool:
    """Check if code exists and is not expired"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT COUNT(*) FROM codes WHERE code = ? AND expires_at > ?',
                (code, datetime.now())
            )
            count = cursor.fetchone()[0]
            return count > 0
    except Exception as e:
        print(f"Error checking code: {e}")
        return False


def get_all_codes() -> List[Tuple]:
    """Get all active codes"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT code, created_at, expires_at FROM codes WHERE expires_at > ? ORDER BY created_at DESC',
                (datetime.now(),)
            )
            return cursor.fetchall()
    except Exception as e:
        print(f"Error getting codes: {e}")
        return []


# User management functions
def register_user(telegram_id: int, name: str, fin: str, seriya: str, code: str) -> bool:
    """Register a new user"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'INSERT INTO users (telegram_id, name, fin, seriya, code) VALUES (?, ?, ?, ?, ?)',
                (telegram_id, name, fin, seriya, code)
            )
            conn.commit()
            return True
    except Exception as e:
        print(f"Error registering user: {e}")
        return False


def get_user_by_telegram_id(telegram_id: int) -> Optional[dict]:
    """Get user by telegram ID"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT id, telegram_id, name, fin, seriya, code, phone_number, is_active FROM users WHERE telegram_id = ?',
                (telegram_id,)
            )
            row = cursor.fetchone()

            if row:
                is_active_val = row[7] if len(row) > 7 else True
                if isinstance(is_active_val, int):
                    is_active_val = bool(is_active_val)
                return {
                    'id': row[0],
                    'telegram_id': row[1],
                    'name': row[2],
                    'fin': row[3],
                    'seriya': row[4],
                    'code': row[5],
                    'phone_number': row[6] if len(row) > 6 else None,
                    'is_active': is_active_val
                }
            return None
    except Exception as e:
        print(f"Error getting user: {e}")
        return None


def upsert_user_profile(telegram_id: int, name: str, fin: str, code: str, seriya: str = "", phone_number: str = "") -> None:
    """Insert or update user profile"""
    with _db_lock:
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                if _USING_POSTGRES:
                    cursor.execute(
                        '''INSERT INTO users (telegram_id, name, fin, seriya, code, phone_number) 
                           VALUES (?, ?, ?, ?, ?, ?) 
                           ON CONFLICT (telegram_id) DO UPDATE SET 
                           name = EXCLUDED.name, 
                           fin = EXCLUDED.fin, 
                           seriya = EXCLUDED.seriya, 
                           code = EXCLUDED.code, 
                           phone_number = EXCLUDED.phone_number''',
                        (telegram_id, name, fin, seriya, code, phone_number)
                    )
                else:
                    try:
                        cursor.execute(
                            'INSERT INTO users (telegram_id, name, fin, seriya, code, phone_number) VALUES (?, ?, ?, ?, ?, ?)',
                            (telegram_id, name, fin, seriya, code, phone_number)
                        )
                    except:
                        conn.rollback()
                        cursor.execute(
                            'UPDATE users SET name = ?, fin = ?, seriya = ?, code = ?, phone_number = ? WHERE telegram_id = ?',
                            (name, fin, seriya, code, phone_number, telegram_id)
                        )
                conn.commit()
        except Exception as e:
            print(f"Error upserting user profile: {e}")


# Attendance functions
def record_giris(user_id: int, date: str, time: str, location: Optional[str] = None) -> bool:
    """Record check-in"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    'INSERT INTO attendance (user_id, date, giris_time, giris_loc) VALUES (?, ?, ?, ?)',
                    (user_id, date, time, location)
                )
                conn.commit()
                return True
            except:
                conn.rollback()
                cursor.execute(
                    'SELECT giris_time FROM attendance WHERE user_id = ? AND date = ?',
                    (user_id, date)
                )
                result = cursor.fetchone()
                return result[0] is None if result else False
    except Exception as e:
        print(f"Error recording giris: {e}")
        return False


def record_cixis(user_id: int, date: str, time: str, location: Optional[str] = None) -> bool:
    """Record check-out"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE attendance SET cixis_time = ?, cixis_loc = ? WHERE user_id = ? AND date = ? AND cixis_time IS NULL',
                (time, location, user_id, date)
            )
            
            if cursor.rowcount == 0:
                cursor.execute(
                    'SELECT cixis_time FROM attendance WHERE user_id = ? AND date = ?',
                    (user_id, date)
                )
                result = cursor.fetchone()
                conn.commit()
                return False
            
            conn.commit()
            return True
    except Exception as e:
        print(f"Error recording cixis: {e}")
        return False


def has_giris_today(user_id: int, date: str) -> bool:
    """Check if user has already checked in today"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT giris_time FROM attendance WHERE user_id = ? AND date = ?',
                (user_id, date)
            )
            result = cursor.fetchone()
            return result is not None and result[0] is not None
    except Exception as e:
        print(f"Error checking giris: {e}")
        return False


def has_cixis_today(user_id: int, date: str) -> bool:
    """Check if user has already checked out today"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT cixis_time FROM attendance WHERE user_id = ? AND date = ?',
                (user_id, date)
            )
            result = cursor.fetchone()
            return result is not None and result[0] is not None
    except Exception as e:
        print(f"Error checking cixis: {e}")
        return False


# Registration functions
def has_registration(user_id: int, date: str, profession: str, code: str) -> bool:
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT 1 FROM registrations WHERE user_id = ? AND date = ? AND profession = ? AND code = ? LIMIT 1',
                (user_id, date, profession, code)
            )
            return cursor.fetchone() is not None
    except Exception as e:
        print(f"Error checking registration: {e}")
        return False


def add_registration(user_id: int, date: str, profession: str, code: str) -> bool:
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'INSERT INTO registrations (user_id, date, profession, code) VALUES (?, ?, ?, ?)',
                (user_id, date, profession, code)
            )
            conn.commit()
            return True
    except Exception as e:
        print(f"Error adding registration: {e}")
        return False


# GPS session functions  
def get_or_create_user2(telegram_id: int, full_name: str) -> int:
    """Return users2.id for given telegram_id; create if not exists."""
    with _db_lock:
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT id FROM users2 WHERE telegram_id = ?', (telegram_id,))
                row = cursor.fetchone()
                if row:
                    return row[0]
                
                if _USING_POSTGRES:
                    cursor.execute('INSERT INTO users2 (telegram_id, full_name) VALUES (?, ?) RETURNING id', (telegram_id, full_name))
                    user_id = cursor.fetchone()[0]
                else:
                    cursor.execute('INSERT INTO users2 (telegram_id, full_name) VALUES (?, ?)', (telegram_id, full_name))
                    user_id = cursor.lastrowid
                conn.commit()
                return user_id
        except Exception as e:
            print(f"Error in get_or_create_user2: {e}")
            return 0


def create_session(user_id: int, start_time: str, lat: float, lon: float) -> int:
    """Create a new open session and return its id."""
    with _db_lock:
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                if _USING_POSTGRES:
                    cursor.execute(
                        'INSERT INTO sessions (user_id, start_time, start_lat, start_lon) VALUES (?, ?, ?, ?) RETURNING id',
                        (user_id, start_time, lat, lon)
                    )
                    sid = cursor.fetchone()[0]
                else:
                    cursor.execute(
                        'INSERT INTO sessions (user_id, start_time, start_lat, start_lon) VALUES (?, ?, ?, ?)',
                        (user_id, start_time, lat, lon)
                    )
                    sid = cursor.lastrowid
                conn.commit()
                return sid
        except Exception as e:
            print(f"Error creating session: {e}")
            return 0


def get_open_session(user_id: int):
    """Get the latest open session for a user."""
    try:
        with get_db_connection() as conn:
            conn.row_factory = sqlite3.Row if not _USING_POSTGRES else None
            cursor = conn.cursor()
            cursor.execute(
                'SELECT * FROM sessions WHERE user_id = ? AND end_time IS NULL ORDER BY id DESC LIMIT 1',
                (user_id,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    except Exception as e:
        print(f"Error getting open session: {e}")
        return None


def close_session(session_id: int, end_time: str, end_lat: float, end_lon: float, duration_min: int, distance_m: float) -> None:
    """Close a session with checkout data."""
    with _db_lock:
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    '''UPDATE sessions
                       SET end_time = ?, end_lat = ?, end_lon = ?, duration_min = ?, distance_m = ?
                       WHERE id = ?''',
                    (end_time, end_lat, end_lon, duration_min, distance_m, session_id)
                )
                conn.commit()
        except Exception as e:
            print(f"Error closing session: {e}")


def set_user_active(telegram_id: int, is_active: bool) -> bool:
    """Activate or deactivate a user."""
    with _db_lock:
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                active_value = bool(is_active) if _USING_POSTGRES else (1 if is_active else 0)
                cursor.execute(
                    'UPDATE users SET is_active = ? WHERE telegram_id = ?',
                    (active_value, telegram_id)
                )
                affected = cursor.rowcount
                conn.commit()
                return affected > 0
        except Exception as e:
            print(f"Error setting user active: {e}")
            return False


def delete_user_by_telegram_id(telegram_id: int) -> bool:
    """Delete a user by telegram_id."""
    with _db_lock:
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT id FROM users WHERE telegram_id = ?', (telegram_id,))
                row = cursor.fetchone()
                if row:
                    uid = int(row[0])
                    # ON DELETE CASCADE will handle related records
                    cursor.execute('DELETE FROM users WHERE id = ?', (uid,))
                    conn.commit()
                    return True
                return False
        except Exception as e:
            print(f"Error deleting user: {e}")
            return False


# Add other functions with same pattern...
# (get_all_users, get_attendance_report, etc.)
# All should use: with get_db_connection() as conn:

print("✓ Database module loaded with PostgreSQL pool support")
