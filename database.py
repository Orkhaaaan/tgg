"""Database management for Worker Attendance Bot."""
import os
import sqlite3
import time
from datetime import datetime, timedelta
from threading import Lock
from typing import List, Optional, Tuple

_DATABASE_URL = os.getenv('DATABASE_URL', '').strip()
if not _DATABASE_URL:
    raise RuntimeError('DATABASE_URL is required (PostgreSQL only)')

import psycopg2
import psycopg2.extras
from psycopg2 import pool

_USING_POSTGRES = True

# Global connection pool for PostgreSQL
_pg_pool: Optional["pool.SimpleConnectionPool"] = None
_pool_lock = Lock()

DB_FILE = 'attendance.db'
_db_lock = Lock()

GROUP_CODE_NO_EXPIRY_DATE = os.getenv('GROUP_CODE_NO_EXPIRY_DATE', '9999-12-31')

def initialize_pool() -> None:
    """Initialize PostgreSQL connection pool. Call once at startup."""
    global _pg_pool
    with _pool_lock:
        if _pg_pool is not None:
            return

        last_err: Optional[Exception] = None
        for attempt in range(12):
            try:
                try:
                    _pg_pool = pool.SimpleConnectionPool(
                        minconn=5,
                        maxconn=100,
                        dsn=_DATABASE_URL,
                        sslmode=os.getenv('PGSSLMODE', 'require'),
                    )
                except TypeError:
                    _pg_pool = pool.SimpleConnectionPool(
                        minconn=5,
                        maxconn=100,
                        dsn=_DATABASE_URL,
                    )
                print('✓ PostgreSQL connection pool initialized (5-100 connections)')
                return
            except Exception as e:
                last_err = e
                msg = str(e).lower()
                if 'starting up' in msg or 'could not connect' in msg or 'connection refused' in msg:
                    time.sleep(1 + attempt * 0.5)
                    continue
                raise
        raise last_err if last_err else RuntimeError('Failed to initialize PostgreSQL pool')

def close_pool() -> None:
    global _pg_pool
    with _pool_lock:
        if _pg_pool is not None:
            _pg_pool.closeall()
            _pg_pool = None

def _qmark_to_percent_s(query: str) -> str:
    out: list[str] = []
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
    def __init__(self, cur):
        self._cur = cur
        self.lastrowid = None

    @property
    def rowcount(self):
        return self._cur.rowcount

    def execute(self, query, params=None):
        q = str(query)
        if q.strip().upper().startswith('PRAGMA'):
            return None
        q = _qmark_to_percent_s(q)
        if params is None:
            params = ()
        return self._cur.execute(q, params)

    def executemany(self, query, params_seq):
        q = str(query)
        if q.strip().upper().startswith('PRAGMA'):
            return None
        q = _qmark_to_percent_s(q)
        return self._cur.executemany(q, params_seq)

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

class _PgCompatConnection:
    def __init__(self, conn, from_pool: bool = False):
        self._conn = conn
        self._from_pool = from_pool
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
        if self._from_pool:
            if _pg_pool is not None:
                _pg_pool.putconn(self._conn)
        else:
            return self._conn.close()

def _pg_connect(_ignored_db_file=None, timeout=None, **_kwargs):
    if _pg_pool is None:
        raise RuntimeError('Connection pool not initialized. Call initialize_pool() first.')
    raw_conn = _pg_pool.getconn()
    return _PgCompatConnection(raw_conn, from_pool=True)

sqlite3.connect = _pg_connect  # type: ignore[assignment]
sqlite3.IntegrityError = psycopg2.IntegrityError  # type: ignore[attr-defined]
sqlite3.OperationalError = psycopg2.OperationalError  # type: ignore[attr-defined]
sqlite3.Row = object()  # type: ignore[attr-defined]

def init_db() -> None:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    if _USING_POSTGRES:
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS users (
                id BIGSERIAL PRIMARY KEY,
                telegram_id BIGINT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                fin TEXT NOT NULL,
                seriya TEXT DEFAULT '',
                code TEXT NOT NULL,
                phone_number TEXT DEFAULT '',
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                registered_at TIMESTAMPTZ DEFAULT NOW()
            )
            '''
        )
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS attendance (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id),
                date DATE NOT NULL,
                giris_time TEXT,
                cixis_time TEXT,
                giris_loc TEXT,
                cixis_loc TEXT,
                UNIQUE(user_id, date)
            )
            '''
        )
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS codes (
                id BIGSERIAL PRIMARY KEY,
                code TEXT UNIQUE NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                expires_at TIMESTAMPTZ NOT NULL
            )
            '''
        )
    else:
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                telegram_id INTEGER UNIQUE NOT NULL,
                name TEXT NOT NULL,
                fin TEXT NOT NULL,
                seriya TEXT DEFAULT '',
                code TEXT NOT NULL,
                phone_number TEXT DEFAULT '',
                is_active INTEGER NOT NULL DEFAULT 1,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS attendance (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                giris_time TEXT,
                cixis_time TEXT,
                giris_loc TEXT,
                cixis_loc TEXT,
                UNIQUE(user_id, date),
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
            '''
        )
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS codes (
                id INTEGER PRIMARY KEY,
                code TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL
            )
            '''
        )

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_code ON users(code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_active ON users(is_active)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_attendance_date ON attendance(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_attendance_user_date ON attendance(user_id, date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_codes_expires ON codes(expires_at)')
    conn.commit()
    conn.close()

def record_giris(user_id: int, date: str, time_str: str, location: Optional[str] = None) -> bool:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    try:
        cursor.execute(
            'INSERT INTO attendance (user_id, date, giris_time, giris_loc) VALUES (?, ?, ?, ?)',
            (user_id, date, time_str, location),
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        cursor.execute('SELECT giris_time FROM attendance WHERE user_id = ? AND date = ?', (user_id, date))
        result = cursor.fetchone()
        conn.close()
        return result[0] is None if result else False

def record_cixis(user_id: int, date: str, time_str: str, location: Optional[str] = None) -> bool:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        'UPDATE attendance SET cixis_time = ?, cixis_loc = ? WHERE user_id = ? AND date = ? AND cixis_time IS NULL',
        (time_str, location, user_id, date),
    )
    ok = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return ok

def get_attendance_logs(date: Optional[str] = None, profession: Optional[str] = None, code: Optional[str] = None) -> List[dict]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    base = (
        'SELECT a.date, u.name, u.fin, u.code, '
        'COALESCE(r.profession, ?) AS profession, '
        'a.giris_time, a.cixis_time, a.giris_loc, a.cixis_loc '
        'FROM attendance a '
        'JOIN users u ON a.user_id = u.id '
        'LEFT JOIN registrations r ON r.user_id = u.id AND r.date = a.date '
    )
    params: list = ['-']
    conds: list[str] = []
    if date:
        conds.append('a.date = ?')
        params.append(date)
    if profession:
        conds.append('r.profession = ?')
        params.append(profession)
    if code:
        conds.append('u.code = ?')
        params.append(code)
    if conds:
        base += ' WHERE ' + ' AND '.join(conds)
    base += ' ORDER BY a.date DESC, r.profession, u.name'
    cursor.execute(base, tuple(params))
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows

def init_registrations() -> None:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    if _USING_POSTGRES:
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS registrations (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id),
                date DATE NOT NULL,
                profession TEXT NOT NULL,
                code TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(user_id, date, code, profession)
            )
            '''
        )
    else:
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS registrations (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                profession TEXT NOT NULL,
                code TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, date, code, profession),
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
            '''
        )
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_regs_date ON registrations(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_regs_code ON registrations(code)')
    conn.commit()
    conn.close()

def get_registrations_summary(date: str) -> List[dict]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        'SELECT profession, code, COUNT(*) AS cnt FROM registrations WHERE date = ? GROUP BY profession, code ORDER BY profession, code',
        (date,),
    )
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows

def add_registration(user_id: int, date: str, profession: str, code: str) -> bool:
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO registrations (user_id, date, profession, code) VALUES (?, ?, ?, ?)',
            (user_id, date, profession, code),
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        return False

def init_group_codes() -> None:
    with _db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        try:
            cursor = conn.cursor()
            cursor.execute('PRAGMA busy_timeout=5000')
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS group_codes (
                    id {id_type} PRIMARY KEY,
                    profession TEXT NOT NULL,
                    date {date_type} NOT NULL,
                    code TEXT NOT NULL,
                    expires_at {date_type} NOT NULL,
                    is_active {active_type} NOT NULL DEFAULT {active_default},
                    UNIQUE(profession, date, code)
                )
                '''
                .format(
                    id_type='BIGSERIAL' if _USING_POSTGRES else 'INTEGER',
                    date_type='DATE' if _USING_POSTGRES else 'TEXT',
                    active_type='BOOLEAN' if _USING_POSTGRES else 'INTEGER',
                    active_default='TRUE' if _USING_POSTGRES else '1',
                )
            )
            if _USING_POSTGRES:
                try:
                    cursor.execute('ALTER TABLE group_codes ADD COLUMN IF NOT EXISTS expires_at DATE')
                except Exception:
                    try:
                        conn.rollback()
                        cursor = conn.cursor()
                    except Exception:
                        pass
                try:
                    cursor.execute('UPDATE group_codes SET expires_at = %s::date WHERE expires_at IS NULL', (GROUP_CODE_NO_EXPIRY_DATE,))
                except Exception:
                    try:
                        conn.rollback()
                        cursor = conn.cursor()
                    except Exception:
                        pass
            else:
                try:
                    cursor.execute('PRAGMA table_info(group_codes)')
                    cols = [c[1] for c in cursor.fetchall()]
                    if 'expires_at' not in cols:
                        cursor.execute('ALTER TABLE group_codes ADD COLUMN expires_at TEXT')
                except Exception:
                    pass
                try:
                    cursor.execute(
                        'UPDATE group_codes SET expires_at = ? WHERE expires_at IS NULL OR expires_at = ""',
                        (GROUP_CODE_NO_EXPIRY_DATE,),
                    )
                except Exception:
                    pass
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_group_codes_date ON group_codes(date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_group_codes_prof ON group_codes(profession)')
            conn.commit()
        finally:
            conn.close()

def add_group_code(profession: str, date: str, code: str, is_active: int = 1, expires_at: Optional[str] = None) -> bool:
    if expires_at is None:
        expires_at = GROUP_CODE_NO_EXPIRY_DATE
    with _db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        try:
            cursor = conn.cursor()
            cursor.execute('PRAGMA busy_timeout=5000')
            cursor.execute(
                'INSERT INTO group_codes (profession, date, code, expires_at, is_active) VALUES (?, ?, ?, ?, ?)',
                (profession, date, code, expires_at, is_active),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            try:
                conn.rollback()
            except Exception:
                pass
            return False
        finally:
            conn.close()
 
 
def get_attendance_logs(date: Optional[str] = None, profession: Optional[str] = None, code: Optional[str] = None) -> List[dict]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    base = (
        'SELECT a.date, u.name, u.fin, u.code, '
        'COALESCE(r.profession, ?) AS profession, '
        'a.giris_time, a.cixis_time, a.giris_loc, a.cixis_loc '
        'FROM attendance a '
        'JOIN users u ON a.user_id = u.id '
        'LEFT JOIN registrations r ON r.user_id = u.id AND r.date = a.date '
    )
    params: list = ['-']
    conds: list[str] = []
    if date:
        conds.append('a.date = ?')
        params.append(date)
    if profession:
        conds.append('r.profession = ?')
        params.append(profession)
    if code:
        conds.append('u.code = ?')
        params.append(code)
    if conds:
        base += ' WHERE ' + ' AND '.join(conds)
    base += ' ORDER BY a.date DESC, r.profession, u.name'
    cursor.execute(base, tuple(params))
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows
 
 
def init_registrations() -> None:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS registrations (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users(id),
            date DATE NOT NULL,
            profession TEXT NOT NULL,
            code TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(user_id, date, code, profession)
        )
        '''
    )
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_regs_date ON registrations(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_regs_code ON registrations(code)')
    conn.commit()
    conn.close()
 
 
def get_registrations_summary(date: str) -> List[dict]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        'SELECT profession, code, COUNT(*) AS cnt FROM registrations WHERE date = ? GROUP BY profession, code ORDER BY profession, code',
        (date,),
    )
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows
 
 
def add_registration(user_id: int, date: str, profession: str, code: str) -> bool:
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO registrations (user_id, date, profession, code) VALUES (?, ?, ?, ?)',
            (user_id, date, profession, code),
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        return False
 
 
def init_group_codes() -> None:
    with _db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        try:
            cursor = conn.cursor()
            cursor.execute('PRAGMA busy_timeout=5000')
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS group_codes (
                    id {id_type} PRIMARY KEY,
                    profession TEXT NOT NULL,
                    date {date_type} NOT NULL,
                    code TEXT NOT NULL,
                    expires_at {date_type} NOT NULL,
                    is_active {active_type} NOT NULL DEFAULT {active_default},
                    UNIQUE(profession, date, code)
                )
                '''
                .format(
                    id_type='BIGSERIAL' if _USING_POSTGRES else 'INTEGER',
                    date_type='DATE' if _USING_POSTGRES else 'TEXT',
                    active_type='BOOLEAN' if _USING_POSTGRES else 'INTEGER',
                    active_default='TRUE' if _USING_POSTGRES else '1',
                )
            )
            if _USING_POSTGRES:
                try:
                    cursor.execute('ALTER TABLE group_codes ADD COLUMN IF NOT EXISTS expires_at DATE')
                except Exception:
                    try:
                        conn.rollback()
                        cursor = conn.cursor()
                    except Exception:
                        pass
                try:
                    cursor.execute('UPDATE group_codes SET expires_at = %s::date WHERE expires_at IS NULL', (GROUP_CODE_NO_EXPIRY_DATE,))
                except Exception:
                    try:
                        conn.rollback()
                        cursor = conn.cursor()
                    except Exception:
                        pass
            else:
                try:
                    cursor.execute('PRAGMA table_info(group_codes)')
                    cols = [c[1] for c in cursor.fetchall()]
                    if 'expires_at' not in cols:
                        cursor.execute('ALTER TABLE group_codes ADD COLUMN expires_at TEXT')
                except Exception:
                    pass
                try:
                    cursor.execute(
                        'UPDATE group_codes SET expires_at = ? WHERE expires_at IS NULL OR expires_at = ""',
                        (GROUP_CODE_NO_EXPIRY_DATE,),
                    )
                except Exception:
                    pass
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_group_codes_date ON group_codes(date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_group_codes_prof ON group_codes(profession)')
            conn.commit()
        finally:
            conn.close()
 
 
def add_group_code(profession: str, date: str, code: str, is_active: int = 1, expires_at: Optional[str] = None) -> bool:
    if expires_at is None:
        expires_at = GROUP_CODE_NO_EXPIRY_DATE
    with _db_lock:
        try:
            conn = sqlite3.connect(DB_FILE, timeout=10)
            try:
                cursor = conn.cursor()
                cursor.execute('PRAGMA busy_timeout=5000')
                cursor.execute(
                    'INSERT INTO group_codes (profession, date, code, expires_at, is_active) VALUES (?, ?, ?, ?, ?)',
                    (profession, date, code, expires_at, (bool(is_active) if _USING_POSTGRES else (1 if is_active else 0))),
                )
                conn.commit()
                return True
            finally:
                conn.close()
        except sqlite3.IntegrityError:
            conn = sqlite3.connect(DB_FILE, timeout=10)
            try:
                cursor = conn.cursor()
                cursor.execute('PRAGMA busy_timeout=5000')
                cursor.execute(
                    'UPDATE group_codes SET is_active = ?, expires_at = ? WHERE profession = ? AND date = ? AND code = ?',
                    ((bool(is_active) if _USING_POSTGRES else (1 if is_active else 0)), expires_at, profession, date, code),
                )
                conn.commit()
                return True
            finally:
                conn.close()
 
 
def delete_group_code(profession: str, date: str, code: str) -> bool:
    with _db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        try:
            cursor = conn.cursor()
            cursor.execute('PRAGMA busy_timeout=5000')
            cursor.execute('DELETE FROM group_codes WHERE profession = ? AND date = ? AND code = ?', (profession, date, code))
            affected = cursor.rowcount
            conn.commit()
            return affected > 0
        finally:
            conn.close()
 
 
def get_group_codes(date: Optional[str] = None, only_active: Optional[bool] = None, active_on: Optional[str] = None) -> List[dict]:
    with _db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.cursor()
            cursor.execute('PRAGMA busy_timeout=5000')
            query = 'SELECT profession, date, code, expires_at, is_active FROM group_codes'
            params: list = []
            conds: list[str] = []
            if date:
                conds.append('date = ?')
                params.append(date)
            if active_on:
                conds.append('date <= ?')
                params.append(active_on)
                conds.append('(expires_at >= ? OR expires_at IS NULL OR expires_at = "")')
                params.append(active_on)
            if only_active is True:
                conds.append('is_active = {active}'.format(active='TRUE' if _USING_POSTGRES else '1'))
            elif only_active is False:
                conds.append('is_active = {inactive}'.format(inactive='FALSE' if _USING_POSTGRES else '0'))
            if conds:
                query += ' WHERE ' + ' AND '.join(conds)
            query += ' ORDER BY date DESC, profession'
            cursor.execute(query, tuple(params))
            return [dict(r) for r in cursor.fetchall()]
        finally:
            conn.close()
 
 
def is_group_code_valid(profession: str, code: str, on_date: Optional[str] = None) -> bool:
    if on_date is None:
        on_date = datetime.now().date().isoformat()
    with _db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        try:
            cursor = conn.cursor()
            cursor.execute('PRAGMA busy_timeout=5000')
            cursor.execute(
                'SELECT 1 FROM group_codes WHERE profession = ? AND code = ? AND date <= ? AND (expires_at >= ? OR expires_at IS NULL OR expires_at = "") AND is_active = {active} LIMIT 1'.format(
                    active='TRUE' if _USING_POSTGRES else '1'
                ),
                (profession, code, on_date, on_date),
            )
            return cursor.fetchone() is not None
        finally:
            conn.close()
 
 
def init_gps_tables() -> None:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    if _USING_POSTGRES:
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS users2 (
                id BIGSERIAL PRIMARY KEY,
                telegram_id BIGINT UNIQUE NOT NULL,
                full_name TEXT NOT NULL
            )
            '''
        )
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS sessions (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users2(id),
                start_time TIMESTAMPTZ NOT NULL,
                start_lat DOUBLE PRECISION NOT NULL,
                start_lon DOUBLE PRECISION NOT NULL,
                end_time TIMESTAMPTZ,
                end_lat DOUBLE PRECISION,
                end_lon DOUBLE PRECISION,
                duration_min INTEGER,
                distance_m DOUBLE PRECISION
            )
            '''
        )
    else:
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS users2 (
                id INTEGER PRIMARY KEY,
                telegram_id INTEGER UNIQUE NOT NULL,
                full_name TEXT NOT NULL
            )
            '''
        )
        cursor.execute(
            '''
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
                FOREIGN KEY(user_id) REFERENCES users2(id)
            )
            '''
        )
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_users2_tid ON users2(telegram_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessions_user_open ON sessions(user_id, end_time)')
    conn.commit()
    conn.close()
 
 
def get_or_create_user2(telegram_id: int, full_name: str) -> int:
     with _db_lock:
         conn = sqlite3.connect(DB_FILE)
         try:
             cursor = conn.cursor()
             cursor.execute('SELECT id FROM users2 WHERE telegram_id = ?', (telegram_id,))
             row = cursor.fetchone()
             if row:
                 return row[0]
             if _USING_POSTGRES:
                 cursor.execute(
                     'INSERT INTO users2 (telegram_id, full_name) VALUES (?, ?) RETURNING id',
                     (telegram_id, full_name),
                 )
                 uid = cursor.fetchone()[0]
             else:
                 cursor.execute('INSERT INTO users2 (telegram_id, full_name) VALUES (?, ?)', (telegram_id, full_name))
                 uid = cursor.lastrowid
             conn.commit()
             return uid
         finally:
             conn.close()
 
 
def create_session(user_id: int, start_time: str, lat: float, lon: float) -> int:
     with _db_lock:
         conn = sqlite3.connect(DB_FILE)
         cursor = conn.cursor()
         if _USING_POSTGRES:
             cursor.execute(
                 'INSERT INTO sessions (user_id, start_time, start_lat, start_lon) VALUES (?, ?, ?, ?) RETURNING id',
                 (user_id, start_time, lat, lon),
             )
             sid = cursor.fetchone()[0]
         else:
             cursor.execute(
                 'INSERT INTO sessions (user_id, start_time, start_lat, start_lon) VALUES (?, ?, ?, ?)',
                 (user_id, start_time, lat, lon),
             )
             sid = cursor.lastrowid
         conn.commit()
         conn.close()
         return sid
 
 
def get_open_session(user_id: int):
     conn = sqlite3.connect(DB_FILE)
     conn.row_factory = sqlite3.Row
     cursor = conn.cursor()
     cursor.execute('SELECT * FROM sessions WHERE user_id = ? AND end_time IS NULL ORDER BY id DESC LIMIT 1', (user_id,))
     row = cursor.fetchone()
     conn.close()
     return dict(row) if row else None
 
 
def close_session(session_id: int, end_time: str, end_lat: float, end_lon: float, duration_min: int, distance_m: float) -> None:
     with _db_lock:
         conn = sqlite3.connect(DB_FILE)
         cursor = conn.cursor()
         cursor.execute(
             'UPDATE sessions SET end_time = ?, end_lat = ?, end_lon = ?, duration_min = ?, distance_m = ? WHERE id = ?',
             (end_time, end_lat, end_lon, duration_min, distance_m, session_id),
         )
         conn.commit()
         conn.close()
 
 
def get_user_session_on_date(user_id: int, iso_date: str):
     conn = sqlite3.connect(DB_FILE)
     conn.row_factory = sqlite3.Row
     cursor = conn.cursor()
     if _USING_POSTGRES:
         cursor.execute(
             'SELECT * FROM sessions WHERE user_id = ? AND start_time::date = ?::date ORDER BY id DESC LIMIT 1',
             (user_id, iso_date),
         )
     else:
         cursor.execute(
             'SELECT * FROM sessions WHERE user_id = ? AND substr(start_time, 1, 10) = ? ORDER BY id DESC LIMIT 1',
             (user_id, iso_date),
         )
     row = cursor.fetchone()
     conn.close()
     return dict(row) if row else None


def get_today_sessions(today_iso_date: str):
     conn = sqlite3.connect(DB_FILE)
     conn.row_factory = sqlite3.Row
     cursor = conn.cursor()
     if _USING_POSTGRES:
         cursor.execute(
             '''
             SELECT s.*, 
                    COALESCE(u_reg.name, u2.full_name, '-') as display_name,
                    u2.full_name
             FROM sessions s
             JOIN users2 u2 ON s.user_id = u2.id
             LEFT JOIN users u_reg ON u2.telegram_id = u_reg.telegram_id
             WHERE s.start_time::date = %s::date
             ORDER BY s.id DESC
             ''',
             (today_iso_date,),
         )
     else:
         cursor.execute(
             '''
             SELECT s.*, 
                    COALESCE(u_reg.name, u2.full_name, '?') as display_name,
                    u2.full_name
             FROM sessions s
             JOIN users2 u2 ON s.user_id = u2.id
             LEFT JOIN users u_reg ON u2.telegram_id = u_reg.telegram_id
             WHERE substr(s.start_time, 1, 10) = ?
             ORDER BY s.id DESC
             ''',
             (today_iso_date,),
         )
     rows = cursor.fetchall()
     conn.close()
     return [dict(r) for r in rows]


def delete_user_all(telegram_id: int) -> bool:
    affected = 0
    with _db_lock:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT id FROM users WHERE telegram_id = ?', (telegram_id,))
        row = cursor.fetchone()
        if row:
            uid = int(row[0])
            cursor.execute('DELETE FROM attendance WHERE user_id = ?', (uid,))
            affected += cursor.rowcount
            cursor.execute('DELETE FROM registrations WHERE user_id = ?', (uid,))
            affected += cursor.rowcount
            cursor.execute('DELETE FROM users WHERE id = ?', (uid,))
            affected += cursor.rowcount
        cursor.execute('SELECT id FROM users2 WHERE telegram_id = ?', (telegram_id,))
        row2 = cursor.fetchone()
        if row2:
            u2id = int(row2[0])
            cursor.execute('DELETE FROM sessions WHERE user_id = ?', (u2id,))
            affected += cursor.rowcount
            cursor.execute('DELETE FROM users2 WHERE id = ?', (u2id,))
            affected += cursor.rowcount
        conn.commit()
        conn.close()
    return affected > 0


def get_last_registration_date(user_id: int) -> Optional[str]:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT date FROM registrations WHERE user_id = ? ORDER BY date DESC LIMIT 1', (user_id,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    val = row[0]
    if isinstance(val, datetime):
        return val.date().isoformat()
    return str(val)


def has_registration(user_id: int, date: str, profession: str, code: str) -> bool:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        'SELECT 1 FROM registrations WHERE user_id = ? AND date = ? AND profession = ? AND code = ? LIMIT 1',
        (user_id, date, profession, code),
    )
    row = cursor.fetchone()
    conn.close()
    return row is not None


def get_registrations(date: Optional[str] = None, profession: Optional[str] = None, code: Optional[str] = None) -> List[dict]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    query = 'SELECT r.date, r.profession, r.code, u.name, u.fin FROM registrations r JOIN users u ON r.user_id = u.id'
    params: list = []
    conds: list[str] = []
    if date:
        conds.append('r.date = ?')
        params.append(date)
    if profession:
        conds.append('r.profession = ?')
        params.append(profession)
    if code:
        conds.append('r.code = ?')
        params.append(code)
    if conds:
        query += ' WHERE ' + ' AND '.join(conds)
    query += ' ORDER BY r.date DESC, r.profession, u.name'
    cursor.execute(query, tuple(params))
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def upsert_user_profile(telegram_id: int, name: str, fin: str, code: str, seriya: str = '', phone_number: str = '') -> None:
    with _db_lock:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        if _USING_POSTGRES:
            cursor.execute(
                'INSERT INTO users (telegram_id, name, fin, seriya, code, phone_number) '
                'VALUES (?, ?, ?, ?, ?, ?) '
                'ON CONFLICT (telegram_id) DO UPDATE SET '
                'name = EXCLUDED.name, fin = EXCLUDED.fin, seriya = EXCLUDED.seriya, code = EXCLUDED.code, phone_number = EXCLUDED.phone_number',
                (telegram_id, name, fin, seriya, code, phone_number),
            )
        else:
            try:
                cursor.execute(
                    'INSERT INTO users (telegram_id, name, fin, seriya, code, phone_number) VALUES (?, ?, ?, ?, ?, ?)',
                    (telegram_id, name, fin, seriya, code, phone_number),
                )
            except sqlite3.IntegrityError:
                conn.rollback()
                cursor.execute(
                    'UPDATE users SET name = ?, fin = ?, seriya = ?, code = ?, phone_number = ? WHERE telegram_id = ?',
                    (name, fin, seriya, code, phone_number, telegram_id),
                )
        conn.commit()
        conn.close()


def get_all_workers_status(code: Optional[str] = None) -> List[dict]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    if code:
        cursor.execute(
            '''
            SELECT 
                u.telegram_id,
                u.name,
                u.fin,
                u.code,
                u.registered_at,
                (SELECT date FROM attendance WHERE user_id = u.id ORDER BY date DESC LIMIT 1) as last_date,
                (SELECT giris_time FROM attendance WHERE user_id = u.id ORDER BY date DESC LIMIT 1) as last_giris,
                (SELECT cixis_time FROM attendance WHERE user_id = u.id ORDER BY date DESC LIMIT 1) as last_cixis
            FROM users u
            WHERE u.code = ?
            ORDER BY u.name
            ''',
            (code,),
        )
    else:
        cursor.execute(
            '''
            SELECT 
                u.telegram_id,
                u.name,
                u.fin,
                u.code,
                u.registered_at,
                (SELECT date FROM attendance WHERE user_id = u.id ORDER BY date DESC LIMIT 1) as last_date,
                (SELECT giris_time FROM attendance WHERE user_id = u.id ORDER BY date DESC LIMIT 1) as last_giris,
                (SELECT cixis_time FROM attendance WHERE user_id = u.id ORDER BY date DESC LIMIT 1) as last_cixis
            FROM users u
            ORDER BY u.code, u.name
            '''
        )
    results = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return results


def get_active_students_count(date: Optional[str] = None) -> int:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    if date:
        if _USING_POSTGRES:
            cursor.execute(
                '''
                SELECT COUNT(DISTINCT u.id)
                FROM users u
                LEFT JOIN registrations r ON r.user_id = u.id AND r.date = ?::date
                WHERE u.is_active = TRUE AND r.user_id IS NOT NULL
                ''',
                (date,),
            )
        else:
            cursor.execute(
                '''
                SELECT COUNT(DISTINCT u.id)
                FROM users u
                LEFT JOIN registrations r ON r.user_id = u.id AND r.date = ?
                WHERE u.is_active = 1 AND r.user_id IS NOT NULL
                ''',
                (date,),
            )
    else:
        cursor.execute('SELECT COUNT(*) FROM users WHERE is_active = {active}'.format(active='TRUE' if _USING_POSTGRES else '1'))
    count = cursor.fetchone()[0]
    conn.close()
    return int(count or 0)


def get_total_registered_students() -> dict:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM users')
    total = cursor.fetchone()[0] or 0
    cursor.execute('SELECT COUNT(*) FROM users WHERE is_active = {active}'.format(active='TRUE' if _USING_POSTGRES else '1'))
    active = cursor.fetchone()[0] or 0
    cursor.execute('SELECT COUNT(*) FROM users WHERE is_active = {inactive}'.format(inactive='FALSE' if _USING_POSTGRES else '0'))
    inactive = cursor.fetchone()[0] or 0
    conn.close()
    return {'total': total, 'active': active, 'inactive': inactive}


def get_daily_report_for_excel(date: str) -> List[dict]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    if _USING_POSTGRES:
        cursor.execute(
            '''
            SELECT 
                u.id,
                u.telegram_id,
                u.name,
                u.fin,
                u.seriya,
                u.code,
                u.phone_number,
                u.is_active,
                COALESCE(a.giris_time, '') as giris_time,
                COALESCE(a.cixis_time, '') as cixis_time,
                COALESCE(a.giris_loc, '') as giris_loc,
                COALESCE(a.cixis_loc, '') as cixis_loc,
                COALESCE(
                    r_today.profession,
                    (SELECT profession FROM registrations WHERE user_id = u.id ORDER BY date DESC LIMIT 1),
                    '-'
                ) as profession,
                s.start_lat,
                s.start_lon,
                s.end_lat,
                s.end_lon
            FROM users u
            LEFT JOIN attendance a ON u.id = a.user_id AND a.date = ?::date
            LEFT JOIN registrations r_today ON r_today.user_id = u.id AND r_today.date = ?::date
            LEFT JOIN users2 u2 ON u.telegram_id = u2.telegram_id
            LEFT JOIN sessions s ON s.user_id = u2.id AND s.start_time::date = ?::date
            WHERE (u.registered_at IS NULL OR u.registered_at::date <= ?::date)
            ORDER BY u.code, u.name
            ''',
            (date, date, date, date),
        )
    else:
        cursor.execute(
            '''
            SELECT 
                u.id,
                u.telegram_id,
                u.name,
                u.fin,
                u.seriya,
                u.code,
                u.phone_number,
                u.is_active,
                COALESCE(a.giris_time, '') as giris_time,
                COALESCE(a.cixis_time, '') as cixis_time,
                COALESCE(a.giris_loc, '') as giris_loc,
                COALESCE(a.cixis_loc, '') as cixis_loc,
                COALESCE(
                    r_today.profession,
                    (SELECT profession FROM registrations WHERE user_id = u.id ORDER BY date DESC LIMIT 1),
                    '-'
                ) as profession,
                s.start_lat,
                s.start_lon,
                s.end_lat,
                s.end_lon
            FROM users u
            LEFT JOIN attendance a ON u.id = a.user_id AND a.date = ?
            LEFT JOIN registrations r_today ON r_today.user_id = u.id AND r_today.date = ?
            LEFT JOIN users2 u2 ON u.telegram_id = u2.telegram_id
            LEFT JOIN sessions s ON s.user_id = u2.id AND substr(s.start_time, 1, 10) = ?
            WHERE (u.registered_at IS NULL OR date(u.registered_at) <= date(?))
            ORDER BY u.code, u.name
            ''',
            (date, date, date, date),
        )
    results: list[dict] = []
    for r in cursor.fetchall():
        d = dict(r)
        if d.get('giris_time') == '':
            d['giris_time'] = None
        if d.get('cixis_time') == '':
            d['cixis_time'] = None
        if d.get('giris_loc') == '':
            d['giris_loc'] = None
        if d.get('cixis_loc') == '':
            d['cixis_loc'] = None
        results.append(d)
    conn.close()
    return results


def get_period_report_for_excel(start_date: str, end_date: str, code: Optional[str] = None) -> List[dict]:
     start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
     end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
     results: list[dict] = []
     cur = start_dt
     while cur <= end_dt:
         d = cur.isoformat()
         day_rows = get_daily_report_for_excel(d)
         if code:
             day_rows = [r for r in day_rows if r.get('code') == code]
         results.extend(day_rows)
         cur += timedelta(days=1)
     return results


def set_user_active(telegram_id: int, is_active: bool) -> bool:
     with _db_lock:
         conn = sqlite3.connect(DB_FILE)
         cursor = conn.cursor()
         active_value = bool(is_active) if _USING_POSTGRES else (1 if is_active else 0)
         cursor.execute('UPDATE users SET is_active = ? WHERE telegram_id = ?', (active_value, telegram_id))
         affected = cursor.rowcount
         conn.commit()
         conn.close()
         return affected > 0


def deactivate_user_by_code(code: str) -> int:
     with _db_lock:
         conn = sqlite3.connect(DB_FILE)
         cursor = conn.cursor()
         cursor.execute(
             'UPDATE users SET is_active = {inactive} WHERE code = ?'.format(inactive='FALSE' if _USING_POSTGRES else '0'),
             (code,),
         )
         affected = cursor.rowcount
         conn.commit()
         conn.close()
         return int(affected or 0)


# === Codes table helpers ===

def add_code(code: str, days_valid: int = 30) -> bool:
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        expires_at = datetime.now() + timedelta(days=days_valid)
        cursor.execute('INSERT INTO codes (code, expires_at) VALUES (?, ?)', (code, expires_at))
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        return False


def remove_code(code: str) -> bool:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM codes WHERE code = ?', (code,))
    affected = cursor.rowcount
    conn.commit()
    conn.close()
    return affected > 0


def is_code_valid(code: str) -> bool:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM codes WHERE code = ? AND expires_at > ?', (code, datetime.now()))
    count = cursor.fetchone()[0]
    conn.close()
    return (count or 0) > 0


def get_all_codes() -> List[Tuple]:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT code, created_at, expires_at FROM codes WHERE expires_at > ? ORDER BY created_at DESC', (datetime.now(),))
    rows = cursor.fetchall()
    conn.close()
    return rows


# === Legacy user helpers ===

def register_user(telegram_id: int, name: str, fin: str, seriya: str, code: str) -> bool:
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO users (telegram_id, name, fin, seriya, code) VALUES (?, ?, ?, ?, ?)',
            (telegram_id, name, fin, seriya, code),
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        return False


def get_all_users() -> List[dict]:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT telegram_id, name FROM users')
    users = [{'telegram_id': row[0], 'name': row[1]} for row in cursor.fetchall()]
    conn.close()
    return users


def get_all_users_with_status(code: Optional[str] = None, only_active: Optional[bool] = None) -> List[dict]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    query = 'SELECT id, telegram_id, name, fin, seriya, code, phone_number, is_active FROM users WHERE 1=1'
    params: list = []
    if code:
        query += ' AND code = ?'
        params.append(code)
    if only_active is True:
        query += ' AND is_active = {active}'.format(active='TRUE' if _USING_POSTGRES else '1')
    elif only_active is False:
        query += ' AND is_active = {inactive}'.format(inactive='FALSE' if _USING_POSTGRES else '0')
    query += ' ORDER BY code, name'
    cursor.execute(query, tuple(params))
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def get_users_by_code(code: str, only_active: Optional[bool] = None) -> List[dict]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    query = 'SELECT id, telegram_id, name, fin, seriya, code, phone_number, is_active FROM users WHERE code = ?'
    params: list = [code]
    if only_active is True:
        query += ' AND is_active = {active}'.format(active='TRUE' if _USING_POSTGRES else '1')
    elif only_active is False:
        query += ' AND is_active = {inactive}'.format(inactive='FALSE' if _USING_POSTGRES else '0')
    query += ' ORDER BY name'
    cursor.execute(query, tuple(params))
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def delete_user_by_telegram_id(telegram_id: int) -> bool:
    return delete_user_all(telegram_id)


# === Attendance helpers ===

def has_giris_today(user_id: int, date: str) -> bool:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT giris_time FROM attendance WHERE user_id = ? AND date = ?', (user_id, date))
    row = cursor.fetchone()
    conn.close()
    return row is not None and row[0] is not None


def has_cixis_today(user_id: int, date: str) -> bool:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT cixis_time FROM attendance WHERE user_id = ? AND date = ?', (user_id, date))
    row = cursor.fetchone()
    conn.close()
    return row is not None and row[0] is not None


def get_attendance_report(code: str, start_date: str, end_date: str) -> List[dict]:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        '''
        SELECT u.name, u.fin, u.seriya, a.date, a.giris_time, a.cixis_time, a.giris_loc, a.cixis_loc
        FROM users u
        LEFT JOIN attendance a ON u.id = a.user_id AND a.date BETWEEN ? AND ?
        WHERE u.code = ?
        ORDER BY u.name, a.date
        ''',
        (start_date, end_date, code),
    )
    rows = cursor.fetchall()
    conn.close()
    out: list[dict] = []
    for r in rows:
        out.append(
            {
                'name': r[0],
                'fin': r[1],
                'seriya': r[2],
                'date': r[3],
                'giris_time': r[4],
                'cixis_time': r[5],
                'giris_loc': r[6],
                'cixis_loc': r[7],
            }
        )
    return out


def get_all_attendance_report(start_date: str, end_date: str) -> List[dict]:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        '''
        SELECT u.name, u.fin, u.seriya, u.code, a.date, a.giris_time, a.cixis_time, a.giris_loc, a.cixis_loc
        FROM users u
        LEFT JOIN attendance a ON u.id = a.user_id AND a.date BETWEEN ? AND ?
        ORDER BY u.code, u.name, a.date
        ''',
        (start_date, end_date),
    )
    rows = cursor.fetchall()
    conn.close()
    out: list[dict] = []
    for r in rows:
        out.append(
            {
                'name': r[0],
                'fin': r[1],
                'seriya': r[2],
                'code': r[3],
                'date': r[4],
                'giris_time': r[5],
                'cixis_time': r[6],
                'giris_loc': r[7],
                'cixis_loc': r[8],
            }
        )
    return out


# === Group code toggling ===

def set_group_code_active(profession: str, date: str, code: str, is_active: int) -> bool:
    with _db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        try:
            cursor = conn.cursor()
            cursor.execute('PRAGMA busy_timeout=5000')
            cursor.execute(
                'UPDATE group_codes SET is_active = ? WHERE profession = ? AND date = ? AND code = ?',
                ((bool(is_active) if _USING_POSTGRES else (1 if is_active else 0)), profession, date, code),
            )
            affected = cursor.rowcount
            conn.commit()
            return affected > 0
        finally:
            conn.close()
