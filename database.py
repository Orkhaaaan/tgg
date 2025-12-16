"""
Database management for Worker Attendance Bot
"""

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
from threading import Lock


_DATABASE_URL = os.getenv('DATABASE_URL', '').strip()
_USING_POSTGRES = _DATABASE_URL.lower().startswith('postgres://') or _DATABASE_URL.lower().startswith('postgresql://')

if _USING_POSTGRES:
    import psycopg2
    import psycopg2.extras


def _qmark_to_percent_s(query: str) -> str:
    # Replace qmark placeholders with psycopg2 %s placeholders, but avoid touching
    # question marks inside SQL string literals (e.g. COALESCE(col, '?')).
    out: list[str] = []
    in_single = False
    in_double = False
    i = 0
    while i < len(query):
        ch = query[i]
        if ch == "'" and not in_double:
            # Handle escaped single quote inside single-quoted strings: ''
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
    def __init__(self, conn):
        self._conn = conn
        self.row_factory = None

    def cursor(self):
        if self.row_factory is not None:
            cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            cur = self._conn.cursor()
        return _PgCompatCursor(cur)

    def commit(self):
        return self._conn.commit()

    def close(self):
        return self._conn.close()


if _USING_POSTGRES:
    _sqlite_connect_original = sqlite3.connect

    def _pg_connect(_ignored_db_file=None, timeout=None, **_kwargs):
        try:
            conn = psycopg2.connect(_DATABASE_URL, sslmode='require')
        except TypeError:
            conn = psycopg2.connect(_DATABASE_URL)
        return _PgCompatConnection(conn)

    sqlite3.connect = _pg_connect  # type: ignore[assignment]
    sqlite3.IntegrityError = psycopg2.IntegrityError  # type: ignore[attr-defined]
    sqlite3.OperationalError = psycopg2.OperationalError  # type: ignore[attr-defined]
    sqlite3.Row = object()  # type: ignore[attr-defined]


DB_FILE = 'attendance.db'
_db_lock = Lock()


def init_db():
    """Initialize database with required tables"""
    if _USING_POSTGRES:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        cursor.execute(
            '''
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

        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_code ON users(code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_attendance_user_date ON attendance(user_id, date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_attendance_date ON attendance(date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_codes_expires ON codes(expires_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_codes_code ON codes(code)')

        conn.commit()
        conn.close()
        return

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Enable WAL mode for better concurrency
    cursor.execute('PRAGMA journal_mode=WAL')
    cursor.execute('PRAGMA synchronous=NORMAL')
    cursor.execute('PRAGMA cache_size=10000')
    cursor.execute('PRAGMA temp_store=MEMORY')

    # Users table
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

    # Add phone_number column if it doesn't exist (for existing databases)
    cursor.execute("PRAGMA table_info(users)")
    columns = [column[1] for column in cursor.fetchall()]
    if 'phone_number' not in columns:
        try:
            cursor.execute('ALTER TABLE users ADD COLUMN phone_number TEXT')
        except sqlite3.OperationalError:
            pass

    # Add is_active column if it doesn't exist (for existing databases)
    if 'is_active' not in columns:
        try:
            cursor.execute('ALTER TABLE users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1')
        except sqlite3.OperationalError:
            pass

    # Attendance table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS attendance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            giris_time TEXT,
            cixis_time TEXT,
            giris_loc TEXT,
            cixis_loc TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id),
            UNIQUE(user_id, date)
        )
    ''')

    # Access codes table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP NOT NULL
        )
    ''')

    # Create indexes for faster queries
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_code ON users(code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_attendance_user_date ON attendance(user_id, date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_attendance_date ON attendance(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_codes_expires ON codes(expires_at)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_codes_code ON codes(code)')

    conn.commit()
    conn.close()


# Code management functions
def add_code(code: str, days_valid: int = 30) -> bool:
    """Add a new access code"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        expires_at = datetime.now() + timedelta(days=days_valid)
        cursor.execute(
            'INSERT INTO codes (code, expires_at) VALUES (?, ?)',
            (code, expires_at)
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        return False


def remove_code(code: str) -> bool:
    """Remove an access code"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM codes WHERE code = ?', (code,))
    affected = cursor.rowcount
    conn.commit()
    conn.close()
    return affected > 0


def is_code_valid(code: str) -> bool:
    """Check if code exists and is not expired"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        'SELECT COUNT(*) FROM codes WHERE code = ? AND expires_at > ?',
        (code, datetime.now())
    )
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0


def get_all_codes() -> List[Tuple]:
    """Get all active codes"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        'SELECT code, created_at, expires_at FROM codes WHERE expires_at > ? ORDER BY created_at DESC',
        (datetime.now(),)
    )
    codes = cursor.fetchall()
    conn.close()
    return codes


# User management functions
def register_user(telegram_id: int, name: str, fin: str, seriya: str, code: str) -> bool:
    """Register a new user"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO users (telegram_id, name, fin, seriya, code) VALUES (?, ?, ?, ?, ?)',
            (telegram_id, name, fin, seriya, code)
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        return False


def get_user_by_telegram_id(telegram_id: int) -> Optional[dict]:
    """Get user by telegram ID"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        'SELECT id, telegram_id, name, fin, seriya, code, phone_number, is_active FROM users WHERE telegram_id = ?',
        (telegram_id,)
    )
    row = cursor.fetchone()
    conn.close()

    if row:
        return {
            'id': row[0],
            'telegram_id': row[1],
            'name': row[2],
            'fin': row[3],
            'seriya': row[4],
            'code': row[5],
            'phone_number': row[6] if len(row) > 6 else None,
            'is_active': row[7] if len(row) > 7 else 1
        }
    return None


def upsert_user_profile(telegram_id: int, name: str, fin: str, code: str, seriya: str = "", phone_number: str = "") -> None:
    """Insert or update user profile by telegram_id. 'seriya' and 'phone_number' are optional."""
    with _db_lock:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        # Try insert first
        try:
            cursor.execute(
                'INSERT INTO users (telegram_id, name, fin, seriya, code, phone_number) VALUES (?, ?, ?, ?, ?, ?)',
                (telegram_id, name, fin, seriya, code, phone_number)
            )
        except sqlite3.IntegrityError:
            # Update existing
            cursor.execute(
                'UPDATE users SET name = ?, fin = ?, seriya = ?, code = ?, phone_number = ? WHERE telegram_id = ?',
                (name, fin, seriya, code, phone_number, telegram_id)
            )
        conn.commit()
        conn.close()


def get_all_users() -> List[dict]:
    """Get all users"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT telegram_id, name FROM users')
    users = [{'telegram_id': row[0], 'name': row[1]} for row in cursor.fetchall()]
    conn.close()
    return users


# Attendance functions
def record_giris(user_id: int, date: str, time: str, location: Optional[str] = None) -> bool:
    """Record check-in"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    try:
        cursor.execute(
            'INSERT INTO attendance (user_id, date, giris_time, giris_loc) VALUES (?, ?, ?, ?)',
            (user_id, date, time, location)
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        # Record exists, check if giris already recorded
        cursor.execute(
            'SELECT giris_time FROM attendance WHERE user_id = ? AND date = ?',
            (user_id, date)
        )
        result = cursor.fetchone()
        conn.close()
        return result[0] is None if result else False


def record_cixis(user_id: int, date: str, time: str, location: Optional[str] = None) -> bool:
    """Record check-out"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Try to update existing record
    cursor.execute(
        'UPDATE attendance SET cixis_time = ?, cixis_loc = ? WHERE user_id = ? AND date = ? AND cixis_time IS NULL',
        (time, location, user_id, date)
    )

    if cursor.rowcount == 0:
        # No record or already has cixis
        cursor.execute(
            'SELECT cixis_time FROM attendance WHERE user_id = ? AND date = ?',
            (user_id, date)
        )
        result = cursor.fetchone()
        conn.commit()
        conn.close()
        return False

    conn.commit()
    conn.close()
    return True


def has_giris_today(user_id: int, date: str) -> bool:
    """Check if user has already checked in today"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        'SELECT giris_time FROM attendance WHERE user_id = ? AND date = ?',
        (user_id, date)
    )
    result = cursor.fetchone()
    conn.close()
    return result is not None and result[0] is not None


def has_cixis_today(user_id: int, date: str) -> bool:
    """Check if user has already checked out today"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        'SELECT cixis_time FROM attendance WHERE user_id = ? AND date = ?',
        (user_id, date)
    )
    result = cursor.fetchone()
    conn.close()
    return result is not None and result[0] is not None


def get_attendance_report(code: str, start_date: str, end_date: str) -> List[dict]:
    """Get attendance report for specific code and date range"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT u.name, u.fin, u.seriya, a.date, a.giris_time, a.cixis_time, a.giris_loc, a.cixis_loc
        FROM users u
        LEFT JOIN attendance a ON u.id = a.user_id AND a.date BETWEEN ? AND ?
        WHERE u.code = ?
        ORDER BY u.name, a.date
    ''', (start_date, end_date, code))

    rows = cursor.fetchall()
    conn.close()

    report = []
    for row in rows:
        report.append({
            'name': row[0],
            'fin': row[1],
            'seriya': row[2],
            'date': row[3],
            'giris_time': row[4],
            'cixis_time': row[5],
            'giris_loc': row[6],
            'cixis_loc': row[7]
        })

    return report


def get_all_attendance_report(start_date: str, end_date: str) -> List[dict]:
    """Get attendance report for all users"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT u.name, u.fin, u.seriya, u.code, a.date, a.giris_time, a.cixis_time, a.giris_loc, a.cixis_loc
        FROM users u
        LEFT JOIN attendance a ON u.id = a.user_id AND a.date BETWEEN ? AND ?
        ORDER BY u.code, u.name, a.date
    ''', (start_date, end_date))

    rows = cursor.fetchall()
    conn.close()

    report = []
    for row in rows:
        report.append({
            'name': row[0],
            'fin': row[1],
            'seriya': row[2],
            'code': row[3],
            'date': row[4],
            'giris_time': row[5],
            'cixis_time': row[6],
            'giris_loc': row[7],
            'cixis_loc': row[8]
        })

    return report


def get_all_workers_status(code: Optional[str] = None) -> List[dict]:
    """Get all workers with their latest check-in/out status"""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    if code:
        cursor.execute('''
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
        ''', (code,))
    else:
        cursor.execute('''
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
        ''')

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_attendance_logs(date: Optional[str] = None, profession: Optional[str] = None, code: Optional[str] = None) -> List[dict]:
    """Return entrance/exit logs with locations, optionally filtered by date, profession, code.
    Profession is resolved from registrations table by matching user_id and date.
    """
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    base = (
        'SELECT a.date, u.name, u.fin, u.code, '
        'COALESCE(r.profession, "-") AS profession, '
        'a.giris_time, a.cixis_time, a.giris_loc, a.cixis_loc '
        'FROM attendance a '
        'JOIN users u ON a.user_id = u.id '
        'LEFT JOIN registrations r ON r.user_id = u.id AND r.date = a.date'
    )
    params: list = []
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


# === Registrations (per-day registration log) ===

def init_registrations() -> None:
    """Create table for per-day registrations to prevent duplicates and for admin listing."""
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


def has_registration(user_id: int, date: str, profession: str, code: str) -> bool:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        'SELECT 1 FROM registrations WHERE user_id = ? AND date = ? AND profession = ? AND code = ? LIMIT 1',
        (user_id, date, profession, code)
    )
    row = cursor.fetchone()
    conn.close()
    return row is not None


def add_registration(user_id: int, date: str, profession: str, code: str) -> bool:
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO registrations (user_id, date, profession, code) VALUES (?, ?, ?, ?)',
            (user_id, date, profession, code)
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        return False


def get_registrations(date: Optional[str] = None, profession: Optional[str] = None, code: Optional[str] = None) -> List[dict]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    query = (
        'SELECT r.date, r.profession, r.code, u.name, u.fin '
        'FROM registrations r JOIN users u ON r.user_id = u.id'
    )
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


def get_last_registration_date(user_id: int) -> Optional[str]:
    """Get the date of the last registration for a user. Returns None if no registration exists."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        'SELECT date FROM registrations WHERE user_id = ? ORDER BY date DESC LIMIT 1',
        (user_id,)
    )
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None


# === Group codes (daily profession codes) ===

def init_group_codes() -> None:
    """Create table for daily group codes: profession, date, code, is_active."""
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
                    is_active {active_type} NOT NULL DEFAULT {active_default},
                    UNIQUE(profession, date)
                )
                '''
                .format(
                    id_type='BIGSERIAL' if _USING_POSTGRES else 'INTEGER',
                    date_type='DATE' if _USING_POSTGRES else 'TEXT',
                    active_type='BOOLEAN' if _USING_POSTGRES else 'INTEGER',
                    active_default='TRUE' if _USING_POSTGRES else '1',
                )
            )
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_group_codes_date ON group_codes(date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_group_codes_prof ON group_codes(profession)')
            conn.commit()
        finally:
            conn.close()


def add_group_code(profession: str, date: str, code: str, is_active: int = 1) -> bool:
    """Insert or replace daily code for a profession+date. Returns True on success."""
    with _db_lock:
        try:
            conn = sqlite3.connect(DB_FILE, timeout=10)
            try:
                cursor = conn.cursor()
                cursor.execute('PRAGMA busy_timeout=5000')
                # Try insert; if exists, update
                cursor.execute(
                    'INSERT INTO group_codes (profession, date, code, is_active) VALUES (?, ?, ?, ?)',
                    (profession, date, code, (bool(is_active) if _USING_POSTGRES else (1 if is_active else 0)))
                )
                conn.commit()
                return True
            finally:
                conn.close()
        except sqlite3.IntegrityError:
            # Update existing
            conn = sqlite3.connect(DB_FILE, timeout=10)
            try:
                cursor = conn.cursor()
                cursor.execute('PRAGMA busy_timeout=5000')
                cursor.execute(
                    'UPDATE group_codes SET code = ?, is_active = ? WHERE profession = ? AND date = ?',
                    (code, (bool(is_active) if _USING_POSTGRES else (1 if is_active else 0)), profession, date)
                )
                conn.commit()
                return True
            finally:
                conn.close()


def set_group_code_active(profession: str, date: str, is_active: int) -> bool:
    """Toggle active flag. Returns True if a row was affected."""
    with _db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        try:
            cursor = conn.cursor()
            cursor.execute('PRAGMA busy_timeout=5000')
            cursor.execute(
                'UPDATE group_codes SET is_active = ? WHERE profession = ? AND date = ?',
                ((bool(is_active) if _USING_POSTGRES else (1 if is_active else 0)), profession, date)
            )
            affected = cursor.rowcount
            conn.commit()
            return affected > 0
        finally:
            conn.close()


def delete_group_code(profession: str, date: str) -> bool:
    """Delete group code for a given profession and date. Returns True if a row was deleted."""
    with _db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        try:
            cursor = conn.cursor()
            cursor.execute('PRAGMA busy_timeout=5000')
            cursor.execute(
                'DELETE FROM group_codes WHERE profession = ? AND date = ?',
                (profession, date)
            )
            affected = cursor.rowcount
            conn.commit()
            return affected > 0
        finally:
            conn.close()


def get_group_codes(date: Optional[str] = None, only_active: Optional[bool] = None) -> List[dict]:
    """List group codes optionally filtered by date and active flag."""
    with _db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.cursor()
            cursor.execute('PRAGMA busy_timeout=5000')
            query = 'SELECT profession, date, code, is_active FROM group_codes'
            params: list = []
            conds: list[str] = []
            if date:
                conds.append('date = ?')
                params.append(date)
            if only_active is True:
                conds.append('is_active = {active}'.format(active='TRUE' if _USING_POSTGRES else '1'))
            elif only_active is False:
                conds.append('is_active = {inactive}'.format(inactive='FALSE' if _USING_POSTGRES else '0'))
            if conds:
                query += ' WHERE ' + ' AND '.join(conds)
            query += ' ORDER BY date DESC, profession'
            cursor.execute(query, tuple(params))
            rows = [dict(r) for r in cursor.fetchall()]
            return rows
        finally:
            conn.close()


def get_code_for(profession: str, date: str) -> Optional[str]:
    """Return code for a given profession and date if exists (ignore active flag)."""
    with _db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        try:
            cursor = conn.cursor()
            cursor.execute('PRAGMA busy_timeout=5000')
            cursor.execute(
                'SELECT code FROM group_codes WHERE profession = ? AND date = ?',
                (profession, date)
            )
            row = cursor.fetchone()
            return row[0] if row else None
        finally:
            conn.close()


# === New minimal GPS attendance schema and helpers ===

def init_gps_tables():
    """Initialize additional tables for GPS-based sessions (non-breaking)."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # users2: minimal profile for GPS flow
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
    # sessions: one row per work session
    if _USING_POSTGRES:
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
    """Return users2.id for given telegram_id; create if not exists."""
    with _db_lock:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT id FROM users2 WHERE telegram_id = ?', (telegram_id,))
        row = cursor.fetchone()
        if row:
            conn.close()
            return row[0]
        if _USING_POSTGRES:
            cursor.execute('INSERT INTO users2 (telegram_id, full_name) VALUES (?, ?) RETURNING id', (telegram_id, full_name))
            user_id = cursor.fetchone()[0]
        else:
            cursor.execute('INSERT INTO users2 (telegram_id, full_name) VALUES (?, ?)', (telegram_id, full_name))
            user_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return user_id


def create_session(user_id: int, start_time: str, lat: float, lon: float) -> int:
    """Create a new open session and return its id."""
    with _db_lock:
        conn = sqlite3.connect(DB_FILE)
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
        conn.close()
        return sid


def get_open_session(user_id: int):
    """Get the latest open session (end_time IS NULL) for a user."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        'SELECT * FROM sessions WHERE user_id = ? AND end_time IS NULL ORDER BY id DESC LIMIT 1',
        (user_id,)
    )
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def delete_user_all(telegram_id: int) -> bool:
    """Delete all data for a user identified by telegram_id across legacy and GPS tables.
    Returns True if any row was affected.
    """
    affected = 0
    with _db_lock:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        # Legacy users -> attendance, registrations
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
        # GPS users2 -> sessions
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


def close_session(session_id: int, end_time: str, end_lat: float, end_lon: float, duration_min: int, distance_m: float) -> None:
    """Close a session with checkout data and computed metrics."""
    with _db_lock:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE sessions
            SET end_time = ?, end_lat = ?, end_lon = ?, duration_min = ?, distance_m = ?
            WHERE id = ?
            ''',
            (end_time, end_lat, end_lon, duration_min, distance_m, session_id)
        )
        conn.commit()
        conn.close()


def get_today_sessions(today_iso_date: str):
    """Return today's GPS sessions joined with users2 and users to get registered name.
    today_iso_date format: YYYY-MM-DD"""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    if _USING_POSTGRES:
        cursor.execute(
            '''
            SELECT s.*, 
                   COALESCE(u_reg.name, u2.full_name, '?') as display_name,
                   u2.full_name
            FROM sessions s
            JOIN users2 u2 ON s.user_id = u2.id
            LEFT JOIN users u_reg ON u2.telegram_id = u_reg.telegram_id
            WHERE s.start_time::date = ?::date
            ORDER BY s.id DESC
            ''',
            (today_iso_date,)
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
            (today_iso_date,)
        )
    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_user_session_on_date(user_id: int, iso_date: str):
    """Return the most recent session for a user on a given ISO date (YYYY-MM-DD), if any."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    if _USING_POSTGRES:
        cursor.execute(
            '''
            SELECT *
            FROM sessions
            WHERE user_id = ? AND start_time::date = ?::date
            ORDER BY id DESC LIMIT 1
            ''',
            (user_id, iso_date)
        )
    else:
        cursor.execute(
            '''
            SELECT *
            FROM sessions
            WHERE user_id = ? AND substr(start_time, 1, 10) = ?
            ORDER BY id DESC LIMIT 1
            ''',
            (user_id, iso_date)
        )
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def get_todays_attendance(today: str) -> List[dict]:
    """Get today's attendance for all workers"""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT 
            u.name,
            u.fin,
            u.code,
            a.giris_time,
            a.cixis_time
        FROM attendance a
        JOIN users u ON a.user_id = u.id
        WHERE a.date = ?
        ORDER BY u.code, u.name
    ''', (today,))
    
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_daily_report_for_excel(date: str) -> List[dict]:
    """Get daily report with all users and their attendance for Excel export.
    Includes all users, even if they didn't check in/out.
    Returns list of dicts with: name, fin, code, giris_time, cixis_time, profession, giris_loc, cixis_loc
    Profession is taken from today's registration, or latest registration if today's doesn't exist.
    GPS sessions are used to get location coordinates, then reverse geocoded to addresses.
    """
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Get all users with their attendance for the date, including users without attendance
    # For profession: first try today's registration, if not found, get the latest registration
    # Also get GPS session coordinates for location
    if _USING_POSTGRES:
        cursor.execute('''
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
                    (SELECT profession FROM registrations 
                     WHERE user_id = u.id 
                     ORDER BY date DESC 
                     LIMIT 1),
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
        ''', (date, date, date, date))
    else:
        cursor.execute('''
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
                    (SELECT profession FROM registrations 
                     WHERE user_id = u.id 
                     ORDER BY date DESC 
                     LIMIT 1),
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
        ''', (date, date, date, date))
    
    results = []
    for row in cursor.fetchall():
        row_dict = dict(row)
        # Convert empty strings back to None for easier checking in Python
        if row_dict.get('giris_time') == '':
            row_dict['giris_time'] = None
        if row_dict.get('cixis_time') == '':
            row_dict['cixis_time'] = None
        if row_dict.get('giris_loc') == '':
            row_dict['giris_loc'] = None
        if row_dict.get('cixis_loc') == '':
            row_dict['cixis_loc'] = None
        # Set defaults for missing fields
        if 'seriya' not in row_dict:
            row_dict['seriya'] = None
        if 'phone_number' not in row_dict:
            row_dict['phone_number'] = None
        if 'is_active' not in row_dict:
            row_dict['is_active'] = 1
        results.append(row_dict)
    
    conn.close()
    return results


def get_period_report_for_excel(start_date: str, end_date: str, code: Optional[str] = None) -> List[dict]:
    """Get report for date range, optionally filtered by code. Returns all users with attendance in period."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Get all dates in range
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    results = []
    current_date = start_dt
    while current_date <= end_dt:
        date_str = current_date.isoformat()
        # Get daily report for each date
        daily_data = get_daily_report_for_excel(date_str)
        # Filter by code if provided
        if code:
            daily_data = [r for r in daily_data if r.get('code') == code]
        results.extend(daily_data)
        current_date += timedelta(days=1)
    
    return results


def get_active_students_count(date: Optional[str] = None) -> int:
    """Get count of active students. If date provided, count students active on that date."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    if date:
        # Count students who were active and had registration on that date
        if _USING_POSTGRES:
            cursor.execute('''
                SELECT COUNT(DISTINCT u.id)
                FROM users u
                LEFT JOIN registrations r ON r.user_id = u.id AND r.date = ?::date
                WHERE u.is_active = TRUE AND r.user_id IS NOT NULL
            ''', (date,))
        else:
            cursor.execute('''
                SELECT COUNT(DISTINCT u.id)
                FROM users u
                LEFT JOIN registrations r ON r.user_id = u.id AND r.date = ?
                WHERE u.is_active = 1 AND r.user_id IS NOT NULL
            ''', (date,))
    else:
        # Count all currently active students
        cursor.execute('SELECT COUNT(*) FROM users WHERE is_active = {active}'.format(active='TRUE' if _USING_POSTGRES else '1'))
    
    count = cursor.fetchone()[0]
    conn.close()
    return count or 0


def get_total_registered_students() -> dict:
    """Get statistics about registered students."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Total registered
    cursor.execute('SELECT COUNT(*) FROM users')
    total = cursor.fetchone()[0] or 0
    
    # Active
    cursor.execute('SELECT COUNT(*) FROM users WHERE is_active = {active}'.format(active='TRUE' if _USING_POSTGRES else '1'))
    active = cursor.fetchone()[0] or 0
    
    # Inactive
    cursor.execute('SELECT COUNT(*) FROM users WHERE is_active = {inactive}'.format(inactive='FALSE' if _USING_POSTGRES else '0'))
    inactive = cursor.fetchone()[0] or 0
    
    # By code
    cursor.execute('SELECT code, COUNT(*) as cnt FROM users GROUP BY code')
    by_code = {row[0]: row[1] for row in cursor.fetchall()}
    
    conn.close()
    
    return {
        'total': total,
        'active': active,
        'inactive': inactive,
        'by_code': by_code
    }


# === User activation/deactivation functions ===

def set_user_active(telegram_id: int, is_active: bool) -> bool:
    """Activate or deactivate a user. Returns True if user was found and updated."""
    with _db_lock:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            'UPDATE users SET is_active = ? WHERE telegram_id = ?',
            (is_active if _USING_POSTGRES else (1 if is_active else 0), telegram_id)
        )
    
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        return affected > 0


def deactivate_user_by_code(code: str) -> int:
    """Deactivate all users with a specific code. Returns number of users deactivated."""
    with _db_lock:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            'UPDATE users SET is_active = {inactive} WHERE code = ?'.format(inactive='FALSE' if _USING_POSTGRES else '0'),
            (code,)
        )
    
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        return affected


def get_all_users_with_status(code: Optional[str] = None, only_active: Optional[bool] = None) -> List[dict]:
    """Get all users with their active status, optionally filtered by code and active status."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    query = '''
        SELECT 
            id, 
            telegram_id, 
            name, 
            fin, 
            seriya, 
            code, 
            phone_number, 
            is_active
        FROM users
        WHERE 1=1
    '''
    params = []
    
    if code:
        query += ' AND code = ?'
        params.append(code)
    
    if only_active is True:
        query += ' AND is_active = {active}'.format(active='TRUE' if _USING_POSTGRES else '1')
    elif only_active is False:
        query += ' AND is_active = {inactive}'.format(inactive='FALSE' if _USING_POSTGRES else '0')
    
    query += ' ORDER BY code, name'
    
    cursor.execute(query, tuple(params))
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_users_by_code(code: str, only_active: Optional[bool] = None) -> List[dict]:
    """Get all users with specific code, optionally filtered by active status."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    query = 'SELECT id, telegram_id, name, fin, seriya, code, phone_number, is_active FROM users WHERE code = ?'
    params = [code]
    
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
    """Delete a user by telegram_id. Returns True if user was found and deleted."""
    with _db_lock:
        affected = 0
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        # Get user id first
        cursor.execute('SELECT id FROM users WHERE telegram_id = ?', (telegram_id,))
        row = cursor.fetchone()
        if row:
            uid = int(row[0])
            # Delete related data
            cursor.execute('DELETE FROM attendance WHERE user_id = ?', (uid,))
            affected += cursor.rowcount
            cursor.execute('DELETE FROM registrations WHERE user_id = ?', (uid,))
            affected += cursor.rowcount
            cursor.execute('DELETE FROM users WHERE id = ?', (uid,))
            affected += cursor.rowcount
        
        conn.commit()
        conn.close()
        return affected > 0
