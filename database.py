

import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from logger_setup import logger
from sqlite_support import connect_database, ensure_performance_indexes
from code_identity import normalize_code, normalize_code_domain
from config import Config

# PATCH_MARKER_2026-10-28-outcome-db-http-pacer

_SUCCESS_SUBMISSION_STATUSES = ("SUCCESS", "SUCCESS_POINTS", "SUCCESS_NO_POINTS")
VIETNAM_TIME = timezone(timedelta(hours=7))


def _vietnam_day_utc_bounds(rotation_date: str) -> tuple[str, str]:
    start = datetime.strptime(rotation_date, "%Y-%m-%d").replace(tzinfo=VIETNAM_TIME)
    return tuple(value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                 for value in (start, start + timedelta(days=1)))


class CodeDatabase:

    def __init__(self, db_path: str = "data/code_history.db", stats_batch_size: int = 50):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._used_cache_lock = threading.Lock()
        self._used_cache = None
        self._pending_used: dict[tuple[str, str], int] = {}
        self.stats_batch_size = max(1, int(stats_batch_size))
        self._stats_pending: list[tuple[str, str, str, datetime]] = []

        self.conn = connect_database(db_path, check_same_thread=False, timeout=30.0)
        self.conn.row_factory = sqlite3.Row
        self._optimize_connection()
        self._init_tables()
        self._load_used_cache()

    def _load_used_cache(self):
        """Load the single-process used-code index; SQL remains the fallback."""
        try:
            with self._lock:
                rows = self.conn.execute("SELECT domain, code FROM used_codes").fetchall()
            snapshot = {(normalize_code_domain(row[0]), normalize_code(row[1], row[0]))
                        for row in rows}
            with self._used_cache_lock:
                self._used_cache = snapshot
            logger.info("[Used-Code-Cache] loaded %s entries", len(snapshot))
        except Exception:
            logger.exception("[Used-Code-Cache] load failed; using database checks")

    def is_code_used_cached(self, domain: str, code: str):
        domain = normalize_code_domain(domain)
        key = (domain, normalize_code(code, domain))
        with self._used_cache_lock:
            return None if self._used_cache is None else (key in self._used_cache or key in self._pending_used)

    def unused_codes_cached(self, domain: str, codes):
        domain = normalize_code_domain(domain)
        normalized = {normalize_code(code, domain) for code in (codes or [])
                      if str(code or '').strip()}
        with self._used_cache_lock:
            if self._used_cache is None:
                return None
            return {code for code in normalized if (domain, code) not in self._used_cache
                    and (domain, code) not in self._pending_used}

    def _optimize_connection(self):

        pragmas = [
            "PRAGMA journal_mode=WAL",
            "PRAGMA synchronous=NORMAL",
            "PRAGMA cache_size=-32000",
            "PRAGMA temp_store=MEMORY",
            "PRAGMA mmap_size=268435456",
            "PRAGMA wal_autocheckpoint=256",
            "PRAGMA journal_size_limit=4194304",
            "PRAGMA busy_timeout=30000",
        ]
        for pragma in pragmas:
            try:
                self.conn.execute(pragma)
            except Exception:
                pass
        self.conn.commit()

    def _init_tables(self):

        try:
            with self._lock:
                self.conn.execute("""
                    CREATE TABLE IF NOT EXISTS code_submission (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        code TEXT NOT NULL,
                        account TEXT NOT NULL,
                        website TEXT NOT NULL,
                        status TEXT,
                        result TEXT,
                        submitted_at TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(code, account, website)
                    )
                """)

                table_sql = self.conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='code_submission'"
                ).fetchone()[0]
                if "unique(code,account)" in "".join(table_sql.lower().split()):
                    self.conn.execute("BEGIN IMMEDIATE")
                    try:
                        self.conn.execute("ALTER TABLE code_submission RENAME TO code_submission_old")
                        self.conn.execute("""CREATE TABLE code_submission (
                            id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT NOT NULL,
                            account TEXT NOT NULL, website TEXT NOT NULL, status TEXT,
                            result TEXT, submitted_at TIMESTAMP,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE(code, account, website))""")
                        self.conn.execute("INSERT INTO code_submission SELECT * FROM code_submission_old")
                        self.conn.execute("DROP TABLE code_submission_old")
                        self.conn.commit()
                    except BaseException:
                        self.conn.rollback()
                        raise
                self.conn.execute("""
                    CREATE TABLE IF NOT EXISTS used_codes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        domain TEXT NOT NULL,
                        code TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(domain, code)
                    )
                """)
                self.conn.execute("""
                    CREATE TABLE IF NOT EXISTS submission_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        code TEXT NOT NULL,
                        account TEXT NOT NULL,
                        website TEXT NOT NULL,
                        status TEXT,
                        result TEXT,
                        attempt INTEGER,
                        submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                self.conn.execute("""
                    CREATE TABLE IF NOT EXISTS http_submission_guard (
                        domain TEXT NOT NULL,
                        account TEXT NOT NULL,
                        code TEXT NOT NULL,
                        token TEXT,
                        dispatched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (domain, account, code)
                    )
                """)
                guard_columns = {
                    row[1] for row in self.conn.execute(
                        'PRAGMA table_info("http_submission_guard")'
                    )
                }
                if "token" not in guard_columns:
                    self.conn.execute("ALTER TABLE http_submission_guard ADD COLUMN token TEXT")
                self.conn.execute(
                    "UPDATE http_submission_guard SET token = 'legacy-' || lower(hex(randomblob(16))) "
                    "WHERE token IS NULL"
                )
                self.conn.execute("""
                    CREATE TABLE IF NOT EXISTS account_stats (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        account TEXT NOT NULL UNIQUE,
                        total_submitted INTEGER DEFAULT 0,
                        total_success INTEGER DEFAULT 0,
                        total_failed INTEGER DEFAULT 0,
                        last_submit TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                self.conn.execute("""
                    CREATE TABLE IF NOT EXISTS website_stats (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        website TEXT NOT NULL UNIQUE,
                        total_submitted INTEGER DEFAULT 0,
                        total_success INTEGER DEFAULT 0,
                        total_failed INTEGER DEFAULT 0,
                        last_submit TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                ensure_performance_indexes(self.conn)
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_code ON code_submission(code)")
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_account ON submission_log(account)")
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_website ON submission_log(website)")
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_submitted_at ON submission_log(submitted_at)")
                self.conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_submission_status_time "
                    "ON submission_log(status, submitted_at)"
                )
                self.conn.execute("""
                    CREATE TABLE IF NOT EXISTS account_rotation (
                        domain TEXT PRIMARY KEY,
                        rotation_date TEXT NOT NULL,
                        cursor INTEGER NOT NULL DEFAULT 0,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                self.conn.execute("""
                    CREATE TABLE IF NOT EXISTS account_exhausted (
                        domain TEXT NOT NULL,
                        account TEXT NOT NULL,
                        rotation_date TEXT NOT NULL,
                        marked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (domain, account, rotation_date)
                    )
                """)




                self.conn.execute("""
                    CREATE TABLE IF NOT EXISTS account_success_done (
                        domain TEXT NOT NULL,
                        account TEXT NOT NULL,
                        rotation_date TEXT NOT NULL,
                        marked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (domain, account, rotation_date)
                    )
                """)

                self.conn.execute("DROP INDEX IF EXISTS idx_account_exhausted_day")
                self.conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_account_exhausted_lookup "
                    "ON account_exhausted(domain, rotation_date, account)"
                )
                self.conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_account_success_done_lookup "
                    "ON account_success_done(domain, rotation_date, account)"
                )
                self.conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_account_success_done_day "
                    "ON account_success_done(rotation_date, domain, account)"
                )
                self.conn.commit()
            logger.info("✅ Database tables khởi tạo xong (WAL mode)")
        except Exception as e:
            logger.error(f"❌ Lỗi tạo tables: {e}")
            raise

    def get_account_rotation(self, domain: str, rotation_date: str):
        """Load this site's cursor and successful accounts for one local day.

        Legacy markers may contain URL/domain or username case variations.
        Submission history also recovers successes recorded before the separate
        rotation marker was committed by older versions of the bot.
        """
        domain = normalize_code_domain(domain)
        utc_start, utc_end = _vietnam_day_utc_bounds(rotation_date)
        with self._lock:
            try:
                rotations = self.conn.execute(
                    "SELECT domain, cursor FROM account_rotation "
                    "WHERE rotation_date = ? ORDER BY updated_at DESC, rowid DESC",
                    (rotation_date,),
                ).fetchall()
                cursor = next((int(row[1]) for row in rotations
                               if normalize_code_domain(row[0]) == domain), 0)
                done_accounts = self.conn.execute(
                    "SELECT domain, account FROM account_success_done "
                    "WHERE rotation_date = ?",
                    (rotation_date,),
                ).fetchall()
                successes = self.conn.execute(
                    "SELECT website, account FROM submission_log "
                    "WHERE status IN (?, ?, ?) AND submitted_at >= ? AND submitted_at < ?",
                    (*_SUCCESS_SUBMISSION_STATUSES, utc_start, utc_end),
                ).fetchall()
                done = {
                    str(row[1]).strip().casefold() for row in (*done_accounts, *successes)
                    if normalize_code_domain(row[0]) == domain and str(row[1]).strip()
                }
                return cursor, done
            except Exception as e:
                logger.error("❌ Lỗi đọc account rotation: %s", e)
                # An unavailable DB must not reopen successful accounts.
                raise

    def get_account_exhausted(self, domain: str, rotation_date: str) -> set[str]:
        """Load accounts explicitly reported as limited for the local day."""
        domain = normalize_code_domain(domain)
        utc_start, utc_end = _vietnam_day_utc_bounds(rotation_date)
        with self._lock:
            rows = self.conn.execute(
                "SELECT domain, account FROM account_exhausted WHERE rotation_date = ?",
                (rotation_date,),
            ).fetchall()
            evidence = self.conn.execute(
                "SELECT website, account, status, result FROM submission_log "
                "WHERE status IN ('ACCOUNT_LIMIT', 'ACCOUNT_BLOCKED') "
                "AND submitted_at >= ? AND submitted_at < ?",
                (utc_start, utc_end),
            ).fetchall() if domain == "llwincode.com" else []
        accounts = {str(account).strip().casefold() for stored_domain, account in rows
                    if normalize_code_domain(stored_domain) == domain and str(account or '').strip()}
        if domain == "llwincode.com":
            from submission_outcomes import is_llwin_campaign_limit
            for account in list(accounts):
                matches = [(status, result) for website, user, status, result in evidence
                           if normalize_code_domain(website) == domain
                           and str(user or '').strip().casefold() == account]
                # Do not reopen when evidence is missing, blocked, or account-wide.
                if matches and all(status == "ACCOUNT_LIMIT" and is_llwin_campaign_limit(domain, result)
                                   for status, result in matches):
                    accounts.remove(account)
        return accounts

    def save_account_cursor(self, domain: str, cursor: int, rotation_date: str | None = None):
        domain = normalize_code_domain(domain)
        day = rotation_date or datetime.now(VIETNAM_TIME).strftime("%Y-%m-%d")
        with self._lock:
            try:
                self.conn.execute(
                    "INSERT INTO account_rotation(domain, rotation_date, cursor, updated_at) "
                    "VALUES (?, ?, ?, CURRENT_TIMESTAMP) "
                    "ON CONFLICT(domain) DO UPDATE SET "
                    "rotation_date=excluded.rotation_date, cursor=excluded.cursor, "
                    "updated_at=CURRENT_TIMESTAMP",
                    (domain, day, int(cursor)),
                )
                self.conn.commit()
            except Exception as e:
                logger.error("❌ Lỗi lưu account cursor: %s", e)
                self.conn.rollback()

    def mark_account_success_done(self, domain: str, account: str, rotation_date: str):
        domain = normalize_code_domain(domain)
        account = str(account).strip().casefold()
        with self._lock:
            try:
                self.conn.execute(
                    "INSERT OR IGNORE INTO account_success_done(domain, account, rotation_date) "
                    "VALUES (?, ?, ?)",
                    (domain, account, rotation_date),
                )
                self.conn.commit()
            except Exception as e:
                logger.error("❌ Lỗi lưu account success-done: %s", e)
                self.conn.rollback()

    def mark_account_exhausted(self, domain: str, account: str, rotation_date: str):

        with self._lock:
            try:
                self.conn.execute(
                    "INSERT OR IGNORE INTO account_exhausted(domain, account, rotation_date) "
                    "VALUES (?, ?, ?)",
                    (domain, account, rotation_date),
                )
                self.conn.commit()
            except Exception as e:
                logger.error("❌ Lỗi lưu account exhausted: %s", e)
                self.conn.rollback()

    def _flush_stats_locked(self) -> None:
        if not self._stats_pending:
            return
        account_totals: dict[str, list] = {}
        website_totals: dict[str, list] = {}
        for account, website, status, submitted_at in self._stats_pending:
            success = int(status in _SUCCESS_SUBMISSION_STATUSES)
            failed = int(status == "FAILED")
            for totals, key in ((account_totals, account), (website_totals, website)):
                entry = totals.setdefault(key, [0, 0, 0, submitted_at])
                entry[0] += 1
                entry[1] += success
                entry[2] += failed
                entry[3] = max(entry[3], submitted_at)
        try:
            for account, (total, success, failed, last_submit) in account_totals.items():
                self.conn.execute(
                    """INSERT INTO account_stats
                       (account, total_submitted, total_success, total_failed, last_submit)
                       VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT(account) DO UPDATE SET
                         total_submitted=account_stats.total_submitted + excluded.total_submitted,
                         total_success=account_stats.total_success + excluded.total_success,
                         total_failed=account_stats.total_failed + excluded.total_failed,
                         last_submit=excluded.last_submit""",
                    (account, total, success, failed, last_submit),
                )
            for website, (total, success, failed, last_submit) in website_totals.items():
                self.conn.execute(
                    """INSERT INTO website_stats
                       (website, total_submitted, total_success, total_failed, last_submit)
                       VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT(website) DO UPDATE SET
                         total_submitted=website_stats.total_submitted + excluded.total_submitted,
                         total_success=website_stats.total_success + excluded.total_success,
                         total_failed=website_stats.total_failed + excluded.total_failed,
                         last_submit=excluded.last_submit""",
                    (website, total, success, failed, last_submit),
                )
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            logger.exception("Submission statistics batch transaction failed")
            raise
        self._stats_pending.clear()

    def record_submission(self, code: str, account: str, website: str,
                          status: str, result: str = None, attempt: int = 1):
        code = normalize_code(code, website)
        result = str(result)[:2048] if result is not None else None

        with self._lock:
            try:
                now_utc = datetime.now(timezone.utc)
                now = now_utc.astimezone(VIETNAM_TIME).replace(tzinfo=None)
                submitted_utc = now_utc.strftime("%Y-%m-%d %H:%M:%S")
                self.conn.execute("""
                    INSERT INTO submission_log (code, account, website, status, result, attempt, submitted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (code, account, website, status, result, attempt, submitted_utc))

                self.conn.execute("""
                    INSERT OR REPLACE INTO code_submission
                    (code, account, website, status, result, submitted_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (code, account, website, status, result, submitted_utc))

                if status in _SUCCESS_SUBMISSION_STATUSES:
                    # Persist the exclusion in the same transaction as the
                    # confirmed result, before control returns to the worker.
                    self.conn.execute(
                        "INSERT OR IGNORE INTO account_success_done(domain, account, rotation_date) "
                        "VALUES (?, ?, ?)",
                        (normalize_code_domain(website), str(account).strip().casefold(),
                         now.strftime("%Y-%m-%d")),
                    )
                self.conn.commit()
                self._stats_pending.append((account, website, status, now))
                if len(self._stats_pending) >= self.stats_batch_size:
                    self._flush_stats_locked()
                logger.debug(f"💾 [{account}] Code {code}: {status}")

                return True
            except BaseException:
                try:
                    self.conn.rollback()
                except sqlite3.Error:
                    pass
                logger.exception("Submission transaction failed: account=%s website=%s", account, website)
                raise

    def claim_http_submission(self, domain: str, account: str, code: str) -> str | None:
        """Persist a unique claim token before POST; an existing claim prevents replay."""
        domain = normalize_code_domain(domain)
        token = uuid.uuid4().hex
        with self._lock, self.conn:
            cursor = self.conn.execute(
                "INSERT OR IGNORE INTO http_submission_guard(domain, account, code, token) "
                "VALUES (?, ?, ?, ?)",
                (domain, str(account).strip().casefold(), normalize_code(code, domain), token),
            )
            return token if cursor.rowcount == 1 else None

    def release_http_submission(
        self, domain: str, account: str, code: str, token: str, ok: bool,
    ) -> bool:
        """Retain a successful claim; release only the matching token after a confirmed non-success."""
        if ok:
            return False
        domain = normalize_code_domain(domain)
        with self._lock, self.conn:
            cursor = self.conn.execute(
                "DELETE FROM http_submission_guard "
                "WHERE domain=? AND account=? AND code=? AND token=?",
                (domain, str(account).strip().casefold(), normalize_code(code, domain), token),
            )
            return cursor.rowcount == 1

    def is_code_used(self, domain: str, code: str) -> bool:
        domain = normalize_code_domain(domain)
        with self._lock:
            try:
                row = self.conn.execute(
                    "SELECT 1 FROM used_codes WHERE domain = ? AND code = ?",
                    (domain, normalize_code(code, domain))
                ).fetchone()
                return row is not None
            except Exception as e:

                logger.error(
                    f"❌ Lỗi check used_codes, coi như đã dùng để tránh trùng: {e}"
                )
                return True

    def unused_codes(self, domain: str, codes) -> set[str]:

        domain = normalize_code_domain(domain)
        normalized = list(dict.fromkeys(
            normalize_code(code, domain) for code in (codes or []) if str(code or "").strip()
        ))
        if not normalized:
            return set()
        placeholders = ",".join("?" for _ in normalized)
        with self._lock:
            try:
                rows = self.conn.execute(
                    f"SELECT code FROM used_codes WHERE domain = ? AND code IN ({placeholders})",
                    (domain, *normalized),
                ).fetchall()
                used = {normalize_code(row[0], domain) for row in rows}
                return {code for code in normalized if code not in used}
            except Exception as e:
                logger.error("❌ Lỗi batch check used_codes, bỏ qua toàn bộ batch để tránh submit trùng: %s", e)
                return set()

    def mark_code_used(self, domain: str, code: str) -> bool:
        domain = normalize_code_domain(domain)
        code = normalize_code(code, domain)
        key = (domain, code)

        with self._used_cache_lock:
            self._pending_used[key] = self._pending_used.get(key, 0) + 1
        try:
            with self._lock:
                try:
                    self.conn.execute(
                        "INSERT INTO used_codes (domain, code) VALUES (?, ?)",
                        (domain, code)
                    )
                    self.conn.commit()
                    with self._used_cache_lock:
                        if self._used_cache is not None:
                            self._used_cache.add(key)
                    return True
                except sqlite3.IntegrityError:
                    try:
                        self.conn.rollback()
                    except Exception:
                        pass
                    with self._used_cache_lock:
                        if self._used_cache is not None:
                            self._used_cache.add(key)
                    return False
                except Exception as e:
                    logger.error(f"❌ Lỗi mark_code_used: {e}")
                    try:
                        self.conn.rollback()
                    except Exception:
                        pass
                    return False
        finally:
            with self._used_cache_lock:
                remaining = self._pending_used[key] - 1
                if remaining:
                    self._pending_used[key] = remaining
                else:
                    self._pending_used.pop(key, None)

    def maintain_storage(self, *, compact=False):
        """Bound result history and replay guards; run on the background DB executor.

        Guards survive restarts for the retained window. No message/media payload
        is stored here. RAM ingress never opens a database connection.
        """
        days = Config.DB_RETENTION_DAYS
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        detail_cap = Config.DB_MAX_DETAIL_ROWS
        guard_cap = Config.DB_MAX_GUARD_ROWS
        removed = {}
        with self._lock:
            try:
                self._flush_stats_locked()
                for table, stamp, cap in (
                    ('submission_log', 'submitted_at', detail_cap),
                    ('code_submission', 'submitted_at', detail_cap),
                    ('used_codes', 'created_at', guard_cap),
                    ('http_submission_guard', 'dispatched_at', guard_cap),
                ):
                    count = self.conn.execute(
                        f'DELETE FROM "{table}" WHERE "{stamp}" < ?', (cutoff,)).rowcount
                    threshold = self.conn.execute(
                        f'SELECT rowid FROM "{table}" ORDER BY rowid DESC LIMIT 1 OFFSET ?',
                        (cap - 1,),
                    ).fetchone()
                    if threshold is not None:
                        count += self.conn.execute(
                            f'DELETE FROM "{table}" WHERE rowid < ?', (threshold[0],)).rowcount
                    removed[table] = count
                day = (datetime.now(VIETNAM_TIME) - timedelta(days=days)).strftime('%Y-%m-%d')
                for table in ('account_rotation', 'account_exhausted', 'account_success_done'):
                    self.conn.execute(f'DELETE FROM "{table}" WHERE rotation_date < ?', (day,))
                # Old oversized server responses must not keep bloated pages.
                for table in ('submission_log', 'code_submission'):
                    self.conn.execute(f'UPDATE "{table}" SET result=substr(result,1,2048) WHERE length(result)>2048')
                self.conn.commit()
                rows = self.conn.execute('SELECT domain, code FROM used_codes').fetchall()
                with self._used_cache_lock:
                    self._used_cache = {(normalize_code_domain(r[0]), normalize_code(r[1], r[0])) for r in rows}
                self.conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                pages = self.conn.execute('PRAGMA page_count').fetchone()[0]
                free = self.conn.execute('PRAGMA freelist_count').fetchone()[0]
                if compact or free > max(256, pages // 4):
                    self.conn.execute('VACUUM')
                    self.conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            except BaseException:
                self.conn.rollback()
                raise
        if any(removed.values()):
            logger.info('[DB-Retention] days=%s rows_removed=%s', days, removed)
        return removed

    def close(self):
        with self._lock:
            try:
                self._flush_stats_locked()
                self.conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                self.conn.close()
                logger.info("Database closed")
                return True
            except Exception:
                logger.exception("Failed to close database")
                raise


_db_instance = None

def init_database(db_path: str = "data/code_history.db") -> CodeDatabase:
    global _db_instance
    if _db_instance is None:
        _db_instance = CodeDatabase(db_path)
    elif Path(_db_instance.db_path).expanduser().resolve() != Path(db_path).expanduser().resolve():
        raise RuntimeError(
            "Database singleton đã được khởi tạo với path khác: "
            f"{_db_instance.db_path!r} != {db_path!r}"
        )
    return _db_instance
