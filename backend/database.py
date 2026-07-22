"""
数据库初始化 + 查询模块
"""
import math
import os
import sqlite3

DB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
DB_PATH = os.path.join(DB_DIR, "market.db")
DEFAULT_WECHAT_RECIPIENT_NAME = "默认微信用户"
DEFAULT_WECHAT_USERID = "oQEbY277xNWNfWEMnTDuIg9e_ayQ"


def get_conn():
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA cache_size=-8000")
    return conn


def init_db():
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS index_daily (
            code TEXT NOT NULL, trade_date TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL, volume REAL, amount REAL,
            PRIMARY KEY (code, trade_date)
        );
        CREATE TABLE IF NOT EXISTS stock_daily (
            code TEXT NOT NULL, trade_date TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL, volume REAL, amount REAL,
            pe_ttm REAL, pb REAL, total_mv REAL,
            PRIMARY KEY (code, trade_date)
        );
        CREATE TABLE IF NOT EXISTS watchlist (
            code TEXT PRIMARY KEY, name TEXT NOT NULL, exchange TEXT NOT NULL,
            add_time TEXT DEFAULT (datetime('now','localtime')), sort_order INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS market_amount_snapshot (
            trade_date TEXT NOT NULL, trade_minute TEXT NOT NULL,
            market_type TEXT NOT NULL, amount REAL NOT NULL,
            data_time TEXT NOT NULL,
            PRIMARY KEY (trade_date, trade_minute, market_type)
        );
        CREATE TABLE IF NOT EXISTS wechat_recipient (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            userid TEXT NOT NULL,
            is_default INTEGER NOT NULL DEFAULT 0,
            is_system INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS wechat_alert (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            target_price REAL NOT NULL,
            recipient_id INTEGER,
            userid TEXT,
            status TEXT NOT NULL DEFAULT '监测中',
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            triggered_at TEXT,
            last_error TEXT,
            last_checked_price REAL
        );
        CREATE INDEX IF NOT EXISTS idx_id_td ON index_daily(trade_date);
        CREATE INDEX IF NOT EXISTS idx_sd_td ON stock_daily(trade_date);
        CREATE INDEX IF NOT EXISTS idx_sd_code ON stock_daily(code);
        CREATE INDEX IF NOT EXISTS idx_alert_status_code ON wechat_alert(status, code);
    """)
    _migrate_wechat_alert_schema(conn)
    _ensure_default_wechat_recipient(conn)
    if _has_column(conn, "wechat_alert", "recipient_id"):
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_recipient ON wechat_alert(recipient_id)")
    conn.commit()
    conn.close()


def _has_column(conn, table, column):
    return any(row["name"] == column for row in conn.execute(f"PRAGMA table_info({table})"))


def _migrate_wechat_alert_schema(conn):
    if not _has_column(conn, "wechat_alert", "recipient_id"):
        conn.execute("ALTER TABLE wechat_alert ADD COLUMN recipient_id INTEGER")


def _ensure_default_wechat_recipient(conn):
    row = conn.execute("SELECT id FROM wechat_recipient WHERE is_system=1 LIMIT 1").fetchone()
    if row:
        default_id = row["id"]
        conn.execute(
            """
            UPDATE wechat_recipient
            SET name=?, userid=?, updated_at=datetime('now','localtime')
            WHERE id=?
            """,
            (DEFAULT_WECHAT_RECIPIENT_NAME, DEFAULT_WECHAT_USERID, default_id),
        )
    else:
        cursor = conn.execute(
            """
            INSERT INTO wechat_recipient (name, userid, is_default, is_system)
            VALUES (?, ?, 1, 1)
            """,
            (DEFAULT_WECHAT_RECIPIENT_NAME, DEFAULT_WECHAT_USERID),
        )
        default_id = cursor.lastrowid
    has_default = conn.execute("SELECT id FROM wechat_recipient WHERE is_default=1 LIMIT 1").fetchone()
    if not has_default:
        conn.execute("UPDATE wechat_recipient SET is_default=1 WHERE id=?", (default_id,))
    conn.execute("UPDATE wechat_alert SET recipient_id=? WHERE recipient_id IS NULL AND (userid IS NULL OR userid='')", (default_id,))
    legacy_rows = conn.execute(
        "SELECT DISTINCT userid FROM wechat_alert WHERE recipient_id IS NULL AND userid IS NOT NULL AND userid<>''"
    ).fetchall()
    for row in legacy_rows:
        userid = row["userid"]
        recipient = conn.execute("SELECT id FROM wechat_recipient WHERE userid=? LIMIT 1", (userid,)).fetchone()
        if recipient:
            recipient_id = recipient["id"]
        else:
            cursor = conn.execute(
                """
                INSERT INTO wechat_recipient (name, userid, is_default, is_system)
                VALUES (?, ?, 0, 0)
                """,
                ("历史接收人", userid),
            )
            recipient_id = cursor.lastrowid
        conn.execute("UPDATE wechat_alert SET recipient_id=? WHERE recipient_id IS NULL AND userid=?", (recipient_id, userid))


def get_index_latest(conn, code):
    row = conn.execute(
        "SELECT * FROM index_daily WHERE code=? ORDER BY trade_date DESC LIMIT 1", (code,)
    ).fetchone()
    return dict(row) if row else None


def get_stock_latest(conn, code):
    row = conn.execute(
        "SELECT * FROM stock_daily WHERE code=? ORDER BY trade_date DESC LIMIT 1", (code,)
    ).fetchone()
    return dict(row) if row else None


MA_WINDOW = 2500


def _index_ma_sql(where_clause="", min_trade_date=None):
    date_filter = "ma2500 IS NOT NULL"
    params = []
    if min_trade_date:
        date_filter += " AND trade_date >= ?"
        params.append(min_trade_date)
    sql = f"""
        WITH source AS (
            SELECT trade_date, close
            FROM index_daily
            WHERE code=? AND close IS NOT NULL
        ),
        stats AS (SELECT COUNT(*) AS total_count FROM source),
        calc AS (
            SELECT trade_date, close,
                   COUNT(close) OVER (ORDER BY trade_date ROWS BETWEEN {MA_WINDOW - 1} PRECEDING AND CURRENT ROW) AS sample_count,
                   AVG(close) OVER (ORDER BY trade_date ROWS BETWEEN {MA_WINDOW - 1} PRECEDING AND CURRENT ROW) AS rolling_ma
            FROM source
        )
        SELECT * FROM (
            SELECT calc.trade_date,
                   calc.close,
                   calc.sample_count,
                   stats.total_count,
                   CASE WHEN calc.sample_count = {MA_WINDOW} THEN calc.rolling_ma END AS ma2500
            FROM calc CROSS JOIN stats
        )
        WHERE {date_filter}
        {where_clause}
    """
    return sql, params


def get_ma2500(conn, code):
    sql, extra_params = _index_ma_sql("ORDER BY trade_date DESC LIMIT 1")
    row = conn.execute(sql, (code, *extra_params)).fetchone()
    return dict(row) if row and row["ma2500"] else None


def get_ma2500_history(conn, code):
    sql, extra_params = _index_ma_sql("ORDER BY trade_date")
    rows = conn.execute(sql, (code, *extra_params)).fetchall()
    return [dict(row) for row in rows]


def get_ma2500_history_since(conn, code, min_trade_date):
    sql, extra_params = _index_ma_sql("ORDER BY trade_date", min_trade_date)
    rows = conn.execute(sql, (code, *extra_params)).fetchall()
    return [dict(row) for row in rows]


def get_ma2500_deviation_history(conn, code):
    sql, extra_params = _index_ma_sql("")
    rows = conn.execute(sql, (code, *extra_params)).fetchall()
    return [dict(row) for row in rows]


def get_index_sample_count(conn, code):
    row = conn.execute(
        "SELECT COUNT(close) AS n FROM index_daily WHERE code=? AND close IS NOT NULL", (code,)
    ).fetchone()
    return int(row["n"] or 0)


def get_index_trend_history(conn, code, min_trade_date, trend_window=20):
    rows = conn.execute(
        """
        SELECT * FROM (
            SELECT trade_date, open, high, low, close,
                   COUNT(close) OVER (ORDER BY trade_date ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) AS count250,
                   AVG(close) OVER (ORDER BY trade_date ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) AS ma250,
                   COUNT(close) OVER (ORDER BY trade_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS count120,
                   AVG(close) OVER (ORDER BY trade_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS ma120
            FROM index_daily WHERE code=? AND close IS NOT NULL
        ) ORDER BY trade_date
        """,
        (code,),
    ).fetchall()
    calculated = []
    slopes = []
    for index, row in enumerate(rows):
        ma250 = row["ma250"] if row["count250"] == 250 else None
        ma120 = row["ma120"] if row["count120"] == 120 else None
        slope = None
        if ma250 is not None and index >= trend_window:
            previous = rows[index - trend_window]
            previous_ma = previous["ma250"] if previous["count250"] == 250 else None
            if previous_ma:
                slope = (ma250 / previous_ma - 1) * 100
        slopes.append(slope)
        accel = None
        if slope is not None and index >= trend_window and slopes[index - trend_window] is not None:
            accel = slope - slopes[index - trend_window]
        calculated.append({
            "trade_date": row["trade_date"],
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "ma250": ma250,
            "ma120": ma120,
            "slope": slope,
            "accel": accel,
        })
    return [row for row in calculated if row["trade_date"] >= min_trade_date]


def get_index_volume(conn):
    """最新一日两市成交额：上证指数 + 深证成指 amount，单位亿。"""
    date_row = conn.execute("SELECT MAX(trade_date) FROM index_daily WHERE code='000001'").fetchone()
    if not date_row or not date_row[0]:
        return None
    trade_date = date_row[0]
    row = conn.execute(
        "SELECT SUM(amount) FROM index_daily WHERE trade_date=? AND code IN ('000001','399001')",
        (trade_date,),
    ).fetchone()
    return round(row[0] / 1e8, 0) if row and row[0] else None


def get_price_percentile(conn, code, current_price=None):
    if current_price is None:
        current = conn.execute(
            "SELECT close FROM stock_daily WHERE code=? AND close IS NOT NULL ORDER BY trade_date DESC LIMIT 1",
            (code,),
        ).fetchone()
        if not current:
            return None
        current_price = current["close"]
    row = conn.execute(
        """
        SELECT (COUNT(*) * 1.0) / (SELECT COUNT(*) FROM stock_daily WHERE code=? AND close IS NOT NULL) AS pct
        FROM stock_daily WHERE code=? AND close IS NOT NULL AND close <= ?
        """,
        (code, code, current_price),
    ).fetchone()
    return round(row["pct"] * 100, 1) if row and row["pct"] is not None else None


def get_index_value_at_percentile(conn, index_code, percentile):
    if percentile is None:
        return None
    count_row = conn.execute(
        "SELECT COUNT(*) AS n FROM index_daily WHERE code=? AND close IS NOT NULL", (index_code,)
    ).fetchone()
    count = count_row["n"] if count_row else 0
    if count <= 0:
        return None
    rank = max(1, min(count, int(math.ceil(count * percentile / 100.0))))
    row = conn.execute(
        """
        SELECT trade_date, close FROM index_daily
        WHERE code=? AND close IS NOT NULL
        ORDER BY close
        LIMIT 1 OFFSET ?
        """,
        (index_code, rank - 1),
    ).fetchone()
    if not row:
        return None
    return {"value": round(row["close"], 2), "trade_date": row["trade_date"], "rank": rank, "count": count}


def get_price_mapped_sh(conn, code, current_price=None, current_index=None):
    if current_price is None:
        current = get_stock_latest(conn, code)
        current_price = current["close"] if current else None
    if current_index is None:
        current = get_index_latest(conn, "000001")
        current_index = current["close"] if current else None
    if current_price is None or current_index is None:
        return None
    rows = conn.execute(
        """
        SELECT sd.trade_date, sd.close AS stock_close, id.close AS index_close
        FROM stock_daily sd
        JOIN index_daily id ON id.code='000001' AND id.trade_date=sd.trade_date
        WHERE sd.code=? AND sd.close IS NOT NULL AND id.close IS NOT NULL
        ORDER BY sd.trade_date
        """,
        (code,),
    ).fetchall()
    sample_count = len(rows)
    if sample_count < 250:
        return {"sample_count": sample_count, "required_count": 250}
    stock_rank = sum(1 for row in rows if row["stock_close"] <= current_price)
    stock_percentile = stock_rank / sample_count * 100
    index_sorted = sorted(rows, key=lambda row: row["index_close"])
    mapped_rank = max(1, min(sample_count, int(math.ceil(sample_count * stock_percentile / 100))))
    mapped_row = index_sorted[mapped_rank - 1]
    index_rank = sum(1 for row in rows if row["index_close"] <= current_index)
    index_percentile = index_rank / sample_count * 100
    position_gap = index_percentile - stock_percentile
    return {
        "avg": round(mapped_row["index_close"], 2),
        "mapped_sh_index": round(mapped_row["index_close"], 2),
        "mapped_date": mapped_row["trade_date"],
        "stock_price_percentile": round(stock_percentile, 1),
        "index_percentile": round(index_percentile, 1),
        "position_gap": round(position_gap, 1),
        "sample_count": sample_count,
        "required_count": 250,
    }


def save_market_amount_snapshots(conn, trade_date, trade_minute, amounts, data_time):
    for market_type, amount in amounts.items():
        conn.execute(
            "INSERT OR REPLACE INTO market_amount_snapshot VALUES (?,?,?,?,?)",
            (trade_date, trade_minute, market_type, amount, data_time),
        )
    conn.execute(
        "DELETE FROM market_amount_snapshot WHERE trade_date < strftime('%Y%m%d','now','-45 days')"
    )
    conn.commit()


def get_market_amount_baseline(conn, market_type, trade_minute, before_date, limit=20):
    rows = conn.execute(
        """
        SELECT amount FROM market_amount_snapshot
        WHERE market_type=? AND trade_minute=? AND trade_date<?
        ORDER BY trade_date DESC LIMIT ?
        """,
        (market_type, trade_minute, before_date, limit),
    ).fetchall()
    values = sorted(row["amount"] for row in rows if row["amount"] is not None)
    if len(values) < 10:
        return None, len(values)
    middle = len(values) // 2
    median = values[middle] if len(values) % 2 else (values[middle - 1] + values[middle]) / 2
    return median, len(values)


def get_watchlist(conn):
    return [dict(row) for row in conn.execute("SELECT * FROM watchlist ORDER BY sort_order, add_time")]


def add_watchlist(conn, code, name, exchange):
    conn.execute(
        "INSERT OR REPLACE INTO watchlist VALUES (?,?,?,datetime('now','localtime'),0)",
        (code, name, exchange),
    )
    conn.commit()


def remove_watchlist(conn, code):
    conn.execute("DELETE FROM watchlist WHERE code=?", (code,))
    conn.commit()


def get_watchlist_item(conn, code):
    row = conn.execute("SELECT * FROM watchlist WHERE code=?", (code,)).fetchone()
    return dict(row) if row else None


def list_wechat_recipients(conn):
    return [dict(row) for row in conn.execute(
        """
        SELECT * FROM wechat_recipient
        ORDER BY is_default DESC, is_system DESC, created_at, id
        """
    )]


def get_wechat_recipient(conn, recipient_id):
    row = conn.execute("SELECT * FROM wechat_recipient WHERE id=?", (recipient_id,)).fetchone()
    return dict(row) if row else None


def add_wechat_recipient(conn, name, userid, is_default=False):
    if is_default:
        conn.execute("UPDATE wechat_recipient SET is_default=0")
    cursor = conn.execute(
        """
        INSERT INTO wechat_recipient (name, userid, is_default, is_system)
        VALUES (?, ?, ?, 0)
        """,
        (name.strip(), userid.strip(), 1 if is_default else 0),
    )
    conn.commit()
    return cursor.lastrowid


def set_default_wechat_recipient(conn, recipient_id):
    recipient = get_wechat_recipient(conn, recipient_id)
    if not recipient:
        return False
    conn.execute("UPDATE wechat_recipient SET is_default=0")
    conn.execute(
        "UPDATE wechat_recipient SET is_default=1, updated_at=datetime('now','localtime') WHERE id=?",
        (recipient_id,),
    )
    conn.commit()
    return True


def count_active_alerts_by_recipient(conn, recipient_id):
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM wechat_alert WHERE recipient_id=? AND status='监测中'",
        (recipient_id,),
    ).fetchone()
    return int(row["n"] or 0)


def delete_wechat_recipient(conn, recipient_id):
    conn.execute("DELETE FROM wechat_recipient WHERE id=? AND is_system=0", (recipient_id,))
    conn.commit()


def list_wechat_alerts(conn, active_only=False):
    sql = """
        SELECT a.*, r.name AS recipient_name, r.userid AS recipient_userid,
               r.is_default AS recipient_is_default, r.is_system AS recipient_is_system
        FROM wechat_alert a
        LEFT JOIN wechat_recipient r ON r.id=a.recipient_id
    """
    params = ()
    if active_only:
        sql += " WHERE a.status='监测中'"
    sql += " ORDER BY CASE a.status WHEN '监测中' THEN 0 WHEN '已停用' THEN 1 ELSE 2 END, a.created_at DESC, a.id DESC"
    return [dict(row) for row in conn.execute(sql, params)]


def get_wechat_alert(conn, alert_id):
    row = conn.execute(
        """
        SELECT a.*, r.name AS recipient_name, r.userid AS recipient_userid,
               r.is_default AS recipient_is_default, r.is_system AS recipient_is_system
        FROM wechat_alert a
        LEFT JOIN wechat_recipient r ON r.id=a.recipient_id
        WHERE a.id=?
        """,
        (alert_id,),
    ).fetchone()
    return dict(row) if row else None


def add_wechat_alert(conn, code, name, target_price, recipient_id):
    cursor = conn.execute(
        """
        INSERT INTO wechat_alert (code, name, target_price, recipient_id)
        VALUES (?, ?, ?, ?)
        """,
        (code, name, float(target_price), recipient_id),
    )
    conn.commit()
    return cursor.lastrowid


def set_wechat_alert_status(conn, alert_id, status):
    conn.execute(
        """
        UPDATE wechat_alert
        SET status=?, updated_at=datetime('now','localtime'), last_error=NULL
        WHERE id=? AND status!='已触发并停用'
        """,
        (status, alert_id),
    )
    conn.commit()


def delete_wechat_alert(conn, alert_id):
    conn.execute("DELETE FROM wechat_alert WHERE id=?", (alert_id,))
    conn.commit()


def mark_wechat_alert_triggered(conn, alert_id, current_price):
    conn.execute(
        """
        UPDATE wechat_alert
        SET status='已触发并停用',
            triggered_at=datetime('now','localtime'),
            updated_at=datetime('now','localtime'),
            last_error=NULL,
            last_checked_price=?
        WHERE id=?
        """,
        (current_price, alert_id),
    )
    conn.commit()


def record_wechat_alert_failure(conn, alert_id, current_price, error):
    conn.execute(
        """
        UPDATE wechat_alert
        SET updated_at=datetime('now','localtime'),
            last_error=?,
            last_checked_price=?
        WHERE id=? AND status='监测中'
        """,
        (str(error)[:300], current_price, alert_id),
    )
    conn.commit()


def classification(pct):
    if pct is None:
        return "fair", "数据不足"
    if pct < 10:
        return "low", "极度低估"
    if pct < 30:
        return "low", "低估"
    if pct < 70:
        return "fair", "合理"
    if pct < 90:
        return "high", "高估"
    return "high", "极度高估"
