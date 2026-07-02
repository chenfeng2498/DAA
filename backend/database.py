"""
数据库初始化 + 查询模块
"""
import sqlite3, os

DB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
DB_PATH = os.path.join(DB_DIR, "market.db")

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
        CREATE INDEX IF NOT EXISTS idx_id_td ON index_daily(trade_date);
        CREATE INDEX IF NOT EXISTS idx_sd_td ON stock_daily(trade_date);
        CREATE INDEX IF NOT EXISTS idx_sd_code ON stock_daily(code);
    """)
    conn.commit(); conn.close()

# ====== 查询 ======

def get_index_latest(conn, code):
    r = conn.execute("SELECT * FROM index_daily WHERE code=? ORDER BY trade_date DESC LIMIT 1", (code,)).fetchone()
    return dict(r) if r else None

def get_stock_latest(conn, code):
    r = conn.execute("SELECT * FROM stock_daily WHERE code=? ORDER BY trade_date DESC LIMIT 1", (code,)).fetchone()
    return dict(r) if r else None

def get_ma2500(conn, code):
    r = conn.execute("""
        SELECT * FROM (
            SELECT trade_date, close,
                   AVG(close) OVER (ORDER BY trade_date ROWS BETWEEN 2499 PRECEDING AND CURRENT ROW) AS ma2500
            FROM index_daily WHERE code=?
        ) ORDER BY trade_date DESC LIMIT 1
    """, (code,)).fetchone()
    return dict(r) if r and r["ma2500"] else None

def get_ma2500_history(conn, code):
    rows = conn.execute("""
        SELECT * FROM (
            SELECT trade_date, close,
                   AVG(close) OVER (ORDER BY trade_date ROWS BETWEEN 2499 PRECEDING AND CURRENT ROW) AS ma2500
            FROM index_daily WHERE code=?
        ) WHERE ma2500 IS NOT NULL ORDER BY trade_date
    """, (code,)).fetchall()
    return [dict(r) for r in rows]

def get_ma2500_deviation_history(conn, code):
    rows = conn.execute("""
        SELECT * FROM (
            SELECT trade_date, close,
                   AVG(close) OVER (ORDER BY trade_date ROWS BETWEEN 2499 PRECEDING AND CURRENT ROW) AS ma2500
            FROM index_daily WHERE code=?
        ) WHERE ma2500 IS NOT NULL
    """, (code,)).fetchall()
    return [dict(r) for r in rows]

def get_index_volume(conn):
    """获取最新一日的两市成交额（上证+深证 amount 之和，亿）"""
    date_row = conn.execute("SELECT MAX(trade_date) FROM index_daily WHERE code='000001'").fetchone()
    if not date_row or not date_row[0]: return None
    td = date_row[0]
    amt = conn.execute(
        "SELECT SUM(amount) FROM index_daily WHERE trade_date=? AND code IN ('000001','399001')", (td,)
    ).fetchone()
    return round(amt[0] / 1e8, 0) if amt and amt[0] else None

def get_price_percentile(conn, code):
    """用股价计算历史分位（PE 不可用时）"""
    cur = conn.execute(
        "SELECT close FROM stock_daily WHERE code=? ORDER BY trade_date DESC LIMIT 1", (code,)
    ).fetchone()
    if not cur: return None
    pct = conn.execute("""
        SELECT (COUNT(*) * 1.0) / (SELECT COUNT(*) FROM stock_daily WHERE code=?) AS pct
        FROM stock_daily WHERE code=? AND close <= ?
    """, (code, code, cur["close"])).fetchone()
    return round(pct["pct"] * 100, 1) if pct else None

def get_price_similar_sh(conn, code, top_n=5):
    """价格相似时点 → 对应上证均值"""
    cur = conn.execute(
        "SELECT close FROM stock_daily WHERE code=? ORDER BY trade_date DESC LIMIT 1", (code,)
    ).fetchone()
    if not cur: return None
    rows = conn.execute("""
        SELECT sd.trade_date, sd.close, id.close as sh_close
        FROM stock_daily sd
        JOIN index_daily id ON id.code='000001' AND id.trade_date=sd.trade_date
        WHERE sd.code=?
        ORDER BY ABS(sd.close - ?)
        LIMIT ?
    """, (code, cur["close"], top_n)).fetchall()
    if not rows: return None
    shs = [r["sh_close"] for r in rows]
    return {"avg": round(sum(shs)/len(shs), 1), "min": round(min(shs), 1),
            "max": round(max(shs), 1), "dates": [r["trade_date"] for r in rows], "count": len(rows)}

def get_watchlist(conn):
    return [dict(r) for r in conn.execute("SELECT * FROM watchlist ORDER BY sort_order, add_time")]

def add_watchlist(conn, code, name, exchange):
    conn.execute("INSERT OR REPLACE INTO watchlist VALUES (?,?,?,datetime('now','localtime'),0)",
                 (code, name, exchange))
    conn.commit()

def remove_watchlist(conn, code):
    conn.execute("DELETE FROM watchlist WHERE code=?", (code,)); conn.commit()

def classification(pct):
    if pct is None: return "fair", "数据不足"
    if pct < 10: return "low", "极度低估"
    if pct < 30: return "low", "低估"
    if pct < 70: return "fair", "合理"
    if pct < 90: return "high", "高估"
    return "high", "极度高估"
