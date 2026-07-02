"""
数据抓取模块 —— 从东方财富 API 获取行情数据
"""
import json, time, requests
from database import get_conn

INDEX_MAP = {
    "000001": {"name": "上证指数", "exchange": "1", "secid": "1.000001"},
    "399001": {"name": "深证成指", "exchange": "0", "secid": "0.399001"},
    "399006": {"name": "创业板指", "exchange": "0", "secid": "0.399006"},
    "000688": {"name": "科创50", "exchange": "1", "secid": "1.000688"},
}

def fetch_index_realtime():
    secids = ",".join([v["secid"] for v in INDEX_MAP.values()])
    url = f"https://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&fields=f2,f3,f4,f12,f14&secids={secids}"
    try:
        resp = requests.get(url, timeout=3)
        data = resp.json()
        result = {}
        if data.get("data") and data["data"].get("diff"):
            for item in data["data"]["diff"]:
                result[item["f12"]] = {
                    "code": item["f12"], "name": item.get("f14", ""),
                    "price": item.get("f2"), "change_pct": item.get("f3"),
                    "change_amt": item.get("f4"),
                }
        return result
    except Exception as e:
        print(f"获取指数实时行情失败: {e}")
        return {}

def fetch_market_breadth():
    url = "https://push2.eastmoney.com/api/qt/stock/get?secid=1.000001&fields=f43,f44,f45,f46,f47,f48,f50,f51,f52,f60,f116,f117,f162,f167,f168,f169,f170,f171,f292"
    try:
        resp = requests.get(url, timeout=3)
        data = resp.json().get("data", {})
        return {
            "up_count": data.get("f170", 0),
            "down_count": data.get("f171", 0),
            "flat_count": data.get("f167", 0),
            "limit_up": data.get("f169", 0) or 0,
            "limit_down": 0,
            "total_amount": data.get("f60", 0) / 1e8 if data.get("f60") else 0,
        }
    except Exception as e:
        print(f"获取市场概况失败: {e}")
        return {}

def fetch_stock_realtime(code, exchange="1"):
    """获取个股实时行情 + PE/PB/市值"""
    secid = f"{exchange}.{code}"
    url = (
        f"https://push2.eastmoney.com/api/qt/stock/get"
        f"?secid={secid}"
        f"&fields=f43,f44,f45,f46,f48,f50,f51,f52,f57,f58,f60,f115,f116,f117,f118,f162,f164"
    )
    try:
        resp = requests.get(url, timeout=3)
        data = resp.json().get("data", {})
        if not data: return {}
        return {
            "price": data.get("f43"),
            "change_pct": data.get("f170"),
            "pe_ttm": data.get("f115"),
            "pb": data.get("f117"),
            "total_mv": data.get("f118"),
            "pe_pct": data.get("f164"),  # PE历史分位（东方财富直接提供）
        }
    except Exception as e:
        print(f"获取 {code} 实时行情失败: {e}")
        return {}

def fetch_index_daily(code, beg="20140101", end=None):
    if end is None: end = time.strftime("%Y%m%d")
    info = INDEX_MAP.get(code)
    if not info: return []
    url = (
        f"https://push2his.eastmoney.com/api/qt/stock/kline/get"
        f"?secid={info['secid']}&klt=101&fqt=0"
        f"&fields1=f1,f2,f3,f4,f5,f6"
        f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
        f"&beg={beg}&end={end}&lmt=5000"
    )
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if not data.get("data") or not data["data"].get("klines"): return []
        rows = []
        for line in data["data"]["klines"]:
            parts = line.split(",")
            rows.append({
                "trade_date": parts[0].replace("-", ""),
                "open": float(parts[1]), "close": float(parts[2]),
                "high": float(parts[3]), "low": float(parts[4]),
                "volume": float(parts[5]), "amount": float(parts[6]),
            })
        return rows
    except Exception as e:
        print(f"获取 {code} 日线失败: {e}")
        return []

def fetch_stock_daily(code, exchange="1", beg="20140101", end=None):
    """获取个股历史日线（仅OHLCV，PE/PB从实时接口获取）"""
    if end is None: end = time.strftime("%Y%m%d")
    secid = f"{exchange}.{code}"
    url = (
        f"https://push2his.eastmoney.com/api/qt/stock/kline/get"
        f"?secid={secid}&klt=101&fqt=1"
        f"&fields1=f1,f2,f3,f4,f5,f6"
        f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
        f"&beg={beg}&end={end}&lmt=5000"
    )
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if not data.get("data") or not data["data"].get("klines"): return []
        rows = []
        for line in data["data"]["klines"]:
            parts = line.split(",")
            rows.append({
                "trade_date": parts[0].replace("-", ""),
                "open": float(parts[1]), "close": float(parts[2]),
                "high": float(parts[3]), "low": float(parts[4]),
                "volume": float(parts[5]), "amount": float(parts[6]),
            })
        return rows
    except Exception as e:
        print(f"获取 {code} 个股日线失败: {e}")
        return []

def sync_index_history(conn, code):
    existing = set(r[0] for r in conn.execute("SELECT trade_date FROM index_daily WHERE code=?", (code,)))
    rows = fetch_index_daily(code)
    new_count = 0
    for row in rows:
        if row["trade_date"] not in existing:
            conn.execute(
                "INSERT OR REPLACE INTO index_daily VALUES (?,?,?,?,?,?,?,?)",
                (code, row["trade_date"], row["open"], row["high"], row["low"],
                 row["close"], row["volume"], row["amount"]))
            new_count += 1
    conn.commit()
    print(f"  {code}: 新增 {new_count} 条")
    return new_count

def sync_stock_history(conn, code, exchange):
    existing = set(r[0] for r in conn.execute("SELECT trade_date FROM stock_daily WHERE code=?", (code,)))
    rows = fetch_stock_daily(code, exchange)
    new_count = 0
    for row in rows:
        if row["trade_date"] not in existing:
            conn.execute(
                "INSERT OR REPLACE INTO stock_daily VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (code, row["trade_date"], row["open"], row["high"], row["low"],
                 row["close"], row["volume"], row["amount"], None, None, None))
            new_count += 1
    conn.commit()
    print(f"  {code}: 新增 {new_count} 条")
    return new_count

def sync_all_history():
    conn = get_conn()
    print("开始同步指数历史数据...")
    for code in INDEX_MAP:
        sync_index_history(conn, code)
    print("开始同步自选股历史数据...")
    stocks = conn.execute("SELECT code, exchange FROM watchlist").fetchall()
    for s in stocks:
        sync_stock_history(conn, s["code"], s["exchange"])
    conn.close()
    print("同步完成")
