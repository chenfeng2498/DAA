"""
数据抓取模块：东方财富接口封装。
"""
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import ceil
from urllib.parse import quote, urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from database import get_conn

INDEX_MAP = {
    "000001": {"name": "上证指数", "exchange": "1", "secid": "1.000001"},
    "399001": {"name": "深证成指", "exchange": "0", "secid": "0.399001"},
    "399006": {"name": "创业板指", "exchange": "0", "secid": "0.399006"},
    "000688": {"name": "科创50", "exchange": "1", "secid": "1.000688"},
}

_SESSION = requests.Session()
_RETRY = Retry(
    total=3,
    connect=3,
    read=3,
    backoff_factor=0.6,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET",),
    raise_on_status=False,
)
_ADAPTER = HTTPAdapter(max_retries=_RETRY, pool_connections=8, pool_maxsize=16)
_SESSION.mount("http://", _ADAPTER)
_SESSION.mount("https://", _ADAPTER)
_SESSION.headers.update({
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Connection": "close",
    "Referer": "https://quote.eastmoney.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
})

_HOST_FALLBACKS = {
    "push2.eastmoney.com": ("push2delay.eastmoney.com",),
}


def _candidate_urls(url):
    parsed = urlparse(url)
    hosts = (parsed.netloc, *_HOST_FALLBACKS.get(parsed.netloc, ()))
    for host in dict.fromkeys(hosts):
        yield urlunparse(parsed._replace(netloc=host))


def _request_json(url, timeout=8):
    last_error = None
    for candidate_url in _candidate_urls(url):
        for attempt in range(3):
            try:
                if attempt:
                    time.sleep(0.5 * attempt + random.random() * 0.2)
                response = _SESSION.get(candidate_url, timeout=timeout)
                response.raise_for_status()
                response.encoding = "utf-8"
                text = response.text.strip()
                if not text:
                    raise ValueError("empty response")
                return response.json()
            except Exception as exc:
                last_error = exc
    raise last_error


def _safe_number(value, scale=1, allow_negative_one=False):
    if value in (None, "", "-"):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number == -1 and not allow_negative_one:
        return None
    return number / scale


def _normalize_price(value):
    number = _safe_number(value)
    if number is None:
        return None
    # Quote接口常用价格放大100倍，历史K线接口则已是正常价格。
    return round(number / 100, 2) if abs(number) > 10000 else round(number, 2)


def fetch_index_realtime():
    secids = ",".join([v["secid"] for v in INDEX_MAP.values()])
    url = f"https://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&fields=f2,f3,f4,f12,f14&secids={secids}"
    try:
        data = _request_json(url, timeout=5)
        result = {}
        for item in data.get("data", {}).get("diff", []) or []:
            code = item.get("f12")
            if not code:
                continue
            result[code] = {
                "code": code,
                "name": item.get("f14", ""),
                "price": _safe_number(item.get("f2")),
                "change_pct": _safe_number(item.get("f3"), allow_negative_one=True),
                "change_amt": _safe_number(item.get("f4"), allow_negative_one=True),
            }
        return result
    except Exception as exc:
        print(f"获取指数实时行情失败: {exc}")
        return {}


def fetch_market_breadth():
    # f104/f105/f106 是上涨/下跌/平盘家数；f168/f169 常用于量比/涨停等，字段含义会随接口变动。
    url = (
        "https://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2"
        "&fields=f104,f105,f106,f12,f14,f62&secids=1.000001"
    )
    try:
        data = _request_json(url, timeout=5)
        diff = data.get("data", {}).get("diff", []) or []
        item = diff[0] if diff else {}
        up_count = int(_safe_number(item.get("f104")) or 0)
        down_count = int(_safe_number(item.get("f105")) or 0)
        flat_count = int(_safe_number(item.get("f106")) or 0)
        return {
            "up_count": up_count,
            "down_count": down_count,
            "flat_count": flat_count,
            "limit_up": 0,
            "limit_down": 0,
            "total_amount": 0,
        }
    except Exception as exc:
        print(f"获取市场概况失败: {exc}")
        return {}


def fetch_market_distribution():
    def approx_limit_threshold(code, name):
        code = str(code or "")
        name = str(name or "").upper()
        if "ST" in name:
            return 5
        if code.startswith(("300", "301", "688")):
            return 20
        if code.startswith(("4", "8")):
            return 30
        return 10

    def fetch_all(fs, fields, fid):
        page_size = 100
        base = (
            "https://push2.eastmoney.com/api/qt/clist/get?po=1&np=1&fltt=2&invt=2"
            "&ut=bd1d9ddb04089700cf9c27f6f7426281"
            f"&pz={page_size}&fid={fid}&fs={fs}&fields={fields}"
        )
        first_data = _request_json(f"{base}&pn=1", timeout=12).get("data", {}) or {}
        first_items = first_data.get("diff", []) or []
        total = int(first_data.get("total") or len(first_items))
        page_count = max(1, ceil(total / page_size))
        pages = {1: first_items}
        if page_count > 1:
            with ThreadPoolExecutor(max_workers=6) as executor:
                futures = {
                    executor.submit(_request_json, f"{base}&pn={page}", 12): page
                    for page in range(2, page_count + 1)
                }
                for future in as_completed(futures):
                    page = futures[future]
                    page_data = future.result().get("data", {}) or {}
                    pages[page] = page_data.get("diff", []) or []
        items = []
        for page in range(1, page_count + 1):
            items.extend(pages.get(page, []))
        if len(items) < total:
            raise ValueError(f"incomplete market list: {len(items)}/{total}")
        return items[:total]

    try:
        items = fetch_all(
            "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048",
            "f2,f3,f6,f12,f13,f14,f18",
            "f3",
        )
        etf_items = fetch_all("b:MK0021", "f2,f6,f12,f14", "f6")
        labels = ["<-10%", "-10~-7%", "-7~-5%", "-5~-3%", "-3~0%", "0%", "0~3%", "3~5%", "5~7%", "7~10%", ">10%"]
        buckets = [0] * len(labels)
        up_count = down_count = flat_count = limit_up = limit_down = 0
        amounts = {"all_a": 0.0, "sh_a": 0.0, "sz_a": 0.0, "bj_a": 0.0, "etf": 0.0}
        for item in items:
            pct = _safe_number(item.get("f3"), allow_negative_one=True)
            price = _safe_number(item.get("f2"))
            previous = _safe_number(item.get("f18"))
            amount = _safe_number(item.get("f6")) or 0
            code = str(item.get("f12") or "")
            name = str(item.get("f14") or "")
            market = str(item.get("f13") or "")
            if pct is None or price is None or price <= 0 or previous is None or previous <= 0:
                continue
            if pct < -10:
                bucket = 0
            elif pct < -7:
                bucket = 1
            elif pct < -5:
                bucket = 2
            elif pct < -3:
                bucket = 3
            elif pct < 0:
                bucket = 4
            elif pct == 0:
                bucket = 5
            elif pct <= 3:
                bucket = 6
            elif pct <= 5:
                bucket = 7
            elif pct <= 7:
                bucket = 8
            elif pct <= 10:
                bucket = 9
            else:
                bucket = 10
            buckets[bucket] += 1
            if pct > 0:
                up_count += 1
            elif pct < 0:
                down_count += 1
            else:
                flat_count += 1
            limit_threshold = approx_limit_threshold(code, name)
            if pct >= limit_threshold - 0.2:
                limit_up += 1
            if pct <= -limit_threshold + 0.2:
                limit_down += 1
            amounts["all_a"] += amount
            if code.startswith(("8", "4")):
                amounts["bj_a"] += amount
            elif market == "1":
                amounts["sh_a"] += amount
            else:
                amounts["sz_a"] += amount
        for item in etf_items:
            price = _safe_number(item.get("f2"))
            if price is not None and price > 0:
                amounts["etf"] += _safe_number(item.get("f6")) or 0
        valid_count = up_count + down_count + flat_count
        breadth_score = (up_count - down_count) / valid_count * 100 if valid_count else None
        if breadth_score is None:
            mood_label, mood_cls = "等待实时数据", "gray"
        elif breadth_score >= 40:
            mood_label, mood_cls = "极强", "high"
        elif breadth_score >= 15:
            mood_label, mood_cls = "偏强", "high"
        elif breadth_score > -15:
            mood_label, mood_cls = "均衡", "fair"
        elif breadth_score > -40:
            mood_label, mood_cls = "偏弱", "low"
        else:
            mood_label, mood_cls = "极弱", "low"
        return {
            "labels": labels,
            "buckets": buckets,
            "up_count": up_count,
            "down_count": down_count,
            "flat_count": flat_count,
            "up_down_ratio": round(up_count / down_count, 2) if down_count else (None if up_count == 0 else round(up_count, 2)),
            "limit_up": limit_up,
            "limit_down": limit_down,
            "breadth_score": round(breadth_score, 1) if breadth_score is not None else None,
            "mood_label": mood_label,
            "mood_cls": mood_cls,
            "amounts": {key: round(value / 1e8, 1) for key, value in amounts.items()},
        }
    except Exception as exc:
        print(f"获取全市场涨跌分布失败: {exc}")
        return {}


def fetch_stock_realtime(code, exchange="1"):
    secid = f"{exchange}.{code}"
    url = (
        "https://push2.eastmoney.com/api/qt/stock/get"
        f"?secid={secid}"
        "&fields=f43,f57,f58,f115,f116,f117,f118,f162,f167,f168,f169,f170"
    )
    try:
        data = _request_json(url, timeout=5).get("data", {}) or {}
        return {
            "code": code,
            "name": data.get("f58"),
            "price": _safe_number(data.get("f43"), scale=100),
            "change_pct": _safe_number(data.get("f170"), scale=100, allow_negative_one=True),
            "pe_ttm": _safe_number(data.get("f115"), scale=100),
            "pb": _safe_number(data.get("f167"), scale=100) or _safe_number(data.get("f117"), scale=100),
            "total_mv": _safe_number(data.get("f116"), scale=1e8) or _safe_number(data.get("f118"), scale=1e8),
            "pe_pct": _safe_number(data.get("f162"), scale=100),
        }
    except Exception as exc:
        print(f"获取 {code} 实时行情失败: {exc}")
        return {}


def search_stock(keyword, limit=10):
    query = quote(keyword.strip())
    if not query:
        return []
    url = (
        "https://searchapi.eastmoney.com/api/suggest/get"
        f"?input={query}&type=14&token=&count={limit}"
    )
    try:
        data = _request_json(url, timeout=5)
        items = data.get("QuotationCodeTable", {}).get("Data", []) or data.get("data", []) or []
        result = []
        for item in items:
            code = item.get("Code") or item.get("code") or item.get("QuoteID", "").split(".")[-1]
            name = item.get("Name") or item.get("name") or item.get("SecurityName")
            market = str(item.get("MarketType") or item.get("market") or "")
            if not code or not name:
                continue
            exchange = "1" if code.startswith("6") or market in ("1", "沪A") else "0"
            if code.startswith(("0", "3", "4", "6", "8")):
                result.append({"code": code, "name": name, "exchange": exchange})
            if len(result) >= limit:
                break
        return result
    except Exception as exc:
        print(f"搜索股票失败: {exc}")
        return []


def _parse_kline_rows(data):
    rows = []
    for line in data.get("data", {}).get("klines", []) or []:
        parts = line.split(",")
        if len(parts) < 7:
            continue
        rows.append({
            "trade_date": parts[0].replace("-", ""),
            "open": float(parts[1]),
            "close": float(parts[2]),
            "high": float(parts[3]),
            "low": float(parts[4]),
            "volume": float(parts[5]),
            "amount": float(parts[6]),
        })
    return rows


def fetch_index_daily(code, beg="19900101", end=None):
    if end is None:
        end = time.strftime("%Y%m%d")
    info = INDEX_MAP.get(code)
    if not info:
        return []
    url = (
        "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        f"?secid={info['secid']}&klt=101&fqt=0"
        "&fields1=f1,f2,f3,f4,f5,f6"
        "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
        f"&beg={beg}&end={end}&lmt=5000"
    )
    try:
        return _parse_kline_rows(_request_json(url, timeout=12))
    except Exception as exc:
        print(f"获取 {code} 日线失败: {exc}")
        return []


def fetch_index_daily_chunked(code, beg="19900101", end=None, chunk_years=8):
    if end is None:
        end = time.strftime("%Y%m%d")
    rows = []
    start_year = int(beg[:4])
    end_year = int(end[:4])
    for year in range(start_year, end_year + 1, chunk_years):
        chunk_beg = f"{year}0101"
        chunk_end = f"{min(year + chunk_years - 1, end_year)}1231"
        if chunk_beg < beg:
            chunk_beg = beg
        if chunk_end > end:
            chunk_end = end
        part = fetch_index_daily(code, chunk_beg, chunk_end)
        rows.extend(part)
        time.sleep(0.2 + random.random() * 0.2)
    dedup = {row["trade_date"]: row for row in rows}
    return [dedup[key] for key in sorted(dedup)]


def fetch_stock_daily(code, exchange="1", beg="19900101", end=None):
    if end is None:
        end = time.strftime("%Y%m%d")
    secid = f"{exchange}.{code}"
    url = (
        "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        f"?secid={secid}&klt=101&fqt=1"
        "&fields1=f1,f2,f3,f4,f5,f6"
        "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
        f"&beg={beg}&end={end}&lmt=5000"
    )
    try:
        return _parse_kline_rows(_request_json(url, timeout=12))
    except Exception as exc:
        print(f"获取 {code} 个股日线失败: {exc}")
        return []


def sync_index_history(conn, code):
    existing = {row[0] for row in conn.execute("SELECT trade_date FROM index_daily WHERE code=?", (code,))}
    rows = fetch_index_daily_chunked(code)
    new_count = 0
    for row in rows:
        if row["trade_date"] in existing:
            continue
        conn.execute(
            "INSERT OR REPLACE INTO index_daily VALUES (?,?,?,?,?,?,?,?)",
            (code, row["trade_date"], row["open"], row["high"], row["low"], row["close"], row["volume"], row["amount"]),
        )
        new_count += 1
    conn.commit()
    print(f"  {code}: 新增 {new_count} 条")
    return new_count


def sync_stock_history(conn, code, exchange):
    existing = {row[0] for row in conn.execute("SELECT trade_date FROM stock_daily WHERE code=?", (code,))}
    rows = []
    end_year = int(time.strftime("%Y"))
    for year in range(1990, end_year + 1, 8):
        rows.extend(fetch_stock_daily(code, exchange, f"{year}0101", f"{min(year + 7, end_year)}1231"))
        time.sleep(0.2 + random.random() * 0.2)
    rows = list({row["trade_date"]: row for row in rows}.values())
    new_count = 0
    for row in rows:
        if row["trade_date"] in existing:
            continue
        conn.execute(
            "INSERT OR REPLACE INTO stock_daily VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (code, row["trade_date"], row["open"], row["high"], row["low"], row["close"], row["volume"], row["amount"], None, None, None),
        )
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
    for stock in conn.execute("SELECT code, exchange FROM watchlist").fetchall():
        sync_stock_history(conn, stock["code"], stock["exchange"])
    conn.close()
    print("同步完成")


def sync_one_stock(code, exchange):
    conn = get_conn()
    try:
        return sync_stock_history(conn, code, exchange)
    finally:
        conn.close()

