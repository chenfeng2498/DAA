"""
大A监控 - HTTP 服务端 v4
"""
import json, os, time, threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from database import (
    get_conn, init_db, get_index_latest, get_stock_latest,
    get_ma2500, get_ma2500_history, get_ma2500_deviation_history,
    get_price_percentile, get_price_similar_sh, get_index_volume,
    get_watchlist, add_watchlist, remove_watchlist, classification
)
from fetcher import INDEX_MAP

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FRONTEND_DIR = BASE_DIR
INDEX_CODES = ["000001", "399001", "399006", "000688"]

_cache_lock = threading.Lock()
_cached_data = None

def build_dashboard_data():
    conn = get_conn()
    data = {
        "indices": [], "sentiment": {}, "volume": {}, "stocks": [],
        "summary": "", "time": time.strftime("%H:%M:%S"), "ma2500_chart": [],
    }
    # ====== 指数 + MA2500 ======
    for code in INDEX_CODES:
        latest = get_index_latest(conn, code)
        ma = get_ma2500(conn, code)
        deviation = None; pct = None
        cls_label = "fair"; cls_text = "数据不足"
        if ma and ma.get("ma2500") and ma["ma2500"] > 0:
            deviation = round((ma["close"] - ma["ma2500"]) / ma["ma2500"] * 100, 1)
            all_dev = get_ma2500_deviation_history(conn, code)
            if all_dev:
                below = sum(1 for d in all_dev
                    if d.get("ma2500") and d["ma2500"] > 0
                    and (d["close"] - d["ma2500"]) / d["ma2500"] * 100 <= deviation)
                pct = round(below / len(all_dev) * 100, 1) if all_dev else None
                cls_label, cls_text = classification(pct)
        data["indices"].append({
            "code": code, "name": INDEX_MAP.get(code, {}).get("name", code),
            "price": latest["close"] if latest else None,
            "change_pct": None, "change_amt": None,
            "ma2500": round(ma["ma2500"], 1) if ma and ma.get("ma2500") else None,
            "deviation": deviation, "deviation_pct": pct,
            "cls_label": cls_label, "cls_text": cls_text,
        })
    # ====== 成交量 (DB) ======
    db_vol = get_index_volume(conn)
    vol_label = "正常"; vol_cls = "normal"
    if db_vol:
        if db_vol > 15000: vol_label, vol_cls = "活跃", "active"
        elif db_vol < 8000: vol_label, vol_cls = "低迷", "quiet"
    data["volume"] = {"total_amount": db_vol or 0, "label": vol_label, "cls": vol_cls}
    # ====== 情绪 (DB 占位) ======
    data["sentiment"] = {"up_count": 0, "down_count": 0, "ratio": 0,
                         "limit_up": 0, "limit_down": 0,
                         "mood_label": "等待实时数据", "mood_cls": "positive"}
    # ====== 自选股（股价分位 + 大盘映射） ======
    for s in get_watchlist(conn):
        code = s["code"]
        pct = get_price_percentile(conn, code)
        cls_label, cls_text = classification(pct)
        mapping = get_price_similar_sh(conn, code)
        sh_latest = get_index_latest(conn, "000001")
        sh_current = sh_latest["close"] if sh_latest else None
        map_compare = None
        if mapping and sh_current:
            if sh_current > mapping["avg"] * 1.05: map_compare = "up"
            elif sh_current < mapping["avg"] * 0.95: map_compare = "down"
            else: map_compare = "equal"
        latest = get_stock_latest(conn, code)
        data["stocks"].append({
            "code": code, "name": s["name"],
            "price": latest["close"] if latest else None,
            "pe_ttm": None, "pb": None, "total_mv": None,
            "pct": pct, "cls_label": cls_label, "cls_text": cls_text,
            "map_sh_avg": mapping["avg"] if mapping else None,
            "map_sh_min": mapping["min"] if mapping else None,
            "map_sh_max": mapping["max"] if mapping else None,
            "sh_current": sh_current, "map_compare": map_compare,
        })
    # ====== 总结 ======
    sh = data["indices"][0] if data["indices"] else {}
    ds = ""; uv = sum(1 for s in data["stocks"] if s.get("cls_label") == "low")
    if sh.get("deviation") is not None:
        d = "高于" if sh["deviation"] > 0 else "低于"
        ds = f"{d}十年线 {abs(sh['deviation'])}%"
    data["summary"] = (
        f"上证 {sh.get('price', '--')}，{ds}，市场{sh.get('cls_text', '--')}，"
        f"成交{vol_label}（{db_vol or 0:.0f}亿）。自选 {len(data['stocks'])} 只中 {uv} 只低估。"
    )
    # ====== 图表：MA2500 + 4条边界 = 5线6区 ======
    rows = get_ma2500_history(conn, "000001")
    data["ma2500_chart"] = []
    for r in rows:
        ma = r.get("ma2500")
        data["ma2500_chart"].append({
            "date": r["trade_date"], "close": r["close"],
            "ma2500": ma,
            "ma_plus30": round(ma * 1.3, 1) if ma else None,
            "ma_plus10": round(ma * 1.1, 1) if ma else None,
            "ma_minus10": round(ma * 0.9, 1) if ma else None,
            "ma_minus30": round(ma * 0.7, 1) if ma else None,
        })
    conn.close()
    return data

def background_refresh():
    global _cached_data
    from fetcher import fetch_index_realtime, fetch_market_breadth, fetch_stock_realtime
    while True:
        try:
            new_data = build_dashboard_data()
            # 尝试覆盖实时数据
            try:
                rt = fetch_index_realtime()
                for idx in new_data["indices"]:
                    item = rt.get(idx["code"], {})
                    if item.get("price"): idx["price"] = item["price"]
                    idx["change_pct"] = item.get("change_pct")
                    idx["change_amt"] = item.get("change_amt")
            except Exception as e: print(f"  [实时指数] {e}")
            try:
                breadth = fetch_market_breadth()
                if breadth:
                    up = breadth.get("up_count", 0) or 0
                    down = breadth.get("down_count", 0) or 0
                    ratio = round(up / down, 2) if down > 0 else 0
                    ml, mc = "正常偏多", "positive"
                    if ratio > 2: ml, mc = "亢奋", "positive"
                    elif ratio >= 1: pass
                    elif ratio >= 0.5: ml, mc = "正常偏空", "negative"
                    else: ml, mc = "恐慌", "negative"
                    total_amt = breadth.get("total_amount", 0) or 0
                    vl, vc = "正常", "normal"
                    if total_amt > 15000: vl, vc = "活跃", "active"
                    elif total_amt < 8000: vl, vc = "低迷", "quiet"
                    new_data["sentiment"] = {
                        "up_count": up, "down_count": down, "ratio": ratio,
                        "limit_up": breadth.get("limit_up", 0),
                        "limit_down": breadth.get("limit_down", 0),
                        "mood_label": ml, "mood_cls": mc,
                    }
                    new_data["volume"] = {"total_amount": round(total_amt, 0), "label": vl, "cls": vc}
                    sh = new_data["indices"][0] if new_data["indices"] else {}
                    ds = ""; uv = sum(1 for s in new_data["stocks"] if s.get("cls_label") == "low")
                    if sh.get("deviation") is not None:
                        d = "高于" if sh["deviation"] > 0 else "低于"
                        ds = f"{d}十年线 {abs(sh['deviation'])}%"
                    new_data["summary"] = (
                        f"上证 {sh.get('price', '--')}，{ds}，市场{sh.get('cls_text', '--')}，"
                        f"成交{vl}（{total_amt:.0f}亿），情绪{ml}。"
                        f"自选 {len(new_data['stocks'])} 只中 {uv} 只低估。"
                    )
            except Exception as e: print(f"  [实时情绪] {e}")
            # 尝试覆盖个股 PE/PB/市值
            try:
                for s in new_data["stocks"]:
                    exch = "1" if s["code"].startswith("6") else "0"
                    rt = fetch_stock_realtime(s["code"], exch)
                    if rt:
                        if rt.get("pe_ttm"): s["pe_ttm"] = rt["pe_ttm"]
                        if rt.get("pb"): s["pb"] = rt["pb"]
                        if rt.get("total_mv"): s["total_mv"] = rt["total_mv"]
            except Exception as e: print(f"  [实时个股] {e}")
            new_data["time"] = time.strftime("%H:%M:%S")
            with _cache_lock: _cached_data = new_data
        except Exception as e: print(f"  [后台错误] {e}")
        time.sleep(10)

class APIHandler(BaseHTTPRequestHandler):
    def log_message(self, f, *a): pass
    def _send_json(self, data, code=200):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers(); self.wfile.write(body)
    def _send_html(self, path):
        fp = os.path.join(FRONTEND_DIR, path.lstrip("/"))
        if not os.path.exists(fp): self._send_json({"error": "not found"}, 404); return
        with open(fp, "rb") as f: content = f.read()
        ct = "text/html; charset=utf-8"
        if path.endswith(".css"): ct = "text/css"
        elif path.endswith(".js"): ct = "application/javascript"
        self.send_response(200)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", len(content))
        self.end_headers(); self.wfile.write(content)
    def do_GET(self):
        p = urlparse(self.path)
        q = parse_qs(p.query)
        try:
            if p.path == "/api/dashboard":
                with _cache_lock: data = _cached_data
                if data is None: data = build_dashboard_data()
                self._send_json(data)
            elif p.path == "/api/watchlist":
                if q.get("action") == ["add"]:
                    c = get_conn()
                    add_watchlist(c, q.get("code", [""])[0], q.get("name", [""])[0], q.get("exchange", ["1"])[0])
                    c.close(); self._send_json({"ok": True})
                elif q.get("action") == ["remove"]:
                    c = get_conn(); remove_watchlist(c, q.get("code", [""])[0])
                    c.close(); self._send_json({"ok": True})
                else:
                    c = get_conn(); self._send_json(get_watchlist(c)); c.close()
            else:
                self._send_html(p.path.lstrip("/") or "index.html")
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

def run_server(host="0.0.0.0", port=8080):
    init_db()
    print("大A监控服务已启动: http://localhost:8080")
    threading.Thread(target=background_refresh, daemon=True).start()
    print("后台数据刷新线程已启动")
    HTTPServer((host, port), APIHandler).serve_forever()

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "sync":
        init_db()
        from fetcher import sync_all_history
        sync_all_history()
    else:
        # 同步最新日线
                run_server()
