"""
大A监控 - HTTP 服务端
"""
import json
import os
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import requests

from database import (
    add_watchlist,
    add_wechat_recipient,
    add_wechat_alert,
    classification,
    count_active_alerts_by_recipient,
    delete_wechat_recipient,
    delete_wechat_alert,
    get_conn,
    get_index_latest,
    get_market_amount_baseline,
    get_index_sample_count,
    get_index_trend_history,
    get_index_volume,
    get_ma2500,
    get_ma2500_history_since,
    get_price_mapped_sh,
    get_price_percentile,
    get_stock_latest,
    get_watchlist,
    get_watchlist_item,
    get_wechat_alert,
    get_wechat_recipient,
    init_db,
    list_wechat_alerts,
    list_wechat_recipients,
    mark_wechat_alert_triggered,
    record_wechat_alert_failure,
    remove_watchlist,
    save_market_amount_snapshots,
    set_default_wechat_recipient,
    set_wechat_alert_status,
)
from fetcher import INDEX_MAP

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FRONTEND_DIR = BASE_DIR
INDEX_CODES = ["000001", "399001", "399006", "000688"]
TWENTY_YEAR_START = f"{datetime.now().year - 20}0101"
THREE_YEAR_START = f"{datetime.now().year - 3}0101"
TREND_WINDOW = 20
ALERT_STATUS_ACTIVE = "监测中"
ALERT_STATUS_DISABLED = "已停用"
ALERT_STATUS_TRIGGERED = "已触发并停用"
DEFAULT_WXPUSH_ENDPOINT = "http://111.230.16.149:5566/wxsend"
DEFAULT_WXPUSH_USERID = "oQEbY277xNWNfWEMnTDuIg9e_ayQ"
DEFAULT_WXPUSH_TEMPLATE_ID = "yFMKEFTTAPwGIiG7A8yOW2u9lb_RLN-P35SjIVhhGnM"


def _env_float(name, default):
    value = os.getenv(name)
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        print(f"配置 {name}={value!r} 无效，使用默认值 {default}")
        return default


WXPUSH_ENDPOINT = os.getenv("WXPUSH_ENDPOINT", DEFAULT_WXPUSH_ENDPOINT).strip() or DEFAULT_WXPUSH_ENDPOINT
WXPUSH_DEFAULT_USERID = os.getenv("WXPUSH_DEFAULT_USERID", DEFAULT_WXPUSH_USERID).strip()
WXPUSH_DEFAULT_TEMPLATE_ID = os.getenv("WXPUSH_DEFAULT_TEMPLATE_ID", DEFAULT_WXPUSH_TEMPLATE_ID).strip()
WXPUSH_CONNECT_TIMEOUT = _env_float("WXPUSH_CONNECT_TIMEOUT", _env_float("WXPUSH_TIMEOUT", 6))
WXPUSH_READ_TIMEOUT = _env_float("WXPUSH_READ_TIMEOUT", WXPUSH_CONNECT_TIMEOUT)
WXPUSH_TIMEOUT = (WXPUSH_CONNECT_TIMEOUT, WXPUSH_READ_TIMEOUT)

_cache_lock = threading.Lock()
_cached_data = None


def _compare_position(position_gap):
    if position_gap is None:
        return None
    if position_gap > 5:
        return "up"
    if position_gap < -5:
        return "down"
    return "equal"


def _index_classification_by_ma_ratio(close, ma2500):
    if close is None or ma2500 is None or ma2500 <= 0:
        return "fair", "数据不足"
    ratio = close / ma2500
    epsilon = 1e-10
    if ratio + epsilon >= 1.65:
        return "high", "玩命"
    if ratio + epsilon >= 1.45:
        return "high", "高度泡沫"
    if ratio + epsilon >= 1.25:
        return "high", "轻度泡沫"
    if ratio + epsilon >= 1.05:
        return "fair", "估值合理"
    if ratio + epsilon >= 1 / 1.15:
        return "low", "比较便宜"
    return "low", "极度低估"


def _trend_classification(row):
    if not row or any(row.get(key) is None for key in ("close", "ma250", "slope", "accel")):
        return "gray", "趋势数据不足"
    price, ma250, slope, accel = row["close"], row["ma250"], row["slope"], row["accel"]
    if price >= ma250 * 1.05 and slope > 0 and accel < 0:
        return "fair", "高位钝化"
    if price >= ma250 and slope > 0:
        return "high", "牛市趋势"
    if price >= ma250 and slope <= 0:
        return "fair", "高位转弱"
    if price < ma250 and (slope > 0 or accel > 0):
        return "blue", "修复阶段"
    return "low", "熊市趋势"


def _amount_label(ratio):
    if ratio is None:
        return "基准数据不足", "gray"
    if ratio >= 1.5:
        return "极度活跃", "high"
    if ratio >= 1.2:
        return "活跃", "high"
    if ratio >= 0.8:
        return "正常", "fair"
    if ratio >= 0.6:
        return "低迷", "low"
    return "极度低迷", "low"


def _format_index_price(value):
    if value is None:
        return "--"
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "--"


def _downsample(rows, max_points):
    if len(rows) <= max_points:
        return rows
    step = (len(rows) - 1) / (max_points - 1)
    indexes = sorted({0, len(rows) - 1, *(round(index * step) for index in range(max_points))})
    return [rows[index] for index in indexes]


def _mask_userid(userid):
    if not userid:
        return "默认微信用户"
    if len(userid) <= 10:
        return userid[:2] + "***"
    return f"{userid[:4]}...{userid[-4:]}"


def _alert_to_public(alert):
    recipient_name = alert.get("recipient_name") or "默认微信用户"
    recipient_userid = alert.get("recipient_userid") or alert.get("userid")
    return {
        "id": alert["id"],
        "code": alert["code"],
        "name": alert["name"],
        "target_price": alert["target_price"],
        "status": alert["status"],
        "recipient_id": alert.get("recipient_id"),
        "recipient_name": recipient_name,
        "recipient_label": f"{recipient_name} · {_mask_userid(recipient_userid)}",
        "receiver_label": f"{recipient_name} · {_mask_userid(recipient_userid)}",
        "created_at": alert.get("created_at"),
        "updated_at": alert.get("updated_at"),
        "triggered_at": alert.get("triggered_at"),
        "last_error": alert.get("last_error"),
        "last_checked_price": alert.get("last_checked_price"),
    }


def _recipient_to_public(recipient):
    return {
        "id": recipient["id"],
        "name": recipient["name"],
        "userid_label": _mask_userid(recipient.get("userid")),
        "is_default": bool(recipient.get("is_default")),
        "is_system": bool(recipient.get("is_system")),
        "created_at": recipient.get("created_at"),
    }


def _send_wechat_alert(alert, current_price):
    triggered_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    params = {
        "title": "大A监控价格提醒",
        "content": (
            f"{alert['name']}（{alert['code']}）当前价格 {current_price:.2f}，"
            f"已达到目标价格 {float(alert['target_price']):.2f}。\n"
            f"触发时间：{triggered_at}\n"
            "这是一次性价格提醒，发送成功后已自动停用。"
        ),
    }
    userid = alert.get("recipient_userid") or alert.get("userid") or WXPUSH_DEFAULT_USERID
    if not alert.get("recipient_is_system") and userid:
        params["userid"] = userid
    response = requests.get(WXPUSH_ENDPOINT, params=params, timeout=WXPUSH_TIMEOUT)
    if not response.ok:
        raise RuntimeError(f"http_status={response.status_code}")
    result = response.json()
    if result.get("errcode") != 0:
        raise RuntimeError(f"errcode={result.get('errcode')} errmsg={result.get('errmsg')}")
    return result


def check_wxpush_connectivity(send_message=False):
    params = {
        "title": "大A监控连通性测试",
        "content": f"Go-WXPush 连通性测试：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    }
    if send_message and WXPUSH_DEFAULT_USERID:
        params["userid"] = WXPUSH_DEFAULT_USERID
    elif WXPUSH_DEFAULT_USERID:
        params["userid"] = WXPUSH_DEFAULT_USERID
    print(f"Go-WXPush endpoint: {WXPUSH_ENDPOINT}")
    print(f"Go-WXPush timeout: connect={WXPUSH_CONNECT_TIMEOUT}s read={WXPUSH_READ_TIMEOUT}s")
    started_at = time.perf_counter()
    try:
        response = requests.get(WXPUSH_ENDPOINT, params=params, timeout=WXPUSH_TIMEOUT)
        elapsed = time.perf_counter() - started_at
        print(f"HTTP {response.status_code} in {elapsed:.2f}s")
        print(response.text[:500])
        response.raise_for_status()
        result = response.json()
        if result.get("errcode") == 0:
            print("Go-WXPush 连通性正常")
            return True
        print(f"Go-WXPush 返回业务错误: errcode={result.get('errcode')} errmsg={result.get('errmsg')}")
        return False
    except Exception as exc:
        print(f"Go-WXPush 连通性异常: {_safe_alert_error(exc, {'userid': WXPUSH_DEFAULT_USERID})}")
        return False


def _safe_alert_error(exc, alert):
    if isinstance(exc, requests.exceptions.ConnectTimeout):
        return "连接 Go-WXPush 服务超时"
    if isinstance(exc, requests.exceptions.ReadTimeout):
        return "Go-WXPush 服务响应超时"
    if isinstance(exc, requests.exceptions.ConnectionError):
        return "无法连接 Go-WXPush 服务"
    message = str(exc)
    for secret in (WXPUSH_DEFAULT_USERID, WXPUSH_DEFAULT_TEMPLATE_ID, alert.get("userid"), alert.get("recipient_userid")):
        if secret:
            message = message.replace(secret, "***")
    endpoint = requests.Request("GET", WXPUSH_ENDPOINT).prepare().url
    if endpoint:
        message = message.replace(endpoint, WXPUSH_ENDPOINT)
    if "/wxsend?" in message:
        message = message.split("/wxsend?", 1)[0] + "/wxsend?<redacted>"
    if len(message) > 180:
        message = message[:180] + "..."
    return message or exc.__class__.__name__


def monitor_wechat_alerts(current_prices):
    if not current_prices:
        return
    conn = get_conn()
    try:
        alerts = list_wechat_alerts(conn, active_only=True)
    finally:
        conn.close()
    for alert in alerts:
        current_price = current_prices.get(alert["code"])
        if current_price is None or float(current_price) < float(alert["target_price"]):
            continue
        try:
            _send_wechat_alert(alert, float(current_price))
        except Exception as exc:
            safe_error = _safe_alert_error(exc, alert)
            conn = get_conn()
            try:
                record_wechat_alert_failure(conn, alert["id"], current_price, safe_error)
            finally:
                conn.close()
            print(f"微信提醒发送失败 {alert['code']}#{alert['id']}: {safe_error}")
            continue
        conn = get_conn()
        try:
            mark_wechat_alert_triggered(conn, alert["id"], current_price)
        finally:
            conn.close()
        print(f"微信提醒已发送 {alert['code']}#{alert['id']} target={alert['target_price']}")


def build_dashboard_data():
    conn = get_conn()
    data = {
        "indices": [],
        "sentiment": {},
        "volume": {},
        "stocks": [],
        "summary": "",
        "time": time.strftime("%H:%M:%S"),
        "ma2500_chart": [],
        "trend": {"code": "000001", "name": "上证指数", "chart": []},
        "distribution": {},
        "alerts": [],
        "recipients": [],
    }

    for code in INDEX_CODES:
        latest = get_index_latest(conn, code)
        ma = get_ma2500(conn, code)
        close = latest["close"] if latest else None
        sample_count = get_index_sample_count(conn, code)
        cls_label, cls_text = "fair", "数据不足"
        if ma and ma.get("ma2500") and ma["ma2500"] > 0:
            close = ma["close"]
            cls_label, cls_text = _index_classification_by_ma_ratio(close, ma["ma2500"])
        data["indices"].append({
            "code": code,
            "name": INDEX_MAP.get(code, {}).get("name", code),
            "price": close,
            "change_pct": None,
            "change_amt": None,
            "ma2500": round(ma["ma2500"], 2) if ma and ma.get("ma2500") else None,
            "ma_sample_count": sample_count,
            "ma_required_count": 2500,
            "cls_label": cls_label,
            "cls_text": cls_text,
        })

    db_volume = get_index_volume(conn)
    data["volume"] = {"total_amount": db_volume or 0, "label": "等待实时数据", "cls": "gray"}
    data["sentiment"] = {
        "up_count": 0,
        "down_count": 0,
        "ratio": 0,
        "limit_up": 0,
        "limit_down": 0,
        "mood_label": "等待实时数据",
        "mood_cls": "positive",
    }

    sh_latest = get_index_latest(conn, "000001")
    sh_current = sh_latest["close"] if sh_latest else None
    for stock in get_watchlist(conn):
        code = stock["code"]
        latest = get_stock_latest(conn, code)
        price = latest["close"] if latest else None
        pct = get_price_percentile(conn, code, price)
        cls_label, cls_text = classification(pct)
        mapping = get_price_mapped_sh(conn, code, price, sh_current)
        if mapping and mapping.get("stock_price_percentile") is not None:
            pct = mapping["stock_price_percentile"]
            cls_label, cls_text = classification(pct)
        mapped_value = mapping["mapped_sh_index"] if mapping else None
        position_gap = mapping.get("position_gap") if mapping else None
        data["stocks"].append({
            "code": code,
            "name": stock["name"],
            "price": latest["close"] if latest else None,
            "pe_ttm": latest.get("pe_ttm") if latest else None,
            "pb": latest.get("pb") if latest else None,
            "total_mv": latest.get("total_mv") if latest else None,
            "pct": pct,
            "cls_label": cls_label,
            "cls_text": cls_text,
            "map_sh_avg": mapped_value,
            "mapped_sh_index": mapped_value,
            "mapped_date": mapping.get("mapped_date") if mapping else None,
            "mapping_price_date": latest.get("trade_date") if latest else None,
            "index_pct": mapping.get("index_percentile") if mapping else None,
            "position_gap": position_gap,
            "mapping_sample_count": mapping.get("sample_count", 0) if mapping else 0,
            "mapping_required_count": mapping.get("required_count", 250) if mapping else 250,
            "sh_current": sh_current,
            "map_compare": _compare_position(position_gap),
        })

    sh = data["indices"][0] if data["indices"] else {}
    low_count = sum(1 for stock in data["stocks"] if stock.get("cls_label") == "low")
    data["summary"] = (
        f"上证 {_format_index_price(sh.get('price'))}，MA2500 当前{sh.get('cls_text', '数据不足')}；"
        f"市场情绪等待实时数据，成交量等待实时数据。自选 {len(data['stocks'])} 只中 {low_count} 只低估。"
    )

    ma_rows = get_ma2500_history_since(conn, "000001", TWENTY_YEAR_START)
    for row in _downsample(ma_rows, 1800):
        ma = row.get("ma2500")
        data["ma2500_chart"].append({
            "date": row["trade_date"],
            "close": row["close"],
            "ma2500": round(ma, 2) if ma else None,
            "play": round(ma * 1.65, 2) if ma else None,
            "bubble_high": round(ma * 1.45, 2) if ma else None,
            "bubble_light": round(ma * 1.25, 2) if ma else None,
            "fair": round(ma * 1.05, 2) if ma else None,
            "cheap": round(ma / 1.15, 2) if ma else None,
            "extreme_low": round(ma / 1.35, 2) if ma else None,
        })
    trend_rows = get_index_trend_history(conn, "000001", THREE_YEAR_START, TREND_WINDOW)
    trend_cls, trend_text = _trend_classification(trend_rows[-1] if trend_rows else None)
    data["trend"] = {
        "code": "000001",
        "name": "上证指数",
        "window": TREND_WINDOW,
        "cls": trend_cls,
        "label": trend_text,
        "chart": _downsample(trend_rows, 800),
    }
    data["alerts"] = [_alert_to_public(alert) for alert in list_wechat_alerts(conn)]
    data["recipients"] = [_recipient_to_public(recipient) for recipient in list_wechat_recipients(conn)]
    conn.close()
    return data


def background_refresh():
    global _cached_data
    from fetcher import fetch_index_realtime, fetch_market_distribution, fetch_stock_realtime

    while True:
        try:
            with _cache_lock:
                previous_data = _cached_data
            new_data = build_dashboard_data()
            rt_indices = fetch_index_realtime()
            for index in new_data["indices"]:
                item = rt_indices.get(index["code"], {})
                if item.get("price"):
                    index["price"] = item["price"]
                    if index.get("ma2500"):
                        index["cls_label"], index["cls_text"] = _index_classification_by_ma_ratio(
                            item["price"], index["ma2500"]
                        )
                index["change_pct"] = item.get("change_pct")
                index["change_amt"] = item.get("change_amt")
            sh_current = new_data["indices"][0].get("price") if new_data["indices"] else None

            distribution = fetch_market_distribution()
            if distribution:
                now = datetime.now()
                amounts = distribution.get("amounts", {})
                conn = get_conn()
                try:
                    baselines = {}
                    for key in amounts:
                        baseline, baseline_count = get_market_amount_baseline(
                            conn, key, now.strftime("%H:%M"), now.strftime("%Y%m%d")
                        )
                        baselines[key] = (baseline, baseline_count)
                    save_market_amount_snapshots(
                        conn, now.strftime("%Y%m%d"), now.strftime("%H:%M"), amounts, now.isoformat(timespec="seconds")
                    )
                finally:
                    conn.close()
                amount_items = []
                for key, label in (("all_a", "全A"), ("sh_a", "沪市A股"), ("sz_a", "深市A股"), ("bj_a", "北交所"), ("etf", "ETF")):
                    baseline, baseline_count = baselines.get(key, (None, 0))
                    ratio = amounts.get(key, 0) / baseline if baseline else None
                    volume_label, volume_cls = _amount_label(ratio)
                    baseline_change_pct = (ratio - 1) * 100 if ratio is not None else None
                    amount_items.append({
                        "key": key,
                        "label": label,
                        "amount": amounts.get(key, 0),
                        "ratio": round(ratio, 2) if ratio is not None else None,
                        "baseline_change_pct": round(baseline_change_pct, 1) if baseline_change_pct is not None else None,
                        "baseline_count": baseline_count,
                        "volume_label": volume_label,
                        "cls": volume_cls,
                    })
                distribution["amount_items"] = amount_items
                new_data["distribution"] = distribution
                all_a_item = amount_items[0]
                new_data["summary"] = (
                    f"上证 {_format_index_price(new_data['indices'][0].get('price'))}，MA2500 当前"
                    f"{new_data['indices'][0].get('cls_text', '数据不足')}；市场情绪"
                    f"{distribution.get('mood_label', '等待实时数据')}，成交量"
                    f"{all_a_item['volume_label']}。"
                )
            elif previous_data and previous_data.get("distribution"):
                new_data["distribution"] = previous_data["distribution"]

            current_stock_prices = {}
            for stock in new_data["stocks"]:
                exchange = "1" if stock["code"].startswith("6") else "0"
                item = fetch_stock_realtime(stock["code"], exchange)
                if item.get("price"):
                    stock["price"] = item["price"]
                    current_stock_prices[stock["code"]] = item["price"]
                    conn = get_conn()
                    try:
                        latest_adjusted = get_stock_latest(conn, stock["code"])
                        mapping_price = latest_adjusted["close"] if latest_adjusted else None
                        pct = get_price_percentile(conn, stock["code"], mapping_price)
                        mapping = get_price_mapped_sh(conn, stock["code"], mapping_price, sh_current)
                    finally:
                        conn.close()
                    if mapping and mapping.get("stock_price_percentile") is not None:
                        pct = mapping["stock_price_percentile"]
                    cls_label, cls_text = classification(pct)
                    mapped_value = mapping["mapped_sh_index"] if mapping else None
                    stock["pct"] = pct
                    stock["cls_label"] = cls_label
                    stock["cls_text"] = cls_text
                    stock["map_sh_avg"] = mapped_value
                    stock["mapped_sh_index"] = mapped_value
                    stock["mapped_date"] = mapping.get("mapped_date") if mapping else None
                    stock["mapping_price_date"] = latest_adjusted.get("trade_date") if latest_adjusted else None
                    stock["index_pct"] = mapping.get("index_percentile") if mapping else None
                    stock["position_gap"] = mapping.get("position_gap") if mapping else None
                    stock["mapping_sample_count"] = mapping.get("sample_count", 0) if mapping else 0
                    stock["sh_current"] = sh_current
                    stock["map_compare"] = _compare_position(stock["position_gap"])
                if item.get("change_pct") is not None:
                    stock["change_pct"] = item["change_pct"]
                if item.get("pe_ttm"):
                    stock["pe_ttm"] = item["pe_ttm"]
                if item.get("pb"):
                    stock["pb"] = item["pb"]
                if item.get("total_mv"):
                    stock["total_mv"] = item["total_mv"]
                if item.get("name") and stock["name"] == stock["code"]:
                    stock["name"] = item["name"]

            monitor_wechat_alerts(current_stock_prices)
            conn = get_conn()
            try:
                new_data["alerts"] = [_alert_to_public(alert) for alert in list_wechat_alerts(conn)]
                new_data["recipients"] = [_recipient_to_public(recipient) for recipient in list_wechat_recipients(conn)]
            finally:
                conn.close()

            if new_data.get("trend", {}).get("chart") and sh_current is not None:
                latest_trend = dict(new_data["trend"]["chart"][-1])
                latest_trend["close"] = sh_current
                trend_cls, trend_text = _trend_classification(latest_trend)
                new_data["trend"]["cls"] = trend_cls
                new_data["trend"]["label"] = trend_text

            new_data["time"] = time.strftime("%H:%M:%S")
            with _cache_lock:
                _cached_data = new_data
        except Exception as exc:
            print(f"  [后台错误] {exc}")
        time.sleep(10)


class APIHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _send_json(self, data, code=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, path):
        full_path = os.path.join(FRONTEND_DIR, path.lstrip("/"))
        if not os.path.exists(full_path):
            self._send_json({"error": "not found"}, 404)
            return
        with open(full_path, "rb") as file:
            content = file.read()
        content_type = "text/html; charset=utf-8"
        if path.endswith(".css"):
            content_type = "text/css"
        elif path.endswith(".js"):
            content_type = "application/javascript"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", len(content))
        self.end_headers()
        self.wfile.write(content)

    def do_GET(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        try:
            if parsed.path == "/api/dashboard":
                with _cache_lock:
                    data = _cached_data
                if data is None:
                    data = build_dashboard_data()
                self._send_json(data)
            elif parsed.path == "/api/search":
                from fetcher import search_stock
                keyword = query.get("q", [""])[0]
                self._send_json(search_stock(keyword))
            elif parsed.path == "/api/watchlist":
                self._handle_watchlist(query)
            elif parsed.path == "/api/alerts":
                self._handle_alerts(query)
            elif parsed.path == "/api/recipients":
                self._handle_recipients(query)
            else:
                self._send_html(parsed.path.lstrip("/") or "index.html")
        except Exception as exc:
            self._send_json({"error": str(exc)}, 500)

    def _handle_watchlist(self, query):
        action = query.get("action", [""])[0]
        if action == "add":
            code = query.get("code", [""])[0]
            name = query.get("name", [code])[0] or code
            exchange = query.get("exchange", ["1" if code.startswith("6") else "0"])[0]
            conn = get_conn()
            add_watchlist(conn, code, name, exchange)
            conn.close()
            try:
                from fetcher import sync_one_stock
                sync_one_stock(code, exchange)
            except Exception as exc:
                print(f"同步新增股票失败: {exc}")
            global _cached_data
            with _cache_lock:
                _cached_data = None
            self._send_json({"ok": True})
        elif action == "remove":
            code = query.get("code", [""])[0]
            conn = get_conn()
            remove_watchlist(conn, code)
            conn.close()
            with _cache_lock:
                _cached_data = None
            self._send_json({"ok": True})
        else:
            conn = get_conn()
            self._send_json(get_watchlist(conn))
            conn.close()

    def _handle_alerts(self, query):
        action = query.get("action", [""])[0]
        if action == "add":
            code = query.get("code", [""])[0].strip()
            target_raw = query.get("target_price", [""])[0]
            recipient_raw = query.get("recipient_id", [""])[0]
            try:
                target_price = float(target_raw)
            except (TypeError, ValueError):
                self._send_json({"error": "目标价格必须是数字"}, 400)
                return
            try:
                recipient_id = int(recipient_raw)
            except (TypeError, ValueError):
                self._send_json({"error": "请选择微信接收人"}, 400)
                return
            if not code:
                self._send_json({"error": "请选择自选股"}, 400)
                return
            if target_price <= 0:
                self._send_json({"error": "目标价格必须大于 0"}, 400)
                return
            conn = get_conn()
            try:
                stock = get_watchlist_item(conn, code)
                if not stock:
                    self._send_json({"error": "只能为当前自选股创建提醒"}, 400)
                    return
                recipient = get_wechat_recipient(conn, recipient_id)
                if not recipient:
                    self._send_json({"error": "微信接收人不存在"}, 400)
                    return
                alert_id = add_wechat_alert(conn, code, stock["name"], target_price, recipient_id)
                alert = get_wechat_alert(conn, alert_id)
            finally:
                conn.close()
            with _cache_lock:
                global _cached_data
                _cached_data = None
            self._send_json({"ok": True, "alert": _alert_to_public(alert)})
            return
        if action in ("disable", "enable", "delete"):
            try:
                alert_id = int(query.get("id", ["0"])[0])
            except (TypeError, ValueError):
                self._send_json({"error": "提醒 ID 无效"}, 400)
                return
            conn = get_conn()
            try:
                alert = get_wechat_alert(conn, alert_id)
                if not alert:
                    self._send_json({"error": "提醒不存在"}, 404)
                    return
                if action == "delete":
                    delete_wechat_alert(conn, alert_id)
                elif alert["status"] == ALERT_STATUS_TRIGGERED:
                    self._send_json({"error": "已触发提醒不能重新启用或停用"}, 400)
                    return
                elif action == "disable":
                    set_wechat_alert_status(conn, alert_id, ALERT_STATUS_DISABLED)
                else:
                    set_wechat_alert_status(conn, alert_id, ALERT_STATUS_ACTIVE)
            finally:
                conn.close()
            with _cache_lock:
                _cached_data = None
            self._send_json({"ok": True})
            return
        conn = get_conn()
        try:
            self._send_json([_alert_to_public(alert) for alert in list_wechat_alerts(conn)])
        finally:
            conn.close()

    def _handle_recipients(self, query):
        action = query.get("action", [""])[0]
        if action == "add":
            name = query.get("name", [""])[0].strip()
            userid = query.get("userid", [""])[0].strip()
            is_default = query.get("is_default", ["0"])[0] in ("1", "true", "yes")
            if not name:
                self._send_json({"error": "接收人名称不能为空"}, 400)
                return
            if not userid:
                self._send_json({"error": "OpenID 不能为空"}, 400)
                return
            conn = get_conn()
            try:
                recipient_id = add_wechat_recipient(conn, name, userid, is_default)
                recipient = get_wechat_recipient(conn, recipient_id)
            finally:
                conn.close()
            with _cache_lock:
                global _cached_data
                _cached_data = None
            self._send_json({"ok": True, "recipient": _recipient_to_public(recipient)})
            return
        if action in ("set_default", "delete"):
            try:
                recipient_id = int(query.get("id", ["0"])[0])
            except (TypeError, ValueError):
                self._send_json({"error": "接收人 ID 无效"}, 400)
                return
            conn = get_conn()
            try:
                recipient = get_wechat_recipient(conn, recipient_id)
                if not recipient:
                    self._send_json({"error": "接收人不存在"}, 404)
                    return
                if action == "set_default":
                    set_default_wechat_recipient(conn, recipient_id)
                else:
                    active_refs = count_active_alerts_by_recipient(conn, recipient_id)
                    if active_refs:
                        self._send_json({"error": "该接收人仍被监测中的提醒引用，请先停用或删除相关提醒"}, 400)
                        return
                    if recipient.get("is_system"):
                        self._send_json({"error": "默认系统接收人不能删除"}, 400)
                        return
                    delete_wechat_recipient(conn, recipient_id)
            finally:
                conn.close()
            with _cache_lock:
                _cached_data = None
            self._send_json({"ok": True})
            return
        conn = get_conn()
        try:
            self._send_json([_recipient_to_public(recipient) for recipient in list_wechat_recipients(conn)])
        finally:
            conn.close()


def run_server(host="0.0.0.0", port=8090):
    init_db()
    print(f"大A监控服务已启动: http://localhost:{port}")
    print(f"Go-WXPush: {WXPUSH_ENDPOINT} timeout={WXPUSH_CONNECT_TIMEOUT}s/{WXPUSH_READ_TIMEOUT}s")
    threading.Thread(target=background_refresh, daemon=True).start()
    print("后台数据刷新线程已启动")
    ThreadingHTTPServer((host, port), APIHandler).serve_forever()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "sync":
        init_db()
        from fetcher import sync_all_history
        sync_all_history()
    elif len(sys.argv) > 1 and sys.argv[1] == "wxpush-check":
        raise SystemExit(0 if check_wxpush_connectivity() else 1)
    else:
        run_server()

