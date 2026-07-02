import json, os
from http.server import HTTPServer, BaseHTTPRequestHandler
from database import init_db, get_conn, get_watchlist

init_db()

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        body = json.dumps({"ok": True, "watchlist": len(get_watchlist(get_conn()))}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

s = HTTPServer(("0.0.0.0", 8080), H)
print("Minimal server on 8080", flush=True)
s.serve_forever()
