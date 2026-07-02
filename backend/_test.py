import sys
sys.stderr = open("_err.log", "w", encoding="utf-8")
sys.stdout = open("_out.log", "w", encoding="utf-8")

from http.server import HTTPServer
from database import init_db, get_conn
from server import APIHandler

init_db()
s = HTTPServer(("0.0.0.0", 8080), APIHandler)
# NO background sync thread
sys.stdout.write("Server started on 8080\n")
sys.stdout.flush()
s.serve_forever()
