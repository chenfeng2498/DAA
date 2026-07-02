import sys
sys.stderr = open("server_err.log", "w", encoding="utf-8")
sys.stdout = open("server_out.log", "w", encoding="utf-8")
from server import run_server
run_server()
