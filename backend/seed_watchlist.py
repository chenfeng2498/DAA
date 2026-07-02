"""初始化自选股（修复名称）"""
from database import get_conn, init_db, add_watchlist

init_db()
conn = get_conn()

# 先清空重建
conn.execute("DELETE FROM watchlist")
stocks = [
    ("601012", "隆基绿能", "1"),
    ("000796", "凯撒旅业", "0"),
    ("002594", "比亚迪", "0"),
    ("601700", "风范电力", "1"),
]
for code, name, exchange in stocks:
    add_watchlist(conn, code, name, exchange)
    print(f"  添加: {code} {name}")

conn.close()
print("自选股初始化完成")
