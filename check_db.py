import sqlite3, os

db = r"C:\Users\AAVolodin\Chat-bot\data\bot.db"
print("DB path:", db, "| exists:", os.path.exists(db))
con = sqlite3.connect(db)
cur = con.cursor()
for t in ("users","tickets","ticket_events","sync_state"):
    try:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"{t}: OK")
    except Exception as e:
        print(f"{t}: MISSING -> {e}")
con.close()
