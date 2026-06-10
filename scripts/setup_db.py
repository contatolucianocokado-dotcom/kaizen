from pathlib import Path
import sqlite3

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "roas_cache.sqlite3"
SCHEMA = ROOT / "schema.sql"

conn = sqlite3.connect(DB)
conn.executescript(SCHEMA.read_text(encoding="utf-8"))
conn.commit()
conn.close()

print(f"Banco criado/atualizado: {DB}")
