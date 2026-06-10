from pathlib import Path
import sqlite3
import yaml

ROOT = Path(__file__).resolve().parents[1]
cfg_path = ROOT / "config.yaml"
db_path = ROOT / "roas_cache.sqlite3"

print("Kaizen healthcheck")
print(f"config.yaml: {'ok' if cfg_path.exists() else 'ausente'}")
print(f"banco: {'ok' if db_path.exists() else 'ausente'}")

if cfg_path.exists():
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    print(f"projetos: {len(cfg.get('projects') or [])}")
    print(f"google_ads_customers: {len((cfg.get('google_ads') or {}).get('customers') or [])}")
    print(f"gam_networks: {len((cfg.get('gam') or {}).get('networks') or [])}")

if db_path.exists():
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    print(f"tabelas: {len(cur.fetchall())}")
    conn.close()
