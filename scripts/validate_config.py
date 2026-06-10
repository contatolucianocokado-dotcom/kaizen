from pathlib import Path
import sys
import yaml

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config.yaml"

if not CONFIG.exists():
    print("ERRO: config.yaml nao encontrado. Copie config.example.yaml para config.yaml.")
    sys.exit(1)

cfg = yaml.safe_load(CONFIG.read_text(encoding="utf-8")) or {}
required = [
    ("app", "base_url"),
    ("app", "api_token"),
    ("google_ads", "developer_token"),
    ("google_ads", "client_id"),
    ("google_ads", "client_secret"),
    ("google_ads", "refresh_token"),
    ("gam", "networks"),
    ("projects",),
]

errors = []
for path in required:
    ref = cfg
    for key in path:
        if not isinstance(ref, dict) or key not in ref or ref[key] in ("", None, "COLOQUE_AQUI"):
            errors.append(".".join(path))
            break
        ref = ref[key]

if errors:
    print("Configuracao incompleta:")
    for item in errors:
        print(f"- {item}")
    sys.exit(1)

print("config.yaml valido.")
