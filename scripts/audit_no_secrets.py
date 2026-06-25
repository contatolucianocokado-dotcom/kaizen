from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
SKIP = {".git", ".venv", "__pycache__"}
PATTERNS = {
    "oauth_code": re.compile(r"[?&]code=4/[A-Za-z0-9_-]+"),
    "private_key": re.compile(r"-----BEGIN (?:RSA |OPENSSH )?PRIVATE KEY-----"),
    "google_refresh_token": re.compile(r"\b1//[A-Za-z0-9_-]{20,}\b"),
    "jwt": re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b"),
    "ipv4_with_password_context": re.compile(r"(?i)(?:password|senha).{0,80}\b\d{1,3}(?:\.\d{1,3}){3}\b"),
}

findings = []
for path in ROOT.rglob("*"):
    if not path.is_file() or any(part in SKIP for part in path.parts):
        continue
    if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".sqlite", ".db"}:
        continue
    try:
        text = path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        continue
    for name, pattern in PATTERNS.items():
        if pattern.search(text):
            findings.append(f"{path.relative_to(ROOT)}: {name}")

if findings:
    print("Dados potencialmente sensiveis encontrados:")
    print("\n".join(findings))
    raise SystemExit(1)

print("OK: nenhum segredo conhecido foi encontrado.")
