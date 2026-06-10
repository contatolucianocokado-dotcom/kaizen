# Instalacao

## Requisitos

- VPS Linux.
- Python 3.10+.
- SQLite.
- Nginx.
- Dominio apontado para a VPS.
- Acesso Google Ads e Google Ad Manager.

## Instalar

```bash
git clone https://github.com/contatolucianocokado-dotcom/kaizen.git
cd kaizen
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config.example.yaml config.yaml
cp .env.example .env
python scripts/setup_db.py
python scripts/validate_config.py
python scripts/healthcheck.py
```

## Subir local

```bash
python scripts/run_local.py
```

## Producao

Use `systemd` ou supervisor para manter o app ativo. Configure Nginx como proxy reverso para a porta do app.
