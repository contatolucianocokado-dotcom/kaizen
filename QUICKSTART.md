# Quickstart

## 1. Preparar servidor

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip sqlite3 nginx
```

## 2. Instalar projeto

```bash
git clone https://github.com/contatolucianocokado-dotcom/kaizen.git
cd kaizen
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config.example.yaml config.yaml
cp .env.example .env
```

## 3. Configurar

Leia tambem `docs/01-SETUP-INICIAL.md` para configurar o fluxo completo de Google Ads, GA4, GTM, WordPress/WPCode e Clarity.

Edite `config.yaml` e troque:

- Dominio do app.
- Token interno.
- OAuth completo: Google Ads, GAM, GA4 e GTM.
- Conta GTM e conta GA4 mae.
- Clarity Project ID e Data Export API Token, quando existirem.
- Contas Google Ads.
- Networks GAM.
- Projetos.

## 4. Criar banco

```bash
python scripts/setup_db.py
```

## 5. Validar

```bash
python scripts/validate_config.py
python scripts/healthcheck.py
```

## 6. Rodar local

```bash
python scripts/run_local.py
```

## 7. Instalar Google Ads Script

Copie `google_ads_script/kaizen_google_ads_script.js` para cada conta Google Ads e altere:

```js
const KAIZEN_BASE_URL = 'https://seu-dominio.com';
const KAIZEN_TOKEN = 'troque-este-token';
```

## 8. Validar dados

Confirme:

- Investimento aparece.
- Receita GAM aparece.
- Attr ID fecha com `utm_campaign`.
- Placements aparecem.
- Blocos GAM aparecem.
- Agentes registram alertas.
