# Kaizen

Starter kit sanitizado para montar uma operacao de controle de ROI com Google Ads, Google Ad Manager, atribuicao por Attr ID, auditoria de dados e agentes operacionais.

Este repositorio nao contem senhas, tokens, banco real, cookies, credenciais OAuth, chaves SSH ou dados sensiveis.

## O que este starter entrega

- Estrutura base do painel.
- Schema inicial do banco.
- Configuracao modelo.
- Scripts de instalacao e validacao.
- Google Ads Script modelo.
- Documentacao de Ads, GAM, UTMs, agentes, refresh e troubleshooting.
- Exemplos fake para testar sem dados reais.

## Caminho rapido

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
python scripts/run_local.py
```

Depois abra:

```text
http://127.0.0.1:8080
```

## Arquivos principais

- `QUICKSTART.md`: passo a passo direto.
- `config.example.yaml`: modelo de configuracao.
- `schema.sql`: banco inicial vazio.
- `google_ads_script/kaizen_google_ads_script.js`: script para copiar no Google Ads.
- `docs/`: guias de implementacao.
- `scripts/`: validadores e utilitarios.
- `examples/`: dados fake para teste.

## Regra intrinseca

UTM e dado de origem, nao dado editavel.

O sistema nao altera `utm_campaign`, nao remapeia receita GAM de um ID para outro e nao inventa atribuicao. Se houver divergencia, registra alerta para o agente responsavel.
