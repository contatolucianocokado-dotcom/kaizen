# Google Ads

## Objetivo

Enviar snapshots de investimento, campanhas, grupos, CPA/CPC, placements e executar acoes pendentes.

## Configuracao

1. Criar OAuth com escopo `https://www.googleapis.com/auth/adwords`.
2. Preencher `google_ads` em `config.yaml`.
3. Instalar `google_ads_script/kaizen_google_ads_script.js` em cada conta.
4. Agendar execucao recorrente.

## Validacao

```bash
python scripts/validate_config.py
python scripts/healthcheck.py
```

## Regras

- Demand Gen usa Attr ID em nivel de grupo quando a UTM vem de `adgroupId`.
- Display/Search/PMAX usam Attr ID conforme a UTM recebida.
- O sistema nao altera UTM.
- Acoes de pausar, ativar, CPA e orcamento devem ser registradas com antes/depois.
