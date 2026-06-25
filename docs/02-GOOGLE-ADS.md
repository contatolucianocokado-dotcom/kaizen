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
- Contas novas com investimento devem ser descobertas pela MCC configurada.
- O investimento exibido deve fechar exatamente com o Google Ads por conta, dia e campanha.
- Divergencia bloqueia decisao automatica baseada em ROI ate reconciliar.
- O Apps Script deve enviar hoje; um job separado pode fechar ontem, 7 dias e mes.
- Toda acao pendente precisa de `customer_id` para impedir aplicacao na conta errada.
