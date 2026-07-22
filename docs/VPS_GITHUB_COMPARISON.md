# Comparacao segura VPS x GitHub

Data: 2026-07-22
Instancia: Julio
Aplicacao observada: app.wizseoservicesaustralia.com
Repositorio publico de destino: contatolucianocokado-dotcom/kaizen
Repositorio operacional observado na VPS: cokado1/roas-turnos-app

## Resultado

A VPS contem o app operacional real e estado local que nao deve ser copiado diretamente para o repositorio publico.
O repositorio `kaizen` deve receber somente artefatos sanitizados: documentacao, templates, scripts sem segredo e regras operacionais.

## Evidencias observadas

- Branch da VPS: `main`
- HEAD da VPS: `a69b43f`
- Arquivos rastreados modificados na VPS: 12
- Arquivos nao rastreados na VPS: 735
- Arquivos operacionais ausentes no repositorio publico: `webapp.py`, `webapp_service.py`, `pull_roas.py`, `roas_store.py`, `roi_aggregator.py`, `oauth_utils.py`, `dimension_normalizer.py`

## Arquivos rastreados modificados na VPS

- `config.example.yaml`
- `deploy/sync_fast.turnos.sh`
- `deploy/sync_full.turnos.sh`
- `dimension_normalizer.py`
- `oauth_utils.py`
- `pull_roas.py`
- `requirements.txt`
- `roas_store.py`
- `roi_aggregator.py`
- `tests/test_pull_roas_dates.py`
- `webapp.py`
- `webapp_service.py`

## Decisao

Nao publicar copia bruta da VPS no GitHub.
Qualquer atualizacao do GitHub deve ser feita por extracao segura: regra, documentacao, template ou script reutilizavel sem dado sensivel.

## Checklist obrigatorio antes de publicar conteudo derivado da VPS

1. Confirmar branch, HEAD e origin da VPS.
2. Comparar o arquivo candidato com o repositorio publico alvo.
3. Remover tokens, senhas, OAuth codes, customer IDs privados, network codes, emails privados, URLs internas e payloads reais.
4. Rodar `python scripts/audit_no_secrets.py`.
5. Revisar `git diff --check`.
6. Registrar a decisao no Graphify ou registro operacional.
7. Publicar em branch `codex/*` e abrir PR para revisao.
