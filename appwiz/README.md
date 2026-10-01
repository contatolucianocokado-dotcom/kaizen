# app.wiz recovered layer

Esta pasta guarda a camada operacional recuperada do `app.wiz`.

Ela existe porque o app em producao tinha mais componentes do que o starter publicado em `app/`.
O objetivo aqui e preservar codigo, agentes e regras para consolidacao segura no projeto principal.

## Conteudo

- `webapp.py` e `webapp_service.py`: painel operacional recuperado.
- `quality_os.py`: sistema de regras, scorecards e agentes de qualidade.
- `quality_error_registry.py`: banco de erros, riscos preditivos e aprendizado.
- `agent_backlog.py`: inventario operacional de agentes.
- `campaign_agent_governance.py`: governanca de campanhas.
- `scripts/`: guardas, validadores e rotinas de sincronizacao.
- `systemd/`: unidades modelo para instalacao no servidor.

## Estado

Esta camada ainda nao substitui automaticamente `app/`.
Alguns imports do runtime original ainda precisam ser trazidos ou reimplementados antes de usar como app principal.

## Regras de seguranca

- Nao versionar `config.yaml`, `.env`, bancos, logs, backups ou tokens reais.
- Tokens de servico devem ficar em arquivos externos, como `/etc/wiz/*.env`.
- Rodar `python scripts/audit_no_secrets.py` antes de publicar.
