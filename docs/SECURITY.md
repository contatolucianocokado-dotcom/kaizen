# Seguranca e Configuracao Privada

## Cada instalacao deve fornecer

- Dominio do painel.
- Token interno da API.
- MCC e contas Google Ads.
- Developer token.
- OAuth client ID, client secret e refresh token.
- Networks GAM e moedas.
- Projetos e dominios.
- GA4 e GTM.
- Clarity Project ID e Data Export token.
- Telegram, se usado.
- Credenciais WordPress, se a automacao for habilitada.

## Nunca publicar

- `config.yaml` real.
- `.env` real.
- Banco SQLite/PostgreSQL.
- OAuth callback com `code=`.
- Refresh token.
- Developer token.
- Client secret.
- Token de bot.
- Senhas WordPress.
- IP, senha ou chave SSH.
- Cookies e sessoes de navegador.
- IDs reais de contas e networks quando o repositorio for compartilhado como modelo.

## Placeholders

Use nomes como:

```text
SEU_MCC_ID
SEU_CUSTOMER_ID
SEU_NETWORK_CODE
${GOOGLE_OAUTH_REFRESH_TOKEN}
```

## Auditoria

Antes de cada push:

```bash
python scripts/audit_no_secrets.py
```

Se o script falhar, remova o dado, revogue a credencial exposta e gere outra.
