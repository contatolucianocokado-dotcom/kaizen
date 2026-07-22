# Politica de atualizacao segura

Este repositorio deve ser tratado como versao sanitizada do Kaizen. Ele pode receber regras, documentacao, exemplos e codigo reutilizavel, mas nao deve receber dados reais da operacao.

## Permitido

- Regras de arquitetura e agentes.
- Exemplos com placeholders.
- Scripts de auditoria e validacao.
- Documentacao de processo sem credenciais.
- Testes com dados ficticios.

## Proibido

- Tokens, senhas, cookies e OAuth codes.
- Bancos reais, exports reais e snapshots de producao.
- Customer IDs, network codes, emails privados ou dominios sensiveis quando nao forem exemplos publicos.
- Screenshots de paineis internos.
- Logs com payloads privados.

## Fluxo antes de publicar

1. Trabalhar em clone limpo.
2. Revisar `git status`.
3. Rodar `python scripts/audit_no_secrets.py`.
4. Revisar diff.
5. Commitar somente arquivos sanitizados.
6. Fazer push apenas se a auditoria passar.

## Regra de decisao

Se um dado puder expor cliente, conta, token, receita sensivel, configuracao privada ou acesso operacional, ele nao deve entrar no GitHub.
