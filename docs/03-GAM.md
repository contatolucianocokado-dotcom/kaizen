# Google Ad Manager

## Objetivo

Trazer receita, PMR, eCPM, blocos e placements por Attr ID.

## Configuracao

1. Criar OAuth com escopo `https://www.googleapis.com/auth/admanager`.
2. Preencher `gam.networks` em `config.yaml`.
3. Garantir que relatorios incluam:
   - Site.
   - Chaves-valor.
   - Bloco de anuncios.
   - Receita.
   - Impressoes.
   - Cliques.
   - Taxa de correspondencia.
   - eCPM.

## Validacao

- Receita total do projeto deve bater com GAM.
- Receita por campanha deve vir por `utm_campaign`.
- Blocos devem abrir dentro da campanha.
- Placements devem abrir com blocos vinculados quando possivel.
