# Regras de Dados

## Atribuicao

- Investimento vem do Google Ads.
- Receita vem do GAM.
- ROI = `(receita - investimento) / investimento`.
- PMR vem do GAM.
- eCPM vem do GAM.

## Dados zerados

Campanha sem dados em todos os periodos nao precisa aparecer.

Campanha com investimento sempre precisa aparecer.

## Divergencia

Toda divergencia relevante deve gerar incidente:

- Ads tem investimento e plataforma nao tem.
- GAM tem receita e plataforma nao tem.
- Plataforma soma receita em attr errado.
- Resumo de campanha nao bate com blocos.
- Hoje/ontem/7 dias/mes com periodos inconsistentes.

