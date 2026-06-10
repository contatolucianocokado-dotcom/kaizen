# Google Ads Script

## Objetivo

Enviar dados do Google Ads para o Kaizen e aplicar acoes pendentes.

## Deve enviar

- hoje;
- ontem;
- ultimos 7 dias;
- mes;
- campanhas;
- grupos;
- placements;
- orcamento;
- CPA/CPC;
- status;
- conversoes;
- custo por conversao.

## Deve aplicar

- alterar CPA/CPC quando aprovado;
- alterar orcamento quando aprovado;
- pausar campanha quando aprovado;
- ativar campanha quando aprovado;
- excluir placement quando aprovado.

## Regras

- Preview nao aplica alteracoes.
- Toda aplicacao precisa postar resultado.
- Se endpoint falhar, registrar erro.
- Nao marcar como sucesso sem confirmacao.

