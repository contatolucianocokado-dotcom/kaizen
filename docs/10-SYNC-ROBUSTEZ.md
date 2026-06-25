# Sincronizacao e Robustez

## Separacao de cargas

- Hoje Ads: Google Ads Script.
- Hoje GAM por Attr ID: job rapido dedicado.
- Ontem, 7 dias e mes: fechamento separado.
- Historico, blocos e relatorios secundarios: full sync diario.

## Regras

- Usar lock para impedir sobreposicao.
- Aplicar timeout por job.
- Nunca manter transacao SQLite aberta durante chamada externa.
- Nunca rodar sync pesado dentro de rota web.
- Preservar ultimo snapshot valido quando a API falhar.
- Falha parcial de um network nao pode ser marcada como sucesso total.

## Reconciliacao

Comparar por conta, projeto e dia:

- custo Google Ads;
- custo salvo;
- receita GAM;
- receita atribuida por Attr ID;
- totais do painel.

Numeros financeiros devem ser exatos. Divergencia precisa indicar fonte, periodo e valor.

## Saude

Validar:

- endpoint de health;
- processo web;
- tempo do job;
- memoria;
- banco bloqueado;
- ultima atualizacao de Ads e GAM.

Erros 502, 504, OAuth revogado e `database is locked` precisam de causa raiz, nao apenas restart.
