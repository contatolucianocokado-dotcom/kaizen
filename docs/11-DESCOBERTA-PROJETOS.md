# Descoberta de Contas e Projetos

## Google Ads

Quando `fetch_all_accounts` estiver ativo:

1. Listar somente contas acessiveis pela MCC configurada.
2. Identificar contas e campanhas com investimento.
3. Criar ou atualizar o projeto pelo dominio real.
4. Manter nomes e IDs externos apenas no banco/config privado.

## Projeto

Um projeto deve existir quando houver:

- dominio configurado; ou
- campanha com investimento associada ao dominio; ou
- GAM mapeado ao dominio.

Campanhas sem dados em todos os periodos podem ficar ocultas, mas nao devem ser apagadas automaticamente.

## GAM

Cada dominio precisa indicar seu proprio network e moeda. O sistema nao deve inferir um GAM apenas por nome parecido.

## Validacao

- Conta nova aparece no setup e Campaign Builder.
- Projeto novo aparece após o primeiro investimento.
- Campanhas novas aparecem no nivel correto de Attr ID.
- Nenhum projeto ficticio e criado por nome parcial.
