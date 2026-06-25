# Exclusao de Placements e Canais

## Fluxo

1. Coletar os canais/placements de ontem.
2. Ler URL, nome, tipo, campanha e grupo.
3. Classificar relacao com o tema da URL final.
4. Separar somente itens claramente irrelevantes.
5. Registrar evidencia e motivo.
6. Enviar para aprovacao humana.
7. Criar ou reutilizar lista de exclusao.
8. Vincular a lista a campanha/grupo correto.
9. Confirmar na API que a exclusao foi aplicada.

## Seguranca

- Nao excluir por baixo desempenho sem volume minimo.
- Nao excluir canal apenas pelo nome quando a URL estiver ambigua.
- Nao duplicar exclusoes existentes.
- Demand Gen deve respeitar o nivel de grupo.
- Toda exclusao registra antes/depois, horario e responsavel.

## Interface

Mostrar:

- canal;
- campanha e grupo;
- motivo;
- classificacao;
- status pendente/aplicado/erro;
- lista vinculada.
