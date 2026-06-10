# Floor Price

## Estado recomendado

Aplicacao automatica de floor deve ficar desligada quando houver instabilidade de dados.

## Regra

- Floor nao pode interferir na UTM.
- Floor nao pode alterar receita.
- Floor nao pode ser aplicado antes da validacao dos dados.
- Floor manual deve registrar valor anterior, valor novo e horario.

## Reativacao

Antes de reativar:

1. Validar GAM.
2. Validar UTM.
3. Validar PMR por bloco.
4. Validar eCPM por bloco.
5. Rodar simulacao.
6. Aprovar explicitamente.

