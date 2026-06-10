# Floor Price

## Estado seguro inicial

Automacao de floor deve iniciar desativada:

```yaml
rules:
  floor_automation_enabled: false
```

## Manual primeiro

Antes de automatizar:

1. Criar key-value no GAM.
2. Criar regras de pricing.
3. Validar relatorios.
4. Confirmar PMR/eCPM por bloco.
5. Testar impacto com holdout.

## Cuidado

Floor aplicado errado pode reduzir receita. Qualquer automacao precisa validar:

- PMR.
- eCPM.
- Receita.
- Impressoes desde a ultima alteracao.
- Comparacao contra controle.
