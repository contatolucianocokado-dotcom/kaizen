# Operacao

## Rotina diaria

1. Conferir se Google Ads bate com plataforma.
2. Conferir se GAM bate com plataforma.
3. Conferir se campanhas com investimento aparecem.
4. Conferir se UTMs chegaram no GAM.
5. Conferir se receita esta no attr ID correto.
6. Conferir incidentes abertos.
7. Conferir se houve 502/504.
8. Conferir acoes pendentes de CPA, CPC, orcamento e status.

## Teste obrigatorio apos alteracao

```bash
curl -I https://app.seudominio.com/health
python3 scripts/campaign_type_data_agents.py --fix
python3 scripts/repair_gam_attr_from_raw.py --dry-run
```

## Quando nao tomar decisao automatica

- Ads desatualizado.
- GAM sem exportacao recente.
- Receita sem attr ID.
- UTM divergente.
- Campanha nova sem dados suficientes.
- Conta suspensa.
- PMR/receita incoerente entre campanha e blocos.

