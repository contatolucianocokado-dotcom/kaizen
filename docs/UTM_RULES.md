# Regras de UTM

## Regra principal

UTM e dado de origem. O sistema so le.

## Conta

```text
{lpurl}?utm_source=googleads&utm_medium={network}&utm_campaign={campaignid}&utm_term={campaignid}_{keyword}&utm_content={campaignid}_{placement}&utm_creative={campaignid}_{creative}
```

## Grupo

```text
{lpurl}?utm_source=googleads&utm_medium={network}&utm_campaign={adgroupid}&utm_term={adgroupid}_{keyword}&utm_content={adgroupid}_{placement}&utm_creative={adgroupid}_{creative}
```

## Validacao

- Se chegou campanha, registrar campanha.
- Se chegou grupo, registrar grupo.
- Se chegou outro ID, alertar.
- Se nao chegou ID, nao atribuir receita.

