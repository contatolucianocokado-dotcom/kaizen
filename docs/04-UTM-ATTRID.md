# UTM e Attr ID

## Regra intrinseca

UTM vem da origem. O sistema nao altera, nao corrige e nao inventa UTM.

## Modelos aceitos

Nivel de conta:

```text
{lpurl}?utm_source=googleads&utm_medium={network}&utm_campaign={campaignid}&utm_term={campaignid}_{keyword}&utm_content={campaignid}_{placement}&utm_creative={campaignid}_{creative}
```

Nivel de grupo:

```text
{lpurl}?utm_source=googleads&utm_medium={network}&utm_campaign={adgroupid}&utm_term={adgroupid}_{keyword}&utm_content={adgroupid}_{placement}&utm_creative={adgroupid}_{creative}
```

## Demand Gen

Demand Gen deve usar `adgroupId` quando a estrategia exige controle por grupo.

## Divergencias

Se Google Ads tem gasto e GAM nao tem receita:

- Verificar UTM real no trafego.
- Verificar se GAM recebeu a chave.
- Verificar se o projeto esta mapeado para o GAM correto.
- Registrar alerta para agente de dados.
