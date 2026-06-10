# Arquitetura

## Fluxo principal

```text
Google Ads
  -> UTM real no clique
  -> Site
  -> GPT/GAM recebe chaves-valor
  -> Relatorios GAM
  -> Sync Kaizen
  -> Banco
  -> Painel
  -> Agentes validam
```

## Componentes

- Aplicacao web: painel operacional.
- Banco: armazena snapshots, metricas, acoes e incidentes.
- Google Ads Script: envia snapshots e aplica acoes pendentes.
- Google Ads API: auditoria e operacoes quando OAuth estiver disponivel.
- GAM: fonte da receita, PMR, eCPM e blocos.
- Agentes: validacao, consistencia, aprendizado e alertas.
- Graphify: mapa mental tecnico e operacional.

## Dados principais

- `ads_daily`: investimento por dia.
- `ads_hourly`: investimento por hora.
- `gam_attr_daily`: receita por attr ID.
- `gam_content_daily`: receita por bloco/conteudo.
- `google_ads_campaign_settings_daily`: configuracoes de campanhas e grupos.
- `nexus_agent_events` ou equivalente: eventos dos agentes.
- `operational_health_events`: saude operacional.

