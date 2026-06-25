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
- Campaign Builder: cria rascunhos e publica campanhas com validacao.
- Setup inicial: integra Ads, GA4, GTM, WordPress e Clarity.
- Jobs controlados: sincronizacao recente, fechamento e reconciliacao.
- Agentes: validacao, consistencia, aprendizado e alertas.
- Graphify: mapa mental tecnico e operacional.

## Dados principais

- `ads_daily`: investimento por dia.
- `ads_hourly`: investimento por hora.
- `gam_attr_daily`: receita por attr ID.
- `gam_content_daily`: receita por bloco/conteudo.
- `google_ads_campaign_settings_daily`: configuracoes de campanhas e grupos.
- `kaizen_agent_events` ou equivalente: eventos dos agentes.
- `operational_health_events`: saude operacional.

## Principios de robustez

- Dados recentes do Ads entram pelo Apps Script; API atua como auditoria/fallback.
- Receita por campanha somente entra quando o GAM fornece Attr ID confiavel.
- Jobs pesados usam lock, timeout e nunca rodam sobrepostos.
- Nenhuma rota web executa sincronizacao externa longa.
- Toda implementacao termina com teste objetivo e atualizacao do Graphify.
