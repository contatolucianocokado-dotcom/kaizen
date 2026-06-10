# Troubleshooting

## Investimento nao aparece

- Rodar Google Ads Script.
- Verificar se a conta esta em `config.yaml`.
- Verificar `customer_id`.
- Verificar snapshots recentes.

## Receita nao aparece

- Verificar GAM correto do dominio.
- Verificar `utm_campaign`.
- Verificar relatorio por chave-valor.
- Verificar se o Attr ID existe no GAM.

## PMR igual em todas as campanhas

Provavel agregacao errada. PMR deve ser calculado por Attr ID/bloco quando houver dado suficiente.

## Blocos nao carregam

- Verificar endpoint de blocos.
- Verificar se existe dado GAM por Attr ID.
- Verificar timeout da query.
- Verificar se a campanha usa UTM por campanha ou grupo.

## 502 ou 504

- Verificar processo web.
- Verificar query pesada.
- Verificar Nginx timeout.
- Verificar logs do app.

## Depois de qualquer correcao

Sempre rodar teste de validacao e registrar resultado.
