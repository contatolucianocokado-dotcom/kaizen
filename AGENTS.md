# Agentes Kaizen

## Agentes recomendados

- `agente-utm-attrid`: valida UTMs, attr IDs e divergencias de atribuicao.
- `agente-gam`: valida receita, blocos, PMR, eCPM e exportacoes do GAM.
- `agente-google-ads-script`: valida execucao do Apps Script e snapshots.
- `agente-consistencia-dados`: compara Google Ads, GAM e plataforma.
- `agente-campanhas-novas`: identifica campanhas novas com investimento.
- `agente-orcamento-cpa`: acompanha ajustes de orcamento, CPA, CPC e status.
- `agente-saude-web`: monitora 502, 504, lentidao e falhas de rota.
- `agente-aprendizado`: registra erros, padroes e melhorias.

## Autoridade

- Nivel 5: orquestrador, decisao final humana.
- Nivel 4: saude web e UTM podem bloquear decisao com dados ruins.
- Nivel 3: GAM, Google Ads e consistencia podem abrir incidentes criticos.
- Nivel 2: orcamento/CPA executa acoes preaprovadas.
- Nivel 1: auditoria e observacao.

## Regra de aprendizado

Cada erro deve gerar:

- causa;
- impacto;
- correcao;
- teste;
- aprendizado;
- regra para evitar recorrencia.

