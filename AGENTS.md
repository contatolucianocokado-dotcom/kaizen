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

## Multi-agent quality loop

- Antes de criar novos agentes, sempre mapear os agentes existentes.
- Reutilizar agentes existentes sempre que possivel.
- Para tarefas complexas, usar um fluxo com orquestrador, executor, critico e refatorador.
- Nenhuma entrega estrategica deve ser considerada final antes de passar por uma avaliacao critica.
- O agente critico deve reprovar entregas genericas, incompletas, contraditorias ou desalinhadas com o objetivo.
- Quando o critico reprovar, o trabalho deve voltar para refacao com as falhas e acoes obrigatorias.
- O loop deve ter limite maximo configuravel, padrao 3 tentativas.
- A versao final deve indicar se foi aprovada ou se foi a melhor versao possivel apos o limite de tentativas.
- Toda rodada deve registrar status, nota, falhas e acoes de refacao.
- Nao criar arquitetura paralela se o projeto ja tiver pipeline ou agentes proprios.

## Saida estruturada do agente critico

O agente critico deve retornar um objeto estruturado, nao texto livre:

```json
{
  "status": "approved|rejected|needs_refactor",
  "nota": 0,
  "principal_problema": "",
  "falhas": [],
  "acoes_obrigatorias": [],
  "instrucao_para_refatorador": "",
  "pode_entregar_ao_usuario": false
}
```

O orquestrador deve usar esse objeto para decidir automaticamente:

- entregar quando `pode_entregar_ao_usuario` for verdadeiro;
- chamar o refatorador quando `status` exigir refacao;
- encerrar por limite maximo de tentativas quando o loop atingir o limite configurado.

Nao depender de parsing fragil de texto livre quando for possivel usar schema, tipo, interface ou validacao.

## Registro obrigatorio

- Toda entrega deve registrar o que mudou, por que mudou e quais agentes validaram.
- Todo erro recorrente deve virar regra permanente para evitar repeticao.
- Registrar somente conhecimento operacional sanitizado.
- Nunca registrar tokens, senhas, OAuth codes, cookies, IPs privados, credenciais WordPress, IDs sensiveis ou screenshots com dados sensiveis.

