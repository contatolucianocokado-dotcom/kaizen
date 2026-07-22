# Regras do Sistema Kaizen

## UTM

- UTM e dado de origem.
- O sistema nunca altera `utm_campaign`.
- O sistema nunca remapeia receita GAM de um ID para outro.
- Receita so entra no `attr_id` que realmente veio no GAM.
- Se Google Ads envia em nivel de campanha, usa campanha.
- Se Google Ads envia em nivel de grupo, usa grupo.
- Se GAM recebe outro ID, o sistema alerta, mas nao corrige silenciosamente.
- Se GAM nao recebe ID atribuivel, a receita fica sem campanha e um incidente e aberto.

## Google Ads

- Campanhas com investimento devem aparecer no sistema.
- Campanhas novas devem entrar automaticamente quando tiverem investimento.
- Orcamento, CPA, CPC e status devem vir do Google Ads ou do Apps Script.
- Alteracoes manuais devem ficar registradas com valor anterior, valor novo e horario de Brasilia.
- Acoes pendentes so devem ser consideradas aplicadas apos confirmacao do Google Ads.
- Se nao houver snapshot recente do Google Ads, qualquer decisao automatica deve ser bloqueada.
- Criacao, pausa, ativacao, orcamento, CPA, CPC e placement devem ser reconciliados no proximo snapshot: valor esperado contra valor real.
- Links operacionais do Google Ads devem abrir a campanha especifica usando conta e campanha corretas.

## GAM

- Receita deve ser atribuida pelo ID real recebido nas chaves-valor.
- Blocos de anuncio devem trazer receita, impressoes, cliques, PMR e eCPM.
- Divergencia entre resumo de campanha e blocos deve gerar alerta.
- Se o GAM nao exportar dados recentes, o sistema deve avisar e nao tomar decisao automatica.
- Receita, eCPM e floor devem preservar a moeda original e informar conversao quando houver.
- Dados de um projeto, campanha, attr ID, placement ou bloco nunca podem ser reaproveitados em outro sem chave real correspondente.

## Floor

- Aplicacao automatica de floor deve ficar desligada quando houver instabilidade.
- Qualquer reativacao precisa de aprovacao explicita.
- Floor manual pode ser registrado, mas nao deve reescrever UTM ou receita.

## Agentes

- Todo erro recorrente deve ser atribuido a um agente responsavel.
- Agentes devem registrar causa, impacto, correcao e aprendizado.
- Agentes nao podem alterar regra de negocio sem validacao.
- Todo teste apos implementacao e obrigatorio.
- Agentes de tracking devem validar Google Ads, GAM, GA4 e Clarity antes de liberar ROI, RPS ou funil.
- Agentes de UX, copy, CRO, tecnico e compliance devem validar mudancas estrategicas de funil antes da entrega.

## Saude de dados

- Cada metrica critica deve exibir origem e freshness.
- Fonte desatualizada bloqueia metricas derivadas dependentes dela.
- Fallback deve ser explicito e nunca esconder erro real.
- Sincronizacoes devem registrar quantidade de linhas, periodo, fonte e falha quando ocorrer.

## Horario

- Toda exibicao e todo registro operacional deve usar horario de Brasilia.
