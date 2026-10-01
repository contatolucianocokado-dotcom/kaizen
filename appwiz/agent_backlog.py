from __future__ import annotations

from typing import Any


AGENT_BACKLOG: list[dict[str, Any]] = [
    {
        "slug": "governanca-dados",
        "name": "Nexus Data Guardian",
        "epic": "Dados e Governanca",
        "mission": "Garantir que o sistema opere apenas com dados confiaveis, reconciliados e auditaveis.",
        "function": [
            "Validar ingestao de Ads, GAM, monetizacao e conversao.",
            "Detectar divergencias entre fonte oficial e leitura operacional.",
            "Bloquear dado inconsistente antes de contaminar decisao e aprendizado.",
        ],
        "deliveries": [
            "Health check por fonte e periodo.",
            "Centro de validacao de dados no site.",
            "Reconcilacao por projeto, campanha e monetizacao.",
            "Quarentena de dados suspeitos.",
        ],
        "metrics": [
            "Taxa de divergencia por fonte.",
            "Tempo para detectar erro.",
            "Tempo para reconciliar erro.",
            "Percentual de dados aprovados sem ressalva.",
        ],
        "continuous_improvement": [
            "Estudar padroes novos de divergencia.",
            "Melhorar regras de reconcilacao.",
            "Reduzir falso positivo e falso negativo.",
            "Aprender com falhas de ingestao e atrasos de API.",
        ],
        "technical_backlog": [
            {"title": "Criar source_sync_runs", "priority": "Alta", "expected": "Historico de cada sincronizacao por fonte."},
            {"title": "Criar reconciliation_checks", "priority": "Alta", "expected": "Comparacao oficial x operacional por projeto e campanha."},
            {"title": "Criar data_health_events", "priority": "Alta", "expected": "Alertas de divergencia, atraso e dado parcial."},
            {"title": "Tela Saude de Dados", "priority": "Alta", "expected": "Visao central de qualidade do dado."},
        ],
    },
    {
        "slug": "tracking-utms",
        "name": "Nexus Tracking Guardian",
        "epic": "Dados e Governanca",
        "mission": "Garantir identidade perfeita de trafego, oferta, criativo, funil e sessao.",
        "function": [
            "Padronizar UTMs.",
            "Validar naming por canal e pais.",
            "Resolver identidade operacional entre plataformas.",
        ],
        "deliveries": [
            "Gerador de UTM.",
            "Registry de UTMs validas.",
            "Parser e validador de parametros.",
            "Bloqueio de campanha fora do padrao.",
        ],
        "metrics": [
            "Percentual de campanhas com UTM valida.",
            "Percentual de sessoes atribuidas corretamente.",
            "Taxa de erro de naming.",
            "Taxa de perda de contexto por click/session.",
        ],
        "continuous_improvement": [
            "Estudar naming que mais quebra atribuicao.",
            "Propor campos novos de identificacao.",
            "Ajustar padroes por canal e pais.",
            "Reduzir ambiguidade entre campanhas parecidas.",
        ],
        "technical_backlog": [
            {"title": "Criar utm_registry", "priority": "Alta", "expected": "Cadastro unico de UTMs validas."},
            {"title": "Definir padrao oficial de UTM", "priority": "Alta", "expected": "Naming por canal, pais, oferta, funil e criativo."},
            {"title": "Gerar IDs internos", "priority": "Alta", "expected": "campaign_key, creative_key, landing_key e offer_key."},
            {"title": "Criar wizard de links", "priority": "Media", "expected": "Criacao assistida de links rastreaveis."},
        ],
    },
    {
        "slug": "painel-operacional",
        "name": "Nexus Operations Agent",
        "epic": "Painel e Experiencia",
        "mission": "Exibir a operacao inteira no site de forma correta, atualizada e acionavel.",
        "function": [
            "Consolidar metricas por projeto, campanha e monetizacao.",
            "Servir como frente principal do sistema.",
            "Refletir a verdade operacional validada.",
        ],
        "deliveries": [
            "Home executiva.",
            "Paginas especificas por projeto.",
            "Botões ATUALIZAR HOJE e ATUALIZAR TUDO.",
            "Status de atualizacao e execucao.",
        ],
        "metrics": [
            "Consistencia home x pagina especifica.",
            "Tempo de atualizacao percebido.",
            "Tempo de resposta das rotas.",
            "Taxa de erro da interface operacional.",
        ],
        "continuous_improvement": [
            "Estudar onde o usuario mais navega.",
            "Detectar telas que geram mais duvida.",
            "Reduzir tempo para achar problema.",
            "Melhorar leitura de prioridades.",
        ],
        "technical_backlog": [
            {"title": "Consolidar home com fonte oficial validada", "priority": "Alta", "expected": "Home e pagina do projeto com mesmo dado oficial."},
            {"title": "Criar status visual de fast/full", "priority": "Alta", "expected": "Atualizacao com feedback em tempo real."},
            {"title": "Criar pagina de validacao de dados", "priority": "Alta", "expected": "Visao central das reconciliacoes."},
            {"title": "Criar centro de recomendacoes", "priority": "Media", "expected": "Fila do que fazer agora."},
        ],
    },
    {
        "slug": "ux",
        "name": "Nexus UX Agent",
        "epic": "Painel e Experiencia",
        "mission": "Tornar o sistema rapido de entender e dificil de interpretar errado.",
        "function": [
            "Definir hierarquia visual.",
            "Organizar densidade de informacao.",
            "Melhorar navegacao e clareza de decisao.",
        ],
        "deliveries": [
            "Design system operacional.",
            "Paletas semanticas para investimento, receita, ROI, risco e confianca.",
            "Componentes de tabela, card, badge e banner.",
            "Padroes de loading, erro, vazio e divergencia.",
        ],
        "metrics": [
            "Tempo medio para encontrar problema.",
            "Quantidade de cliques ate a acao principal.",
            "Taxa de uso dos filtros e acoes principais.",
            "Reducao de duvidas recorrentes.",
        ],
        "continuous_improvement": [
            "Estudar onde o operador hesita.",
            "Testar layouts e densidades.",
            "Revisar legibilidade em desktop e mobile.",
            "Refinar a relacao entre analise e acao.",
        ],
        "technical_backlog": [
            {"title": "Criar design system", "priority": "Alta", "expected": "Padrao unico de tipografia, cor e componentes."},
            {"title": "Definir paletas semanticas", "priority": "Alta", "expected": "Leitura rapida de investimento, receita, ROI e risco."},
            {"title": "Redesenhar cards de home e projeto", "priority": "Alta", "expected": "Hoje, Ontem, 7 dias e Mes mais claros."},
            {"title": "Criar breadcrumbs e retorno contextual", "priority": "Media", "expected": "Menos perda de contexto."},
        ],
    },
    {
        "slug": "assistente-chat",
        "name": "Nexus Copilot",
        "epic": "Painel e Experiencia",
        "mission": "Ser o copiloto contextual do sistema.",
        "function": [
            "Explicar numeros.",
            "Responder duvidas com contexto.",
            "Justificar recomendacoes.",
            "Ajudar a agir mais rapido.",
        ],
        "deliveries": [
            "Chat contextual.",
            "Perguntas sugeridas por tela.",
            "Respostas ligadas aos dados reais do painel.",
            "Acoes assistidas com confirmacao.",
        ],
        "metrics": [
            "Taxa de resolucao de duvida.",
            "Tempo reduzido para decisao.",
            "Uso das sugestoes rapidas.",
            "Taxa de satisfacao operacional.",
        ],
        "continuous_improvement": [
            "Estudar duvidas mais frequentes.",
            "Melhorar clareza das respostas.",
            "Melhorar contexto por pagina.",
            "Refinar as acoes que realmente ajudam.",
        ],
        "technical_backlog": [
            {"title": "Criar drawer lateral de chat", "priority": "Media", "expected": "Chat embutido no painel."},
            {"title": "Criar contexto por rota", "priority": "Alta", "expected": "Chat entende projeto, periodo e filtros atuais."},
            {"title": "Integrar com servicos internos", "priority": "Alta", "expected": "Respostas fundamentadas nos dados do sistema."},
            {"title": "Modo acao assistida", "priority": "Media", "expected": "Chat sugere e pede confirmacao antes de executar."},
        ],
    },
    {
        "slug": "trafego-pago",
        "name": "Nexus Traffic Agent",
        "epic": "Aquisicao e Monetizacao",
        "mission": "Maximizar aquisicao qualificada com foco em lucro e escala saudavel.",
        "function": [
            "Gerir CPC, CPA, orcamento, pausa, escala e duplicacao.",
            "Operar Google, Meta, TikTok, Push e Email.",
        ],
        "deliveries": [
            "Ranking de campanhas por rentabilidade.",
            "Sugestao de pausa e escala.",
            "Sugestao de orcamento.",
            "Criacao e duplicacao assistida de campanhas.",
        ],
        "metrics": [
            "Lucro por canal.",
            "ROAS real.",
            "CAC.",
            "Taxa de acerto de escala.",
        ],
        "continuous_improvement": [
            "Revisar decisoes de pausa e escala.",
            "Encontrar novos sinais de winners.",
            "Estudar padroes por pais, dispositivo e criativo.",
            "Refinar thresholds de CPC e CPA.",
        ],
        "technical_backlog": [
            {"title": "Consolidar conector Google Ads", "priority": "Alta", "expected": "Custo oficial por campanha e projeto."},
            {"title": "Modelar campanhas por canal", "priority": "Alta", "expected": "Visao por Google, Meta, TikTok, Push e Email."},
            {"title": "Criar regras de pausa e escala", "priority": "Media", "expected": "Recomendacoes acionaveis."},
            {"title": "Factory de campanhas", "priority": "Media", "expected": "Criacao e duplicacao inteligente."},
        ],
    },
    {
        "slug": "monetizacao",
        "name": "Nexus Monetization Agent",
        "epic": "Aquisicao e Monetizacao",
        "mission": "Maximizar receita e margem por pagina, bloco e contexto.",
        "function": [
            "Controlar pricing, eCPM, fill, match rate e mix de monetizacao.",
            "Orquestrar GAM, AdSense e monetizacoes similares.",
        ],
        "deliveries": [
            "Controle de regra de preco.",
            "Catalogo de degraus.",
            "Estado atual por bloco.",
            "Sugestao de subir, baixar ou manter.",
        ],
        "metrics": [
            "eCPM.",
            "RPM.",
            "Match rate.",
            "Receita por bloco.",
        ],
        "continuous_improvement": [
            "Estudar sensibilidade dos blocos.",
            "Revisar regras por nicho e pais.",
            "Aprender com outcomes dos degraus.",
            "Melhorar o equilibrio entre volume e preco.",
        ],
        "technical_backlog": [
            {"title": "Consolidar pricing por bloco e projeto", "priority": "Alta", "expected": "Visao unica de regra ativa e impacto."},
            {"title": "Criar tela de controle de regra de preco", "priority": "Alta", "expected": "Subida/descida de degrau com contexto."},
            {"title": "Criar simulador de impacto de pricing", "priority": "Media", "expected": "Efeito esperado antes da mudanca."},
            {"title": "Criar fila de sincronizacao com GAM", "priority": "Media", "expected": "Aplicacao controlada no ambiente de monetizacao."},
        ],
    },
    {
        "slug": "ofertas-receita",
        "name": "Nexus Revenue Agent",
        "epic": "Aquisicao e Monetizacao",
        "mission": "Selecionar a melhor combinacao entre oferta, pagina, trafego e modelo de receita.",
        "function": [
            "Comparar afiliados, leads e produtos digitais.",
            "Priorizar a receita mais rentavel por contexto.",
        ],
        "deliveries": [
            "Registry de ofertas.",
            "Ranking por EPC e lucro.",
            "Score de aprovacao e estorno.",
            "Sugestao de troca de oferta.",
        ],
        "metrics": [
            "EPC.",
            "Receita liquida por oferta.",
            "Taxa de aprovacao.",
            "Lucro por modelo de monetizacao.",
        ],
        "continuous_improvement": [
            "Revisar sazonalidade de oferta.",
            "Detectar fadiga de oferta.",
            "Melhorar a escolha por canal e pais.",
            "Encontrar novos mixes de receita.",
        ],
        "technical_backlog": [
            {"title": "Modelar catalogo de ofertas", "priority": "Media", "expected": "Registry de ofertas e payouts."},
            {"title": "Integrar afiliados e checkouts", "priority": "Media", "expected": "Receita real por oferta."},
            {"title": "Criar score de EPC e margem", "priority": "Media", "expected": "Ranking de ofertas."},
            {"title": "Comparar mix de monetizacao por projeto", "priority": "Media", "expected": "Melhor combinacao para lucro."},
        ],
    },
    {
        "slug": "funil",
        "name": "Nexus Funnel Agent",
        "epic": "Funil e Conteudo",
        "mission": "Aumentar avanco entre etapas e conversao final.",
        "function": [
            "Detectar gargalos.",
            "Medir abandono.",
            "Sugerir melhorias estruturais no funil.",
        ],
        "deliveries": [
            "Mapa de funil.",
            "Leitura por etapa.",
            "Sugestoes de melhoria.",
            "Ranking de gargalos.",
        ],
        "metrics": [
            "Taxa por etapa.",
            "Taxa de abandono.",
            "Conversao final.",
            "Valor por sessao.",
        ],
        "continuous_improvement": [
            "Estudar fuga por etapa.",
            "Revisar friccoes recorrentes.",
            "Identificar etapas dispensaveis.",
            "Refinar hipoteses de melhoria.",
        ],
        "technical_backlog": [
            {"title": "Modelar etapas do funil", "priority": "Media", "expected": "Eventos e passos estruturados."},
            {"title": "Medir abandono por etapa", "priority": "Media", "expected": "Gargalos identificaveis."},
            {"title": "Criar tela de funil por projeto", "priority": "Media", "expected": "Leitura de avanco e fuga."},
            {"title": "Integrar com experimentacao", "priority": "Baixa", "expected": "Testar mudancas de funil."},
        ],
    },
    {
        "slug": "conteudo-criativos",
        "name": "Nexus Creative Agent",
        "epic": "Funil e Conteudo",
        "mission": "Descobrir e replicar combinacoes vencedoras de copy e criativo.",
        "function": [
            "Organizar biblioteca de assets.",
            "Medir desempenho por angulo, headline, descricao e criativo.",
            "Sugerir novas variacoes.",
        ],
        "deliveries": [
            "Biblioteca de criativos.",
            "Biblioteca de copy.",
            "Ranking de winners.",
            "Sugestao de variacoes.",
        ],
        "metrics": [
            "CTR.",
            "CVR.",
            "Receita por criativo.",
            "Durabilidade do asset.",
        ],
        "continuous_improvement": [
            "Estudar fadiga criativa.",
            "Mapear padroes de winners.",
            "Melhorar agrupamento por tema e promessa.",
            "Refinar sugestoes de variacoes.",
        ],
        "technical_backlog": [
            {"title": "Criar biblioteca de criativos", "priority": "Media", "expected": "Acervo centralizado."},
            {"title": "Criar biblioteca de copies", "priority": "Media", "expected": "Headlines, descriptions e angulos organizados."},
            {"title": "Medir performance por asset", "priority": "Media", "expected": "Ranking de winners."},
            {"title": "Clusterizar padroes vencedores", "priority": "Baixa", "expected": "Mapa de angulos que funcionam."},
        ],
    },
    {
        "slug": "experimentacao",
        "name": "Nexus Experiment Agent",
        "epic": "Funil e Conteudo",
        "mission": "Validar hipoteses com metodo e reduzir mudancas aleatorias.",
        "function": [
            "Criar, medir e encerrar testes A/B.",
            "Promover vencedores com seguranca.",
        ],
        "deliveries": [
            "Cadastro de experimento.",
            "Baseline.",
            "Grupo controle.",
            "Relatorio final.",
        ],
        "metrics": [
            "Taxa de testes vencedores.",
            "Uplift medio.",
            "Tempo medio por teste.",
            "Valor incremental gerado.",
        ],
        "continuous_improvement": [
            "Revisar quais testes geram mais valor.",
            "Reduzir testes irrelevantes.",
            "Melhorar criterios de vitoria.",
            "Aprender quais hipoteses funcionam por contexto.",
        ],
        "technical_backlog": [
            {"title": "Modelar experimento", "priority": "Media", "expected": "Cadastro padronizado de teste."},
            {"title": "Baseline e grupo controle", "priority": "Media", "expected": "Comparacao valida."},
            {"title": "Tela de experimentos", "priority": "Baixa", "expected": "Gestao visual de testes."},
            {"title": "Promocao assistida do vencedor", "priority": "Baixa", "expected": "Rollout controlado."},
        ],
    },
    {
        "slug": "aprendizado",
        "name": "Nexus Learning Agent",
        "epic": "Inteligencia e Controle",
        "mission": "Aprender com contexto, acao e resultado para melhorar decisoes futuras.",
        "function": [
            "Registrar snapshots.",
            "Registrar acoes.",
            "Registrar outcomes.",
            "Calcular probabilidade e reward esperado.",
        ],
        "deliveries": [
            "policy_learning.",
            "Score de confianca.",
            "Reward esperado.",
            "Ranking de acoes por contexto.",
        ],
        "metrics": [
            "Taxa de acerto preditivo.",
            "Reward medio por politica.",
            "Drift de modelo.",
            "Cobertura de contexto.",
        ],
        "continuous_improvement": [
            "Revisar features uteis.",
            "Recalibrar probabilidades.",
            "Detectar drift.",
            "Melhorar reward para refletir lucro real.",
        ],
        "technical_backlog": [
            {"title": "Consolidar snapshots, actions e outcomes", "priority": "Alta", "expected": "Memoria operacional coerente."},
            {"title": "Melhorar reward economico", "priority": "Alta", "expected": "Score orientado a lucro."},
            {"title": "Calibrar probability_success", "priority": "Media", "expected": "Recomendacoes mais confiaveis."},
            {"title": "Painel de policy learning", "priority": "Media", "expected": "Visibilidade do aprendizado."},
        ],
    },
    {
        "slug": "orquestrador",
        "name": "Nexus Orchestrator",
        "epic": "Inteligencia e Controle",
        "mission": "Priorizar o que deve ser feito agora para aumentar lucro com menor risco.",
        "function": [
            "Conciliar agentes.",
            "Ordenar prioridades.",
            "Decidir fila operacional.",
        ],
        "deliveries": [
            "Fila priorizada de acoes.",
            "Score de urgencia.",
            "Score de impacto.",
            "Score de risco.",
        ],
        "metrics": [
            "Lucro incremental esperado.",
            "Taxa de acerto das prioridades.",
            "Tempo ate execucao relevante.",
            "Conflitos evitados.",
        ],
        "continuous_improvement": [
            "Revisar se priorizou bem.",
            "Melhorar pesos de urgencia, impacto e risco.",
            "Estudar decisoes priorizadas errado.",
            "Melhorar uso do capital operacional.",
        ],
        "technical_backlog": [
            {"title": "Criar score de urgencia", "priority": "Media", "expected": "Fila do que fazer agora."},
            {"title": "Criar score de impacto", "priority": "Media", "expected": "Priorizacao economica."},
            {"title": "Criar score de risco", "priority": "Media", "expected": "Execucao mais segura."},
            {"title": "Montar fila operacional", "priority": "Alta", "expected": "Centro de decisoes."},
        ],
    },
    {
        "slug": "auditor",
        "name": "Nexus Auditor",
        "epic": "Inteligencia e Controle",
        "mission": "Garantir seguranca, conformidade e estabilidade da operacao.",
        "function": [
            "Revisar execucao.",
            "Detectar risco.",
            "Disparar rollback e bloqueios.",
        ],
        "deliveries": [
            "Trilha de auditoria.",
            "Relatorio de incidentes.",
            "Alertas de risco.",
            "Kill switch por modulo.",
        ],
        "metrics": [
            "Incidentes evitados.",
            "Tempo de resposta a falhas.",
            "Taxa de rollback bem-sucedido.",
            "Violacao de politicas.",
        ],
        "continuous_improvement": [
            "Revisar falhas e quase-falhas.",
            "Endurecer guardrails quando necessario.",
            "Aliviar guardrails excessivos.",
            "Melhorar equilibrio entre seguranca e velocidade.",
        ],
        "technical_backlog": [
            {"title": "Criar audit_log", "priority": "Alta", "expected": "Trilha completa de acao."},
            {"title": "Criar regras de rollback", "priority": "Alta", "expected": "Seguranca operacional."},
            {"title": "Criar kill switch por modulo", "priority": "Media", "expected": "Congelamento rapido em incidente."},
            {"title": "Painel de auditoria", "priority": "Baixa", "expected": "Governanca visual."},
        ],
    },
    {
        "slug": "expansao-internacional",
        "name": "Nexus Expansion Agent",
        "epic": "Escala",
        "mission": "Encontrar onde e como replicar winners em novos paises.",
        "function": [
            "Avaliar replicabilidade.",
            "Sugerir pais, oferta e adaptacao.",
            "Medir risco de expansao.",
        ],
        "deliveries": [
            "Lista de candidatos a expansao.",
            "Score de similaridade entre paises.",
            "Sugestao de adaptacao de copy e funil.",
            "Priorizacao de rollout.",
        ],
        "metrics": [
            "Taxa de sucesso por expansao.",
            "Tempo para viabilidade.",
            "Lucro por expansao.",
            "Custo de adaptacao.",
        ],
        "continuous_improvement": [
            "Revisar expansoes que falharam.",
            "Melhorar score de similaridade.",
            "Aprender diferencas culturais e de oferta.",
            "Refinar criterios de entrada em novo pais.",
        ],
        "technical_backlog": [
            {"title": "Modelar paises-alvo e perfis", "priority": "Baixa", "expected": "Base de expansao."},
            {"title": "Criar score de similaridade", "priority": "Baixa", "expected": "Replicacao menos intuitiva e mais cientifica."},
            {"title": "Adaptar copy e oferta por pais", "priority": "Baixa", "expected": "Rollout mais aderente."},
            {"title": "Criar tela de expansao", "priority": "Baixa", "expected": "Fila de expansao."},
        ],
    },
    {
        "slug": "sinais-comerciais-editorial",
        "name": "Julio Commercial Context Agent",
        "epic": "Conteudo e Monetizacao",
        "mission": "Aproximar anuncios de contexto editorial comercial real sem fabricar sinais ou comprometer a experiencia.",
        "function": ["Mapear intencao e entidades por etapa.", "Aplicar taxonomia IAB compativel.", "Manter anuncios a pelo menos 180px de botoes, links, campos e seletores."],
        "deliveries": ["Mapa de sinais por P2/P3.", "Recomendacao contextual por bloco.", "Registro de risco de politica."],
        "metrics": ["RPS por etapa.", "eCPM por URL.", "Reprovacoes por sinal artificial."],
        "continuous_improvement": ["Comparar RPS antes e depois.", "Aprender quais contextos preservam CRO.", "Remover sinais sem relacao com a dor."],
        "technical_backlog": [{"title": "Auditoria contextual P2+", "priority": "Alta", "expected": "Sinais comerciais naturais e verificaveis por etapa."}],
    },
    {
        "slug": "critico-sinais-comerciais",
        "name": "Julio Commercial Signals Critic",
        "epic": "Conteudo e Monetizacao",
        "mission": "Reprovar sinais artificiais, promessas sem base e qualquer otimizacao que reduza utilidade ou viole politicas.",
        "function": ["Avaliar saida estruturada.", "Bloquear keyword stuffing e clique acidental.", "Reprovar anuncio a menos de 180px de qualquer controle interativo."],
        "deliveries": ["Nota de 0 a 100.", "Falhas e acoes obrigatorias.", "Decisao aprovar ou refazer."],
        "metrics": ["Conteudos aprovados com nota 95+.", "Incidentes de policy.", "RPS sem queda de conversao."],
        "continuous_improvement": ["Revisar falsos positivos.", "Comparar comportamento Clarity.", "Elevar profundidade sem inflar texto."],
        "technical_backlog": [{"title": "Gate comercial estruturado", "priority": "Alta", "expected": "Nenhuma publicacao estrategica sem critica 95+."}],
    },
    {
        "slug": "conteudo-alto-valor",
        "name": "Julio High Value Content Agent",
        "epic": "Conteudo e Monetizacao",
        "mission": "Entregar solucao completa por momento cognitivo, incluindo passos, documentos, excecoes e fontes.",
        "function": ["Construir guias executaveis.", "Resolver casos alternativos.", "Manter fontes oficiais atuais."],
        "deliveries": ["P3 aprofundadas.", "Fluxos visuais.", "Checklists e proximos passos."],
        "metrics": ["Progressao P3 para acao.", "Scroll util.", "Retorno para etapa anterior."],
        "continuous_improvement": ["Usar Clarity e Analytics.", "Revisar duvidas reais.", "Atualizar procedimentos oficiais."],
        "technical_backlog": [{"title": "Padrao P3 95+", "priority": "Alta", "expected": "Conteudo executavel e especifico."}],
    },
    {
        "slug": "critico-conteudo",
        "name": "Julio High Value Content Critic",
        "epic": "Conteudo e Monetizacao",
        "mission": "Impedir publicacao de respostas rasas, genericas ou sem solucao verificavel.",
        "function": ["Pontuar profundidade.", "Validar fontes e excecoes.", "Enviar falhas ao refatorador."],
        "deliveries": ["Avaliacao estruturada.", "Lista obrigatoria de refacao.", "Aprovacao somente com nota 95+."],
        "metrics": ["Nota media publicada.", "Reprovacoes corrigidas.", "Reducao de quickback."],
        "continuous_improvement": ["Calibrar nota com dados reais.", "Rever conteudos apos mudancas oficiais.", "Evitar volume sem utilidade."],
        "technical_backlog": [{"title": "Quality gate editorial", "priority": "Alta", "expected": "Entrega final validada por schema."}],
    },
]


def epic_summary() -> list[dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {}
    for agent in AGENT_BACKLOG:
        epic = str(agent["epic"])
        item = summary.setdefault(
            epic,
            {
                "epic": epic,
                "agents": 0,
                "high_priority_tasks": 0,
                "total_tasks": 0,
            },
        )
        item["agents"] += 1
        tasks = agent.get("technical_backlog") or []
        item["total_tasks"] += len(tasks)
        item["high_priority_tasks"] += sum(1 for task in tasks if str(task.get("priority", "")).lower() == "alta")
    return sorted(summary.values(), key=lambda item: item["epic"])


def find_agent(slug: str) -> dict[str, Any] | None:
    for agent in AGENT_BACKLOG:
        if str(agent.get("slug", "") or "") == slug:
            return agent
    return None
