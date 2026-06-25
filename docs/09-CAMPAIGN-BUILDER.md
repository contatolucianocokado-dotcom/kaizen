# Campaign Builder

## Objetivo

Criar e publicar campanhas Search, Demand Gen e Performance Max sem reutilizar contexto de outra URL por engano.

## Entrada comum

- Conta destino descoberta na MCC configurada.
- Tipo da campanha.
- Pais ou mundo.
- URL final.
- Orcamento.
- Objetivo.
- Nicho.
- Variacao.
- Conta/campanha modelo opcional.

## Regra de conteudo

Antes de gerar qualquer ativo, buscar e analisar a URL final. Titulo, corpo, idioma e tema da pagina sao a fonte principal.

Se houver campanha modelo:

- usar estrutura, configuracoes e formato como referencia;
- adaptar idioma e conteudo para a nova URL;
- nunca copiar dominio, texto, imagem ou nicho sem relacao.

Se nao houver modelo, usar o padrao seguro do tipo de campanha.

## Search

- Tres anuncios responsivos.
- Palavras-chave em correspondencia ampla quando solicitado.
- Frases de destaque relacionadas somente a URL.
- Sitelinks com titulo e duas descricoes.
- Maximizar conversoes sem CPA inicial quando esse for o padrao escolhido.
- Meta de conversao real selecionada no Google Ads.
- Expansao de URL final desligada quando configurado.
- Nome sem sufixo de midia indevido.

## Demand Gen

- Escolha de canais.
- Publicos selecionados pelo usuario.
- Dispositivos.
- Imagens ou videos.
- Mais de um grupo: tracking em nivel de grupo com `adgroupId`.
- Anuncio e ativos devem seguir a URL ou a campanha modelo.

## Performance Max

- URL, objetivo, pais/idioma, grupos de recursos e ativos.
- Imagens e videos devem ser revisados antes da publicacao.
- Nao publicar ativo sem relacao semantica com a pagina.

## Confirmacao obrigatoria

Depois de publicar, mostrar:

- criado ou nao criado;
- IDs gerados;
- status;
- erros retornados pela API;
- link da campanha correta no Google Ads.

Clique silencioso sem resultado visivel e falha operacional.
