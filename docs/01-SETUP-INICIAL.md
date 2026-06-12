# Setup Inicial Completo

Este fluxo cria a base operacional de um novo projeto Kaizen sem depender de planilhas e sem expor dados sensiveis no repositorio.

## Objetivo

Ao preencher um dominio e uma conta Google Ads, o setup inicial deve preparar:

- Google Ads: conversoes, meta operacional e tracking template.
- GA4: propriedade/stream e Measurement ID.
- GTM: container, tags, triggers e publicacao.
- WordPress/WPCode: instalacao do container no header/body e validacao no HTML publico.
- Microsoft Clarity: tag via GTM e validacao por Data Export API.
- Checklist final: liberacao apenas quando o setup estiver tecnicamente funcional.

## Entrada minima

- Dominio do site.
- Conta Google Ads dentro da MCC configurada.
- Conta GTM acessivel pelo OAuth.
- Conta GA4 mae.
- Moeda operacional.
- WordPress admin URL, usuario e senha ou application password.
- Clarity Project ID, quando ja existir.
- Clarity Data Export API Token, quando ja existir.

## OAuth completo

O OAuth usado pelo setup precisa cobrir:

- Google Ads API.
- Google Ad Manager API, quando a operacao tambem usa GAM.
- Google Analytics Admin API.
- Google Tag Manager API.

Escopos recomendados:

```text
https://www.googleapis.com/auth/adwords
https://www.googleapis.com/auth/admanager
https://www.googleapis.com/auth/analytics.edit
https://www.googleapis.com/auth/tagmanager.manage.accounts
https://www.googleapis.com/auth/tagmanager.edit.containers
https://www.googleapis.com/auth/tagmanager.edit.containerversions
https://www.googleapis.com/auth/tagmanager.publish
openid
email
https://www.googleapis.com/auth/userinfo.email
```

Regra de interface:

- Mostrar o botao de gerar OAuth apenas quando o OAuth estiver ausente ou invalido.
- Se OAuth estiver ok, exibir apenas o status validado.

## Google Ads

O setup deve automatizar:

- Criacao/validacao das conversoes padrao da operacao.
- Configuracao do tracking template da conta ou do nivel definido.
- Leitura das contas apenas pela MCC configurada.
- Registro dos IDs de conversao e labels no plano do setup.

Tracking template padrao:

```text
{lpurl}?utm_source=googleads&utm_medium={network}&utm_campaign={campaignid}&utm_term={campaignid}_{keyword}&utm_content={campaignid}_{placement}&utm_creative={campaignid}_{creative}
```

Regra intrinseca:

- O sistema nao reescreve UTM do usuario.
- Demand Gen, quando configurado a nivel de grupo, deve usar `adgroupId` no template do grupo.
- Se a campanha ja envia UTM correta, o sistema apenas valida e registra.

## GA4

O setup deve automatizar:

- Criacao da propriedade quando possivel.
- Criacao do web data stream.
- Registro do Measurement ID.
- Criacao das tags GA4 no GTM para os eventos operacionais.

Eventos base:

- `adView`
- `adViewInterstitial`
- `adViewRewarded`

## GTM

O setup deve automatizar:

- Criar ou localizar a conta GTM selecionada.
- Criar ou localizar container web do dominio.
- Criar tags Google Ads.
- Criar tags GA4.
- Criar tag Microsoft Clarity quando houver Project ID.
- Criar triggers dos eventos.
- Publicar workspace.
- Gerar snippets de header e body.

Validacao automatica:

- Container criado ou encontrado.
- Workspace publicado.
- Snippet de header gerado.
- Snippet de body gerado.

## WordPress / WPCode

O setup deve automatizar quando houver credenciais:

- Login no WordPress.
- Instalacao do script GTM no header.
- Instalacao do noscript no body.
- Validacao no HTML publico.

Se nao houver credenciais, o setup deve ficar aguardando instalacao, mas sem perder o plano.

## Microsoft Clarity

A API oficial disponivel para automacao e leitura e a Data Export API.

Ela nao cria projeto automaticamente. O projeto deve existir no Clarity e fornecer:

- Project ID.
- Data Export API Token.

Como encontrar o Project ID:

```text
https://clarity.microsoft.com/projects/view/PROJECT_ID/settings
```

Exemplo:

```text
clarity.microsoft.com/projects/view/abc123xyz/settings
Project ID = abc123xyz
```

O setup deve automatizar:

- Salvar o Project ID no plano.
- Criar tag Microsoft Clarity no GTM.
- Publicar GTM.
- Validar token pela API:

```text
GET https://www.clarity.ms/export-data/api/v1/project-live-insights?numOfDays=1&dimension1=URL
Authorization: Bearer TOKEN
```

Regras de seguranca:

- Nunca exibir token Clarity no painel.
- Nunca gravar token real no repositorio.
- Em respostas de detalhe, mostrar apenas `***received***`.

## Checklist final

O setup so deve liberar quando nao houver bloqueios tecnicos.

Itens esperados:

- Google Ads conversoes e tracking template: ok.
- GTM container e codigo: ok.
- Tags do GTM publicadas: ok.
- WPCode instalado no site: ok.
- GA4 Measurement ID: ok.
- Clarity Project ID: ok ou waiting_input.
- Clarity Data Export API: ok ou waiting_input.
- Validacao GTM automatizada: ok.

Nao exigir mais:

- Confirmacao manual de termos/configuracoes obrigatorias do Google Ads.
- Preview manual do GTM como bloqueio final.

## Status sugeridos

- `rascunho`: plano criado, ainda nao aplicado.
- `parcial`: uma ou mais automacoes falharam.
- `pronto_para_validacao`: faltam entradas externas, como Project ID.
- `liberado_para_trafego`: sem bloqueios tecnicos.

## Teste obrigatorio depois de implementar

Toda mudanca no setup inicial precisa rodar:

```bash
python -m py_compile webapp.py webapp_service.py
```

Validacoes minimas:

- Abrir `/setup-inicial`.
- Gerar plano.
- Aplicar tudo.
- Validar checklist final.
- Confirmar que tokens aparecem mascarados.
- Atualizar o mapa Graphify.

## O que nao deve entrar no repositorio

- Senhas.
- Tokens reais.
- Refresh tokens OAuth.
- Clarity API Token real.
- Banco real.
- Cookies.
- Chaves SSH.
- URLs internas privadas com segredo.
