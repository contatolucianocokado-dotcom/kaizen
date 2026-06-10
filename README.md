# Kaizen

Documentacao sanitizada do sistema Kaizen para controle de ROI, Google Ads, GAM, agentes, UTMs, receita e operacao.

Este repositorio nao contem senhas, tokens, bancos de dados, credenciais OAuth, cookies, chaves SSH ou arquivos sensiveis.

## Objetivo

Permitir que outra pessoa entenda e replique o sistema Kaizen em outro ambiente com seguranca.

## Para deixar funcional

1. Criar uma VPS com Python 3.10+.
2. Clonar este repositorio.
3. Criar ambiente virtual.
4. Instalar dependencias.
5. Criar `config.yaml` real a partir de `examples/config.example.yaml`.
6. Configurar OAuth do Google Ads.
7. Configurar Google Ad Manager.
8. Configurar banco de dados.
9. Subir o servico web.
10. Ativar timers e agentes.
11. Rodar sync inicial.
12. Validar painel, Ads, GAM, UTMs e agentes.

## Comandos base

```bash
git clone https://github.com/SEU_USUARIO/Kaizen.git
cd Kaizen

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
cp examples/config.example.yaml config.yaml
```

## Regra intrinseca

UTM e dado de origem, nao dado editavel.

O sistema nao altera `utm_campaign`, nao remapeia receita GAM de um ID para outro e nao inventa atribuicao.

Se houver divergencia, o sistema registra alerta para o agente responsavel.

