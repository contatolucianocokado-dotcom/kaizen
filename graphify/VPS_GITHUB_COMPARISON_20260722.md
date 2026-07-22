# Registro Graphify - Comparacao VPS x GitHub

Data: 2026-07-22
Instancia: Julio
Tema: atualizacao segura do repositorio publico Kaizen

## Nos principais

- `VPS Julio`: ambiente operacional real de app.wizseoservicesaustralia.com.
- `Repositorio operacional`: cokado1/roas-turnos-app.
- `Repositorio publico`: contatolucianocokado-dotcom/kaizen.
- `Politica segura`: publicar somente artefatos sanitizados.
- `Risco`: vazamento de dados sensiveis ao copiar arquivos brutos da VPS.

## Relacoes

- `VPS Julio` contem `Repositorio operacional`.
- `Repositorio publico` deve receber apenas `Politica segura`, `documentacao`, `templates` e `scripts sem segredo`.
- `Politica segura` bloqueia copia bruta de `webapp.py`, `webapp_service.py`, bancos, backups e configs locais.
- `Auditoria de segredos` deve validar qualquer material candidato antes de push.

## Decisao registrada

Nao sincronizar a VPS em massa com o GitHub publico.
Comparar primeiro, sanitizar depois e publicar apenas mudancas revisaveis em PR.

