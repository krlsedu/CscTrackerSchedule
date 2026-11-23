Release Notes - v26.48.001

Abaixo estão as notas de atualização para a versão v26.48.001. Esta versão introduz a automação para renovação e gerenciamento de certificados SSL integrados ao Nginx Proxy Manager (NPM), garantindo a continuidade operacional e a segurança das comunicações sem necessidade de intervenção manual. Além disso, foram adicionadas coberturas completas de testes unitários, atualizações nas rotinas de agendamento (cron/scheduler), ajustes na esteira de CI/CD e a manutenção da documentação interna de engenharia e contexto da aplicação.

## 🚀 Features
* **SSL / NPM:** Implementação do módulo `NpmCertRenewer` integrado ao agendador de tarefas (`ScheduleJobs.py`) e ao ciclo de vida da aplicação (`app.py`), viabilizando a renovação programada e automatizada de certificados digitais do Nginx Proxy Manager. A implementação visa mitigar riscos de expiração de certificados e indisponibilidade de serviços com TLS, contando com suporte via container no `Dockerfile`, dependências atualizadas no `requirements.txt`, suíte abrangente de testes unitários (`tests/test_npm_cert_renewer.py`, `tests/test_schedule_jobs.py`), workflow para notificações de CI/CD e mapeamento técnico nas diretrizes internas de desenvolvimento (`NpmCertRenewer.py`).
  * *Commit: `1992144` | Autor: @krlsedu*

## 🐛 Fixes
*(Nenhuma alteração desta categoria nesta versão)*

## 🔧 Chore
* **Documentação / AI Memory:** Atualização do artefato de handoff de contexto da sessão de desenvolvimento, alinhando o status das entregas, decisões recentes e próximos passos técnicos para manter a continuidade do projeto (`.ai-memory/handoff.md`).
  * *Commit: `ee6f4d5` | Autor: @krlsedu*