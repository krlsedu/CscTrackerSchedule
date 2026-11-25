Release Notes - v26.48.002

Abaixo estão as notas de atualização para a versão v26.48.002. Esta versão introduz um mecanismo automatizado de renovação e sincronização de certificados SSL para o Nginx Proxy Manager (NPM), mitigando riscos de indisponibilidade por expiração de certificados e eliminando intervenções manuais. Além disso, a entrega inclui ampla cobertura de testes unitários, orquestração via agendador de tarefas e atualizações de governança e documentação de engenharia no repositório.

## 🚀 Features
* **Renovação SSL / Nginx Proxy Manager:** Implementação do módulo `NpmCertRenewer` integrado ao agendador de tarefas (`ScheduleJobs.py`) e à aplicação principal (`app.py`), automatizando a verificação e renovação de certificados SSL gerenciados pelo Nginx Proxy Manager (NPM). A motivação técnica foi eliminar a necessidade de manutenção manual e prevenir incidentes decorrentes de certificados expirados em ambiente de produção; a alteração adiciona tratamento de rotinas de agendamento, novas dependências em `requirements.txt`, ajustes no container em `Dockerfile`, pipeline de notificação de CI/CD (`notify-cicd.yml`) e uma suíte completa de testes automatizados para garantir a estabilidade do fluxo de renovação (`NpmCertRenewer.py`).
  * *Commit: `1992144` | Autor: @krlsedu*

## 🐛 Fixes
*(Nenhuma alteração desta categoria nesta versão)*

## 🔧 Chore
* **Automação de Build:** Atualização dos metadados de versão e sincronização das notas de release para o acionamento do ciclo de build automatizado da versão 26.48.001 (`version.txt`).
  * *Commit: `b972e95` | Autor: @krlsedu*
* **Documentação Interna:** Atualização do artefato de handoff de sessão da equipe (`handoff.md`), garantindo o alinhamento das decisões técnicas e do contexto do projeto para as próximas iterações de desenvolvimento (`.ai-memory/handoff.md`).
  * *Commit: `ee6f4d5` | Autor: @krlsedu*