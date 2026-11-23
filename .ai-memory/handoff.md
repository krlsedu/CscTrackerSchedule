---
project: CscTrackerSchedule
kind: handoff
tier: working
updated_at: 2026-11-23
---

# 🤝 Handoff Ativo — `CscTrackerSchedule`

> **Escopo do Projeto:** Microsserviço agendador HTTP (`http_schedule`) e automação periódica de infraestrutura (incluindo renovação automatizada de certificados SSL via API REST do Nginx Proxy Manager).

## 1. Onde Paramos / Contexto Atual (`WIP`)
- **ID 102 implementada no código (aguardando revisão conjunta):**
  - Criado `NpmCertRenewer.py` integrando com a API REST do Nginx Proxy Manager (`POST /api/tokens`, `GET /api/nginx/certificates`, `POST /api/nginx/certificates/<id>/renew`), com parse resiliente de `expires_on`, filtro `letsencrypt`, `NPM_IGNORE_DOMAINS`, isolamento de falhas e cooldown em memória (`NPM_COOLDOWN_HOURS=12`).
  - Exposto endpoint `POST /npm/renew-certificates` (e `GET`) em `app.py` aceitando `threshold_days`, `force` e `cert_id` via JSON body ou query params.
  - Atualizado `ScheduleJobs.py` com parse seguro de `job["body"]` e auto-seed idempotente do job `"Renew NPM SSL Certificates"` na tabela `http_schedule` quando `NPM_IDENTITY` e `NPM_SECRET` estão definidos.
  - Atualizados `requirements.txt` (`csctracker-py-core==26.47.3`, `csctracker-queue-scheduler==26.47.1`, `requests`) e `Dockerfile` (`FROM python:3.15-slim` com cache de layers).
  - Criada suíte de testes unitários offline em `tests/test_npm_cert_renewer.py` e `tests/test_schedule_jobs.py` (`8/8` testes passando).
  - Provisionada governança `/csctracker` (`.agents/`, `.ai-memory/`, `.junie/guidelines.md`, `.github/workflows/notify-cicd.yml`).

## 2. Tentativas Descartadas / O que Falhou (`Failed Approaches`)
- Ao importar `app` nos testes unitários sem mockar `SchedulerService.init(10)`, as threads de fila do scheduler permaneciam ativas após o término do `unittest`; resolvido mockando `SchedulerService.init` no teste de rota Flask.

## 3. Estado dos Arquivos / Branch / Build
- **Branch:** `master`.
- **Pendência no Portainer (`csctracker-main.yml` — serviço `schedule`):** adicionar variáveis `NPM_URL=http://nginx:81`, `NPM_IDENTITY=<email_admin_npm>`, `NPM_SECRET=<senha_admin_npm>` e `NPM_RENEW_THRESHOLD_DAYS=15`.
- **Pendência de Cofre GitHub (se ainda não configurado no repo/org):** cadastrar `TOKEN_CSCTRACKER` (e opcional `CICD_WEBHOOK_URL`) em **Settings ➔ Secrets and variables ➔ Actions** para o workflow `notify-cicd.yml`.

## 4. Próximos Passos / Pendências Abertas
- Revisão conjunta com o desenvolvedor ➔ commit/push (`CSCTRACKER-102`) ➔ fechamento do Handoff no Drive (`AI_specs/CscTrackerSchedule.md`, `projects/ai-memory/projects/CscTrackerSchedule/` e `.zip`).
