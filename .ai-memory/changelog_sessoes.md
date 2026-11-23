---
project: CscTrackerSchedule
kind: changelog
tier: episodic
updated_at: 2026-11-23
---

# 📅 Changelog de Sessões — `CscTrackerSchedule`

## [2026-11-23] — Ideia 102 (Renovação Automatizada de Certificados SSL no NPM + Python 3.15 + Governança)
- Implementado `NpmCertRenewer.py` e rota `POST|GET /npm/renew-certificates` em `app.py`.
- Adicionado auto-seed idempotente na tabela `http_schedule` e normalização de `body` em `ScheduleJobs.py`.
- Atualizados `requirements.txt` (`csctracker-py-core==26.47.3`, `csctracker-queue-scheduler==26.47.1`) e `Dockerfile` (`python:3.15-slim`).
- Criados testes unitários offline (`8/8` passando) e auto-provisionada estrutura `.agents/`, `.ai-memory/`, `.junie/` e `.github/workflows/notify-cicd.yml`.
