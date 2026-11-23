---
project: CscTrackerSchedule
kind: decision
tier: semantic
pinned: true
updated_at: 2026-11-23
---

# 🏛️ Decisões Arquiteturais e Cláusulas Pétreas — `CscTrackerSchedule`

> Regras específicas e invioláveis deste repositório + histórico de decisões arquiteturais (ADRs).

## 🔒 Cláusulas Pétreas e Regras Invioláveis `[PINNED]`

1. **Renovação de Certificados SSL do Nginx Proxy Manager Somente via API REST Oficial:**
   - É proibido tentar renovar certificados executando `certbot renew` via CLI/exec diretamente no container `nginx`, pois isso não atualiza a tabela `certificate` (`expires_on`) no banco `nginx-db` nem sincroniza a interface do NPM.
   - Toda verificação e renovação deve ocorrer exclusivamente via API REST oficial do NPM (`POST /api/tokens`, `GET /api/nginx/certificates`, `POST /api/nginx/certificates/<id>/renew`) em `NPM_URL` (default `http://nginx:81` na `swarm_network`).
2. **Gatilho via Tabela `http_schedule` e Auto-Seed Idempotente:**
   - O agendamento periódico de tarefas do `CscTrackerSchedule` opera via registros ativos (`is_active = 'S'`) na tabela `http_schedule` consultada pelo `RemoteRepository` (`CscTrackerRepository`).
   - O endpoint `POST /npm/renew-certificates` (também acessível via `GET`) é auto-registrado de forma idempotente pelo `ScheduleJobs.init()` quando `NPM_IDENTITY` e `NPM_SECRET` estão configurados.
3. **WSGI de Produção via `csctracker-py-core` (Waitress) + Python 3.15:**
   - Manter `CMD ["python", "-u", "app.py"]` e `if __name__ == '__main__': starter.start()` como via única de inicialização (Waitress WSGI single-process multi-thread em produção, preservando as threads em background do `SchedulerService`).
   - Manter `FROM python:3.15-slim` com cache de layers no `Dockerfile`.

---

## 📜 Registro de Decisões (ADRs)
- **[2026-11-23] Renovação Automatizada de Certificados SSL via API do NPM e Atualização Core Python 3.15 (ID 102):**
  - Implementado `NpmCertRenewer.py` com autenticação JWT (`/api/tokens`), filtro `provider == "letsencrypt"`, suporte a `NPM_IGNORE_DOMAINS`, isolamento de falhas por certificado e cooldown em memória (`NPM_COOLDOWN_HOURS`, default `12h`, com bypass via `force=true`).
  - Exposto endpoint `POST /npm/renew-certificates` (e `GET`) em `app.py`.
  - Adicionado auto-seed idempotente na tabela `http_schedule` em `ScheduleJobs.py` e corrigido o parse resiliente de `job["body"]`.
  - Atualizado `requirements.txt` para `csctracker-py-core==26.47.3` e `csctracker-queue-scheduler==26.47.1` e `Dockerfile` para `python:3.15-slim`.
