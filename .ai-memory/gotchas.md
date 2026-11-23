---
project: CscTrackerSchedule
kind: gotcha
tier: semantic
updated_at: 2026-11-23
---

# ⚠️ Gotchas e Armadilhas Conhecidas — `CscTrackerSchedule`

1. **Dessincronia do Certbot CLI no Nginx Proxy Manager:**
   - Rodar `certbot renew` direto no shell do container `nginx` renova os arquivos `.pem` em disco mas **não** atualiza `expires_on` no banco `nginx-db` (`certificate`). Usar sempre `POST /api/nginx/certificates/{id}/renew`.
2. **Timeout Síncrono da Chamada `/renew` do NPM:**
   - O endpoint `/api/nginx/certificates/{id}/renew` do NPM executa o Certbot de forma síncrona e pode levar de 15 a 60 segundos por domínio. Manter `NPM_REQUEST_TIMEOUT` com default de `120` segundos.
3. **Cooldown Anti-Rate Limit do Let's Encrypt:**
   - Quando um domínio está com apontamento DNS inválido ou falha na validação HTTP-01, tentar renovar repetidamente bloqueia a conta no Let's Encrypt. O `NpmCertRenewer` aplica cooldown em memória (`NPM_COOLDOWN_HOURS=12`) por certificado após falha, podendo ser forçado manualmente com `{"force": true}`.
4. **Parse de `body` na Tabela `http_schedule`:**
   - O campo `body` na tabela `http_schedule` pode vir como `"{}"`, `""`, `None` ou já convertido em `dict` quando `ScheduleJobs.init()` monta `args_`. `ScheduleJobs._parse_job_body()` normaliza qualquer formato para `dict` sem lançar `TypeError` em `json.loads`.
