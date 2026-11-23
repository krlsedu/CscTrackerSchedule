---
name: csctracker
description: Execute ALWAYS whenever interacting with the CscTracker ecosystem (CscTrackerAndroid, CscTrackerVigilante, CscTrackerVigilanteCurador, CscTrackerNotificationAI, CscTrackerMqtt, CscTrackerBackend, CscTrackerRepository, CscTrackerAiCore, CscTrackerPyCore, CscTrackerIA, CscTrackerCicd, CscTrackerFront, CscTrackerWhatsConversation, CscTrackerFFExtension, and any other CscTracker repo) or when asked to analyze code, diagnose issues, or create patches/prompts for CscTracker projects. Enforces prior user approval before modifying governance/skills, canonical reading chain (SKILL.md -> README.md -> <Project>.md -> ideias/ -> ai-memory/projects/<Project>/), mandatory enforcement of project-specific [PINNED] rules in .ai-memory/decisions.md, token-frugal KISS execution, clean UI guidelines, pure Markdown/YAML/CSV storage with absolute prohibition of Google Docs/Sheets and in-place Drive updates, GitHub-first source resolution with Drive ZIP sync at Handoff, direct codebase modification followed by mutual review and PT-BR commit/push (CSCTRACKER-ID <=50 chars imperative subject), non-blocking git push, absolute fidelity over sanitization, 7-status Markdown/CSV backlog lifecycle, continuous ai-memory lifecycle, and repository auto-provisioning (.agents/, .ai-memory/, .junie/, .github/workflows/).
---
# CscTracker Governance and Execution Protocol

## Summary
Protocolo mestre agnóstico e **Fonte Única da Verdade (SSOT)** da governança global do ecossistema **CscTracker**. Define: (1) aprovação prévia obrigatória antes de alterar a própria skill ou diretrizes; (2) cadeia de leitura e cumprimento compulsório das cláusulas pétreas `[PINNED]` específicas de cada repositório em `.ai-memory/decisions.md` e `AI_specs/<Projeto>.md`; (3) execução KISS com economia estrita de tokens; (4) fidelidade absoluta acima de sanitização; (5) UI limpa sem detalhes de infra; (6) fluxo Antigravity (alteração direta ➔ revisão conjunta ➔ commit PT-BR `CSCTRACKER-<ID>` <= 50 chars + push seguro na `master` via merge); (7) bloqueio de implementação sem Ideia vinculada e backlog de 7 status em `projects/ideias/`; (8) memória contínua `ai-memory` em `projects/ai-memory/projects/<Projeto>/` (desempate pelo mais recente); (9) proibição absoluta de Google Docs/Sheets e updates in-place no Drive; e (10) testes unitários pragmáticos e auto-provisionamento nos repositórios.

## When to Use
- Qualquer menção a repositórios do ecossistema CscTracker (`CscTracker*`).
- Ao analisar logs, investigar falhas, desenhar arquiteturas, gerenciar o backlog de Ideias, consultar/atualizar a `ai-memory` ou aplicar melhorias/correções diretamente no código para o desenvolvedor (Carlos Schwalm).

## 0. Diretriz 3.19 — Aprovação Prévia Obrigatória para Alterações na Skill e Governança
- **PROIBIÇÃO DE AUTO-EDIÇÃO SILENCIOSA:** Sempre que o desenvolvedor relatar um problema ou solicitar uma melhoria na `SKILL.md` ou nos documentos estruturais (`README.md`, `00_INDEX.md`), o assistente **NÃO deve aplicar alterações diretamente**.
- **FLUXO OBRIGATÓRIO:** Antes de gravar qualquer alteração de governança local ou no Google Drive, o assistente DEVE apresentar ao desenvolvedor um **resumo objetivo do que será feito, quais arquivos serão afetados e como a mudança será implementada**, aguardando aprovação expressa.

## 1. Pipeline de Ingestão Canônica (Ordem Obrigatória de Leitura) e Mapa de IDs do Drive
Antes de responder qualquer pergunta técnica, analisar código ou formular alterações/patches/prompts, o assistente DEVE seguir esta sequência:
0. **Sincronismo da própria `SKILL.md` (`projects/SKILL.md` — ID: `1zccc1imMWMiobP4iY0KOmds97OLTUaNW`):** Verificar se `projects/SKILL.md` no Google Drive está mais recente que `~/.gemini/skills/csctracker/SKILL.md` e `.agents/skills/csctracker/SKILL.md` do repositório; se estiver, sincronizar as cópias locais antes de prosseguir.
1. `projects/README.md` (ID: `1E2w6Tw356wIEqz23wSIJz-csa6bGxgDR`): Mapa do ecossistema, catálogo dos serviços ativos e links canônicos.
2. `projects/AI_specs/<NomeDoProjeto>.md` (Pasta `AI_specs` ID: `1PzCHUi9CbajfJat0UpY3HD7jk9nh0Dir`): Especificação técnica estática do projeto alvo.
3. Diretório `projects/ideias/` (Pasta `ideias` ID: `11uqM37jcqbdDReqZreCi5uemSZmbAJYI`):
   - `projects/ideias/ideias.csv` (ID: `10sgqDHrpRJlw39T5Rk8iRbKXdHvUJNsf`): Índice consolidado (`id,nome,status`).
   - `projects/ideias/<ID>.md` (ativas), `prontas/<ID>.md` (concluídas — Pasta ID `1qRvQ5DwyY7P0T9lK7-ud9kalSbceXUKl`), `monetizar/<ID>.md` (SaaS, criada sob demanda).
4. Diretório `projects/ai-memory/` (`1noA7dlsUx4GfaytEZaczaZeOk_r_9p8v`) e `.ai-memory/` local do repositório (`Recall` obrigatório):
   - Globais: `00_INDEX.md` (`1IQotJT4uuro8oIN23CUDbzpoULSf8J8I`), `WIP_GLOBAL.md` (`1Mwc_JQCx_hr3RXOzoRi6ZguhdR9WJ31l`), `INBOX_INTEGRACOES.md` (`1r3AgaEJ9Mv19qPu0ol0s9lUHf3LWqIYC`), `GLOBAL_ECOSSISTEMA.md` (`1Plylw784Cv3RLv5JYCD857IDqB8zCPRb`) e `gotchas_globais.md` (`1ysiAnfWQ0mJh51SYCe9MDxeh5gp5sZOP` quando envolver infra/deploy).
   - Projeto alvo (exclusivamente dentro de `projects/ai-memory/projects/<NomeDoProjeto>/` ou `.ai-memory/`): `handoff.md`, `gotchas.md` e **`decisions.md` (obrigatório absorver todas as regras `[PINNED]` do projeto antes de codar)**.

### Mapa Rápido de IDs Estruturais no Google Drive (Para Aceleração de Boot)
| Caminho no Drive | Tipo | Drive ID |
| :--- | :--- | :--- |
| `CscTracker/` (Raiz) | Folder | `1WCHnvfz375y1akblei9cy6DnEFbdrGWz` |
| `CscTracker/projects/` | Folder | `1mmHZRne2L_7y9Aq7uwUNZiOm5oOC71Hw` |
| `projects/README.md` | `text/markdown` | `1E2w6Tw356wIEqz23wSIJz-csa6bGxgDR` |
| `projects/SKILL.md` | `text/markdown` | `1zccc1imMWMiobP4iY0KOmds97OLTUaNW` |
| `projects/AI_specs/` | Folder | `1PzCHUi9CbajfJat0UpY3HD7jk9nh0Dir` |
| `projects/ideias/` | Folder | `11uqM37jcqbdDReqZreCi5uemSZmbAJYI` |
| `projects/ideias/prontas/` | Folder | `1qRvQ5DwyY7P0T9lK7-ud9kalSbceXUKl` |
| `projects/ideias/ideias.csv` | `text/csv` | `10sgqDHrpRJlw39T5Rk8iRbKXdHvUJNsf` |
| `projects/ai-memory/` | Folder | `1noA7dlsUx4GfaytEZaczaZeOk_r_9p8v` |
| `projects/ai-memory/projects/` | Folder | `1iNcywMpN7MHjQ45iTL1EKGaVkUTyOQvH` |
| `projects/repositorys/` | Folder | `1M8fP4RiZoXYwlgkDJB-JXHm9qnB_e0Qe` |
| `projects/composes/` | Folder | `145ar_e4BYbTuVIrS0VeDmyEoMbMcAx5W` |
| `projects/fluxos/` | Folder | `1NRFsOBZ1fHfKLxMq6rsZPBEn7WnoaX5I` |

> **IDs das subpastas de projetos (`projects/ai-memory/projects/<Projeto>/`):** O índice único fica em `projects/ai-memory/00_INDEX.md` (`1IQotJT4uuro8oIN23CUDbzpoULSf8J8I`) — ou consultar via `gdrive readonly ls 1iNcywMpN7MHjQ45iTL1EKGaVkUTyOQvH`.

## 2. Separação Estrita de Responsabilidades (Fonte Única da Verdade — Anti-Duplicação)
Para evitar retrabalho e desperdício de tokens no `Recall`, **cada arquivo tem um escopo único e inegociável — é PROIBIDO duplicar regras entre eles**:
- **`projects/SKILL.md` (`~/.gemini/skills/csctracker/SKILL.md`):** **ÚNICA fonte das regras globais de governança e comportamento do agente** (Git, commits, branches, backlog, testes, memória, KISS, UI, aprovações). Qualquer nova regra global entra **somente aqui** (e nunca é replicada em `README.md`, `GLOBAL_ECOSSISTEMA.md` ou `gotchas_globais.md`). Nenhuma regra específica de um único repositório mora aqui.
- **`projects/README.md`:** **Exclusivamente mapa do ecossistema** — catálogo dos projetos ativos, links/IDs do Drive e tabela de roteamento arquitetural. Não contém regras de execução/Git.
- **`projects/ai-memory/GLOBAL_ECOSSISTEMA.md`:** **Exclusivamente fatos técnicos e contratos vivos da infraestrutura** (VPS/Portainer, ClickHouse, MQTT, integrações entre serviços). Não contém diretrizes de comportamento do agente.
- **`projects/ai-memory/gotchas_globais.md`:** **Exclusivamente armadilhas técnicas que afetam múltiplos projetos** (ex.: Portainer/Compose). Gotchas específicos de um único repositório ficam em `.ai-memory/gotchas.md` ou `decisions.md` daquele projeto.
- **`projects/AI_specs/<Projeto>.md`:** Arquitetura estática do projeto (propósito, stack, módulos, modelos de dados e links de fluxos Mermaid).
- **`projects/ai-memory/projects/<Projeto>/` (`.ai-memory/` no Git):** Memória viva e **Cláusulas Pétreas `[PINNED]` específicas daquele repositório**:
  - `.ai-memory/decisions.md` (seção `[PINNED]`): Cláusulas pétreas, bloqueios de arquitetura, regras de build/UI/deploy/testes específicas daquele repositório.
  - `.ai-memory/gotchas.md`: Armadilhas descobertas em produção e abordagens que falharam.
  - `.ai-memory/handoff.md`: Estado vivo da última sessão e próximos passos.
- **REGRA DE OURO DE OBEDIÊNCIA LOCAL:** Ao atuar em qualquer projeto, o assistente DEVE ler e cumprir compulsoriamente todas as cláusulas `[PINNED]` do `.ai-memory/decisions.md` daquele repositório como restrições invioláveis. Se alguma solicitação exigir flexibilizar uma regra `[PINNED]` local ou global, aplicar o **Protocolo de Apontamento Obrigatório de Violação** (declarar em destaque qual regra é violada e o impacto técnico antes de propor código).

## 3. Diretriz 3.6 e 3.13 — Economia de Tokens (KISS Global) e Fidelidade Absoluta
- **Economia de Tokens e Princípio KISS (Global para Tudo):**
  - Economizar tokens de raciocínio e saída: ser direto, objetivo, focado no problema real e agir KISS (Keep It Simple, Stupid).
  - Evitar projeções mirabolantes, abstrações prematuras, over-engineering, refatorações fora de escopo ou respostas prolixas. Fazer intervenções cirúrgicas e enxutas no menor número possível de arquivos.
- **Fidelidade Absoluta Prevalece sobre Sanitização (Diretriz 3.13 — Mandatória):**
  - Priorizar SEMPRE fidelidade absoluta ao código, composes e configurações do desenvolvedor, em detrimento de qualquer ímpeto de "limpeza" ou "organização" autônoma.
  - É terminantemente proibido remover, omitir ou alterar flags de infraestrutura (`external: true`, nomes físicos de volumes, portas, networks, aliases, variáveis de ambiente ou configs) sem validação prévia.
  - Caso identifique algo potencialmente sanitizável ou obsoleto, **APONTAR E PERGUNTAR** obrigatoriamente ao desenvolvedor antes de tocar.

## 4. Diretriz 3.9 — Design Limpo de UI e Anti-Poluição Visual (Global)
- Toda interface voltada ao usuário final (mobile, web, desktop ou extensão) deve ser minimalista, limpa e elegante.
- Proibição de expor detalhes internos de infraestrutura na UI (rotas internas/BFF, portas, nomes de microsserviços, sufixos técnicos redundantes).
- Se houver dúvida sobre nomenclatura ou rótulos de interface, **perguntar obrigatoriamente ao desenvolvedor**.

## 5. Diretriz 3.5 e 3.10 — Regra de Entrega: Alteração Direta ➔ Revisão Conjunta ➔ Commit/Push (Antigravity) e Padrão de Commit
- **1. Fluxo Padrão no Antigravity (IDE / CLI / Remote Agents — PRIORIDADE MÁXIMA):**
  - Com acesso direto ao sistema de arquivos e execução (`write_to_file`, `replace_file_content`, `run_command`), o assistente **NÃO** deve emitir blocos `.patch` por padrão.
  - Ciclo obrigatório:
    1. **Alteração Direta:** Aplicar as alterações cirúrgicas diretamente nos arquivos do repositório, respeitando todas as regras `[PINNED]` em `.ai-memory/decisions.md` (e rodar testes/validações locais quando aplicável).
    2. **Revisão Conjunta (PARADA OBRIGATÓRIA):** Apresentar ao desenvolvedor um resumo claro, curto e contextualizado dos arquivos alterados e do rationale, **PARAR e aguardar a aprovação explícita das alterações**.
    3. **Commit & Push (SOMENTE APÓS AUTORIZAÇÃO):** **É terminantemente PROIBIDO executar `git commit`, `git push` ou finalizar o `Handoff` (`.zip`/status) automaticamente sem autorização expressa do desenvolvedor após a revisão** — autorizar a implementação (ex.: *"pode implementar"*) **NÃO** autoriza commit nem push. Só commitar/pushar na mesma etapa se o desenvolvedor pedir expressamente no comando inicial (ex.: *"implemente, commite e faça push"*). Seguir rigorosamente o **Padrão Canônico de Mensagens de Commit (Seção 5.2)** e o **Protocolo de Push Seguro (Seção 5.3)**.
- **2. Padrão Canônico de Mensagens de Commit (Obrigatório em Todo o Ecossistema CscTracker):**
  1. **Língua:** Obrigatoriamente em português (PT-BR), sem prolixidade.
  2. **Primeira Linha (Assunto):** Iniciar obrigatoriamente com o identificador da Ideia vinculada no formato `CSCTRACKER-<ID>` seguido de **uma única frase** no modo imperativo (`Atualiza...`, `Corrige...`, `Adiciona...`, `Remove...`, `Refatora...`), mantendo **no máximo 50 caracteres** no total.
  3. **Linha em Branco:** Obrigatória separando o assunto do corpo.
  4. **Corpo (Explicação Detalhada):** No **máximo duas ou três frases**, focadas no que foi feito e no porquê técnico/funcional.
  ```git
  CSCTRACKER-<ID> <Ação no imperativo (total <= 50 chars)>

  <Primeira frase contextualizando a alteração e impacto principal. Segunda frase com detalhes técnicos relevantes. Terceira frase opcional se estritamente necessário.>
  ```
  5. **Checagem de Credenciais e `.env` antes do Commit:** Antes de commitar, revisar `git diff --staged` em busca de arquivos `.env`, chaves de API, tokens ou senhas. Caso encontre algo sensível, **avisar explicitamente o desenvolvedor e perguntar se deseja commitar mesmo assim**.
- **3. Protocolo de Branch e Execução Segura de Git Push:**
  - **Branch Padrão (`master`):** Trabalhar **sempre na branch `master`** (ou na branch principal ativa do repositório). **Não criar branches separadas** por conta própria — só criar branch para demandas em paralelo quando o desenvolvedor solicitar expressamente.
  - **Não-Bloqueante e Branch Dinâmica:** Usar sempre `GIT_TERMINAL_PROMPT=0` e identificar a branch ativa dinamicamente (`git push origin HEAD` ou `git push origin $(git branch --show-current)`), nunca chumbando `main`/`master` no comando de push.
  - **Proibição Absoluta de Force Push:** Proibido `git push --force` / `-f`, salvo pedido explícito do desenvolvedor.
  - **Remote à Frente (Usar `merge`, NUNCA `rebase`):** Caso o `git push` falhe porque o remoto está à frente, **NUNCA** usar `git rebase` ou `git pull --rebase`. Sincronizar via merge (`GIT_TERMINAL_PROMPT=0 git pull --no-rebase origin $(git branch --show-current)`); se houver conflito de merge, parar e alinhar com o desenvolvedor antes de prosseguir.
  - **Conduta em Caso de Falha de Push (Credencial / Divergência):** Certificar que o commit local está salvo (`git log -1`), nunca alterar `.gitconfig`/`.ssh` ou tentar login interativo, informar o erro com o comando pronto (`git push origin <branch>`) e anotar `[COMMIT_LOCAL_OK - PUSH_PENDENTE: <branch>]` em `.ai-memory/handoff.md`.
- **4. Exceção Estrita para `.patch`:**
  - Entregar bloco `.patch` no chat **SOMENTE** quando: (a) o desenvolvedor pedir expressamente (*"manda em patch"*, *"gere o diff"*), ou (b) em chats web do Gemini sem ferramentas de edição de arquivos.
- **5. Delegação para Agentes Externos (JetBrains Junie / Outras IDEs):**
  - Ao gerar prompts para delegação a agentes externos, incluir obrigatoriamente a diretriz de economia de tokens/KISS e todas as cláusulas `[PINNED]` do `.ai-memory/decisions.md` do projeto alvo (além de código completo verbatim quando o foco for economizar ciclos de raciocínio de agentes pagos como o Junie).

## 6. Diretriz 3.11, 3.14 e 3.22 — Governança do Backlog (`projects/ideias/`) e Rastreabilidade Obrigatória
- **Bloqueio de Implementação sem Ideia Vinculada (Diretriz 3.22 — Mandatória):**
  - **É terminantemente proibido implementar qualquer código ou alteração sem que exista uma Ideia vinculada em `projects/ideias/<ID>.md` e `ideias.csv`.**
  - Se o desenvolvedor solicitar uma implementação e **não houver uma Ideia vinculada**, o assistente **DEVE parar antes de codar, informar explicitamente que não existe Ideia registrada para a demanda e perguntar ao desenvolvedor o que fazer**: se deseja **registrar e refinar a Ideia** primeiro (`Rascunho`/`Nova`) ou **registrar a Ideia e já seguir para a implementação**.
- **Estrutura Canônica em `projects/ideias/` (EXCLUSIVA DO GOOGLE DRIVE):**
  - Todo o backlog de ideias reside **exclusivamente** em `projects/ideias/` no Google Drive (`G:\My Drive\CscTracker (1)\projects\ideias\`). **É terminantemente PROIBIDO criar, copiar ou espelhar arquivos de ideias (ex.: `docs/ideias/`) dentro dos repositórios Git de código-fonte.**
  - `projects/ideias/<ID>.md`: Demandas ativas (nome limpo apenas pelo número: `10.md`, `44.md`, `54.md`).
  - `projects/ideias/prontas/<ID>.md`: Demandas concluídas.
  - `projects/ideias/monetizar/<ID>.md`: Demandas SaaS/monetização.
  - `projects/ideias/ideias.csv`: Índice mestre (`id,nome,status`) sempre atualizado in-place (`gdrive mutate update 10sgqDHrpRJlw39T5Rk8iRbKXdHvUJNsf ... --mime-type text/csv`).
- **Regra de Sequencial Único e Monotônico (`MAX(ID) + 1`):**
  - Novo `<ID>` é sempre o maior ID já registrado (entre `ideias/`, `prontas/`, `monetizar/` e `ideias.csv`) `+ 1`. Proibido reutilizar IDs antigos.
- **Os 7 Status Canônicos Obrigatórios (Proibido criar variações):**
  ```
  [Rascunho] ➔ [Nova] ➔ [Em Andamento] ➔ [Por Publicar] ➔ [Em Homologação] ➔ [Concluída]
                               ▲                                    │
                               │                                    ▼
                               └─────── [Aguardando Ajuste] ◄───────┘
  ```
  1. **`Rascunho`**: Capturada no chat, aguardando confirmação do desenvolvedor.
  2. **`Nova`**: Aprovada no backlog, pronta para iniciar desenvolvimento.
  3. **`Em Andamento`**: Em codificação ativa.
  4. **`Por Publicar`**: Implementada e commitada, aguardando release PyPI, build Docker/CI ou APK.
  5. **`Em Homologação`**: Publicada e rodando em ambiente real, em validação prática.
  6. **`Aguardando Ajuste`**: Falhou ou apresentou pontos na homologação (retorna para `Em Andamento` ao iniciar correção).
  7. **`Concluída`**: 100% homologada e movida para `projects/ideias/prontas/<ID>.md`.
- **Sincronia:** Ao concluir ou alterar uma Ideia, atualizar o `<ID>.md`, o `ideias.csv` e o mapeamento em `AI_specs/<Projeto>.md`.

## 7. Diretriz 3.12 e 3.21 — Gestão Híbrida de Fontes (`git pull` Prévio, GitHub vs. `projects/repositorys/`) e Sincronismo `.zip`
- **Sincronismo Prévio Obrigatório (`git pull` — Diretriz 3.21):** Antes de iniciar qualquer análise de código ou tarefa em um projeto que possua repositório Git (`.git/` local ou remoto no GitHub), o assistente DEVE garantir o `git pull` atualizado:
  - **Com repositório local já existente:** Executar `GIT_TERMINAL_PROMPT=0 git pull --ff-only` (se houver alterações locais não commitadas em `git status --porcelain`, falta de credencial ou divergência, **parar e solicitar ao desenvolvedor** que faça o `git pull` antes de prosseguir).
  - **Em ambiente sem execução direta ou IDE local do usuário:** Solicitar expressamente ao desenvolvedor que execute o `git pull` no projeto antes de iniciar a tarefa.
- **Leitura (GitHub-First):** Com `git`/`gh` ativos, ler/clonar diretamente do GitHub. Sem acesso ao GitHub (ex.: Gemini Web), baixar o `.zip` em `projects/repositorys/<Projeto>/` (`1M8fP4RiZoXYwlgkDJB-JXHm9qnB_e0Qe`).
- **Fechamento (`Handoff`):** `git commit` e `git push` acontecem **somente após apresentação da revisão e aprovação explícita do desenvolvedor**; o espelho `.zip` limpo (sem `.git/`, `.gradle/`, `build/`, `.venv/`, `__pycache__/`) é atualizado in-place em `projects/repositorys/<Projeto>/` preferencialmente no fechamento da sessão/demanda (`Handoff`).
- **Paridade na Dúvida:** Se houver chance de ajustes locais não commitados, perguntar obrigatoriamente: *"O código atual (GitHub/Drive) está alinhado com a sua build em execução ou houve novos ajustes locais?"*
- **Verificação de Versão de Libs Core Python no PyPI (`csctracker-py-core` e `csctracker-ai-core`):** Sempre que atuar em um projeto Python, verificar se ele declara `csctracker-py-core` (`csctracker_py_core`) ou `csctracker-ai-core` (`csctracker_ai_core`) em `requirements.txt`, `pyproject.toml` ou `setup.py`. Caso possua, consultar a última versão no PyPI (`https://pypi.org/pypi/<pacote>/json`) e, se estiver desatualizada, **informar a versão atual vs. a mais recente e perguntar ao desenvolvedor se deseja atualizar** (sem atualizar silenciosamente).

## 8. Diretriz 3.16 — Auto-Provisionamento e Versionamento nos Repositórios (`.agents/`, `.ai-memory/`, `.junie/`, `.github/workflows/`)
Sempre que interagir com qualquer repositório CscTracker, verificar e manter atualizados na raiz **apenas** os arquivos de governança, memória e CI/CD abaixo:
1. `.agents/skills/csctracker/SKILL.md`: Cópia fiel de `projects/SKILL.md` (`1zccc1imMWMiobP4iY0KOmds97OLTUaNW` / `~/.gemini/skills/csctracker/SKILL.md`).
2. `.ai-memory/` (`handoff.md`, `gotchas.md`, `decisions.md`, `changelog_sessoes.md`): Espelhada exclusivamente com `projects/ai-memory/projects/<Projeto>/` (`1iNcywMpN7MHjQ45iTL1EKGaVkUTyOQvH`).
3. `.junie/guidelines.md`: Baseado em `TEMPLATE_JUNIE_GUIDELINES.md` (`1dzwZDG_MIhSBLDzpllakQIyc7PJvKVpv`), orientando economia de tokens/KISS e leitura obrigatória de `.ai-memory/` + `.agents/skills/csctracker/SKILL.md`.
4. `.github/workflows/notify-cicd.yml` (Notificação de Push ao `CscTrackerCicd`):
   - Verificar se existe o workflow de notificação de commits em `.github/workflows/` (ao lado dos workflows de release existentes).
   - Caso não exista, **criar** `.github/workflows/notify-cicd.yml` disparado em `push` (ignorando tags `v*`) enviando `POST` para `${{ secrets.CICD_WEBHOOK_URL || vars.CICD_WEBHOOK_URL || 'https://cicd.csctracker.com/webhook' }}` com header `Authorization: Bearer ${{ secrets.TOKEN_CSCTRACKER }}`, `Content-Type: application/json` e o payload nativo do push (`--data-binary "@${GITHUB_EVENT_PATH}"`).
   - **Passos obrigatórios no cofre do GitHub (orientar o desenvolvedor ao criar):** No repositório (ou organização) no GitHub, acessar **Settings ➔ Secrets and variables ➔ Actions** e cadastrar:
     - **Secret obrigatório:** `TOKEN_CSCTRACKER` (token válido do `auth.csctracker.com`, validado em `/oauth/check_token`).
     - **Variable ou Secret opcional:** `CICD_WEBHOOK_URL` (para sobrescrever a URL padrão `https://cicd.csctracker.com/webhook`).
- **Proibição de Espelhamento de Backlog/Specs no Repositório:** Dentro dos repositórios Git só é permitido manter os itens de governança/memória/workflows acima. É terminantemente proibido criar ou espelhar `ideias/` (ex.: `docs/ideias/`), `AI_specs/` ou `fluxos/` dentro da pasta do projeto.

## 9. Diretriz 3.17 — Protocolo de Memória Contínua `ai-memory` (`Recall ➔ Checkpoint ➔ Consolidate ➔ Handoff`)
- **⚠️ TRAVA ESTRUTURAL INEGOCIÁVEL:** O espelho de qualquer repositório no Google Drive reside obrigatoriamente em **`CscTracker/projects/ai-memory/projects/<NomeDoProjeto>/`** (`G:\My Drive\CscTracker (1)\projects\ai-memory\projects\<NomeDoProjeto>\`, com **dois** níveis `projects`). É terminantemente PROIBIDO criar pastas de projetos direto na raiz `projects/ai-memory/<NomeDoProjeto>/`.
- **Desempate Git vs. Drive (Sempre o Mais Recente):** Em caso de divergência entre `.ai-memory/` no repositório Git e `projects/ai-memory/projects/<NomeDoProjeto>/` no Google Drive, **comparar as datas de modificação e adotar sempre o mais recente como base** (mesclando novidades pontuais do outro se houver, nunca sobrescrevendo o arquivo mais novo).
- **1. `Recall`:** Ler `00_INDEX.md`, `WIP_GLOBAL.md`, `INBOX_INTEGRACOES.md`, `GLOBAL_ECOSSISTEMA.md` e o `.ai-memory/` (`handoff.md`, `gotchas.md`, `decisions.md` `[PINNED]`) do projeto alvo. Avisar imediatamente sobre pendências em `INBOX_INTEGRACOES.md`. Se detectar memória obsoleta que contradiz o código atual (exceto itens `[PINNED]`), atualizar ou propor a remoção no `Handoff` em vez de acumular `[STALE]` — **antes de excluir qualquer item de memória, sempre informar exatamente o que será excluído e pedir confirmação ao desenvolvedor**.
- **2. `Checkpoint` Anti-Compactação:** Em sessões longas, atualizar `.ai-memory/handoff.md` (e `WIP_GLOBAL.md`) a cada etapa concluída ou abordagem que falhou (*Failed Approaches*).
- **3. `Consolidate` e Tags:** Armadilhas vão para `gotchas.md` (ou `gotchas_globais.md`); decisões e regras invioláveis de projeto vão para `decisions.md` com tag `[PINNED]`. Respeitar `[EXPIRES: AAAA-MM-DD]` e `[TEMP: até concluir Ideia #ID]`.
- **4. `Handoff`:** Atualizar `.ai-memory/handoff.md` e `changelog_sessoes.md` no Git e espelhar in-place em `projects/ai-memory/projects/<Projeto>/` no Drive; atualizar `WIP_GLOBAL.md`; registrar aviso em `INBOX_INTEGRACOES.md` caso altere contrato entre serviços; sincronizar `.zip` no Drive; rotacionar `changelog_sessoes.md` (>10 sessões) para `arquivo_sessoes/AAAA-MM.md`.

## 10. Diretriz 3.18 — Proibição Estrita de Google Docs e Google Sheets (Texto Puro `.md`/`.yml`/`.csv` e Update In-Place)
- **REGRA INEGOCIÁVEL:** É terminantemente PROIBIDO criar, salvar, converter ou exportar arquivos para **Google Docs** (`application/vnd.google-apps.document`), **Google Sheets** (`application/vnd.google-apps.spreadsheet`) ou **Google Slides** dentro do ecossistema CscTracker.
- **PADRÃO EXCLUSIVO:** Markdown puro (`text/markdown`, `.md`), YAML (`text/yaml`, `.yml`/`.yaml`) e CSV (`text/csv`, `.csv`).
- **ANTI-DUPLICATAS NO DRIVE:**
  - Para atualizar arquivos existentes: usar sempre `gdrive mutate update <FILE_ID> <CAMINHO_LOCAL> --mime-type "<MIME>"` (preserva ID e evita duplicatas).
  - Para criar arquivos inéditos: confirmar com `gdrive readonly ls <FOLDER_ID>` antes de rodar `gdrive mutate upload <CAMINHO_LOCAL> --parent <FOLDER_ID> --mime-type "<MIME>"`. Remover duplicatas acidentais com `gdrive mutate trash <OLD_ID>`.

## 11. Diretriz 3.20 — Testes Unitários Pragmáticos (Sem Meta de Cobertura)
- **Foco em Comportamento Real (KISS):** Proibido perseguir percentuais artificiais de cobertura. Testes existem para garantir que as coisas funcionem e não sofram regressões.
  - **Quando criar/atualizar:** Sempre em correções de **bugs** (teste de regressão cobrindo o cenário que falhou) e em **regras de negócio, cálculos, parsers, filtros e queries críticas**.
  - **Quando evitar:** Não criar testes artificiais para UI pura/layouts, composes Docker ou código trivial onde o custo de mock supere o benefício (para lógica de UI, testar apenas a função/regra por trás dela).
- **Execução Obrigatória antes da Revisão:** Se o repositório possuir suíte de testes (`pytest`, `unittest`, `./gradlew test`, etc.), executá-la após editar o código e informar o resultado real (ex.: `43/43 passando`) no resumo da revisão conjunta. Se algum teste falhar ou não puder rodar no ambiente, reportar explicitamente (**proibido** apagar ou pular/`skip` testes existentes sem autorização).
- **Isolamento Offline:** Testes unitários devem ser rápidos e determinísticos, sem bater em APIs externas/pagas (Gemini), bancos de produção (ClickHouse) ou dispositivos físicos (usar mocks simples ou SQLite em memória/temporário).
- **Escopo Local:** Particularidades de comando ou fixtures de um repositório ficam em seu `.ai-memory/decisions.md` (`[PINNED]`).