# CscTracker — Project Guidelines (JetBrains Junie & AI Agents)

1. **Economia de Tokens e Princípio KISS:** Economize tokens de pensamento, seja direto, rápido e siga rigorosamente o princípio KISS (Keep It Simple, Stupid). Evite projeções mirabolantes, over-engineering, abstrações prematuras ou refatorações fora do escopo solicitado.
2. **Leitura Obrigatória na Partida (`Recall`):** Antes de propor ou alterar qualquer código neste repositório, leia obrigatoriamente:
   - `.ai-memory/handoff.md` (estado atual do projeto, onde paramos na última sessão e abordagens que já foram testadas e falharam).
   - `.ai-memory/gotchas.md` (armadilhas conhecidas deste repositório que não devem ser repetidas).
   - `.ai-memory/decisions.md` (cláusulas pétreas `[PINNED]` e decisões arquiteturais invioláveis deste projeto).
   - `.agents/skills/csctracker/SKILL.md` (protocolo mestre global de governança e fidelidade absoluta do ecossistema CscTracker).
3. **Fidelidade Absoluta (Diretriz 3.13):** Nunca remova flags de infraestrutura, configurações de compose, variáveis de ambiente ou comportamentos existentes por conta própria para "limpar" código. Na dúvida, pergunte ao desenvolvedor.
4. **Atualização na Saída (`Handoff`):** Ao finalizar qualquer implementação ou correção, atualize `.ai-memory/handoff.md` (e `.ai-memory/gotchas.md` / `.ai-memory/decisions.md` se houver novo aprendizado técnico) resumindo o que foi alterado e os arquivos tocados.
