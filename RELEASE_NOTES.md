Release Notes - v26.47.002

Abaixo estão as notas de atualização para a versão v26.47.002. Esta versão consolida importantes avanços arquiteturais no agendador de tarefas, com destaque para a validação consistente de payloads JSON e enriquecimento do rastreamento de execuções via correlation ID. Adicionalmente, foram aplicadas otimizações estruturais no pipeline de CI/CD, unificação de dependências essenciais e remoção de módulos legados para garantir maior confiabilidade operacional.

## 🚀 Features
* **Agendador de Tarefas:** Implementação da propagação de Correlation ID nos cabeçalhos das requisições HTTP disparadas pelos jobs, garantindo rastreabilidade distribuída de ponta a ponta e simplificando a depuração entre múltiplos serviços (`ScheduleJobs.py`).
  * *Commit: `f019043` | Autor: @carlosschwalm*
* **Monitoramento e Observabilidade:** Integração de rotinas para rastreamento aprofundado dos jobs e enriquecimento de logs durante o fluxo de execução em ScheduleJobs, melhorando a auditoria operacional e o diagnóstico de falhas no agendamento (`ScheduleJobs.py`).
  * *Commit: `64d9f47` | Autor: @carlosschwalm*
* **Integração Contínua:** Implementação e estruturação de pipelines automatizados de deployment e versionamento semântico no Jenkinsfile, suportando automação de builds e governança no ciclo de publicação de artefatos (`Jenkinsfile`).
  * *Commit: `3d27c02` | Autor: @krlsedu*
* **Configuração de Execução:** Configuração e envio de cabeçalhos customizados nas requisições do agendador, juntamente com o ajuste das configurações de deploy em containers Docker (`ScheduleJobs.py`).
  * *Commit: `e17daf5` | Autor: @krlsedu*
* **Inicialização do Projeto:** Criação da estrutura base da aplicação, contendo os módulos core de agendamento de jobs, repositório de persistência, interceptores de requisição e orquestração via Docker Compose (`ScheduleJobs.py`).
  * *Commit: `ade0ec6` | Autor: @krlsedu*

## 🐛 Fixes
* **Validação de Payload:** Implementação do parsing preventivo do corpo do job como JSON e validações robustas no tratamento de requisições HTTP, evitando falhas de serialização e execuções com formato de payload corrompido (`ScheduleJobs.py`).
  * *Commit: `91388d5` | Autor: @carlos.schwalm*
* **Tratamento de Exceções:** Aprimoramento do tratamento de erros e exceções durante o ciclo de vida dos jobs agendados, garantindo maior estabilidade e mitigando quebras não tratadas na esteira de jobs (`ScheduleJobs.py`).
  * *Commit: `38d8bda` | Autor: @krlsedu*
* **Autenticação:** Correção na formatação e passagem de tokens de autorização nas chamadas do agendador de tarefas, prevenindo falhas de acesso durante o disparo dos endpoints alvo (`ScheduleJobs.py`).
  * *Commit: `c2be67e` | Autor: @krlsedu*

## 🔧 Chore
* **Automação de Build:** Atualização da versão do pacote e sincronização do histórico do projeto para geração da build 26.47.001 (`version.txt`).
  * *Commit: `ae8db64` | Autor: @krlsedu*
* **Automação de Build:** Atualização do versionamento para a geração da build 24.03.001 (`version.txt`).
  * *Commit: `9102fc5` | Autor: @krlsedu*
* **Dependências:** Atualização da dependência `csctracker-py-core` para a versão 24.03.001, mantendo alinhamento com as funcionalidades compartilhadas do framework central (`requirements.txt`).
  * *Commit: `c04d9e1` | Autor: @root*
* **Automação de Build:** Atualização de identificador de versão de build para 24.02.013 (`version.txt`).
  * *Commit: `be62872` | Autor: @krlsedu*
* **Dependências:** Atualização da biblioteca `csctracker-queue-scheduler` para a versão 24.02.002 para incorporar correções e otimizações de fila (`requirements.txt`).
  * *Commit: `bb474af` | Autor: @root*
* **Automação de Build:** Incremento de controle de versão para execução da build 24.02.012 (`version.txt`).
  * *Commit: `3c44c12` | Autor: @krlsedu*
* **Dependências:** Atualização da biblioteca `csctracker-py-core` para a versão 24.02.024 para aplicação de patches do núcleo compartilhado (`requirements.txt`).
  * *Commit: `d9faee6` | Autor: @carlosschwalm*
* **Automação de Build:** Atualização do marcador de versão do projeto para a build 24.02.011 (`version.txt`).
  * *Commit: `a71893c` | Autor: @krlsedu*
* **Dependências:** Limpeza e atualização dos pacotes e metadados de dependências internas no requirements (`requirements.txt`).
  * *Commit: `daa7e29` | Autor: @carlosschwalm*
* **Automação de Build:** Incremento de versão para acionamento do pipeline de build 24.02.010 (`version.txt`).
  * *Commit: `db57f79` | Autor: @krlsedu*
* **Versionamento de Código:** Sincronização e resolução de conflitos por meio do merge da branch remota master (`requirements.txt`).
  * *Commit: `3169898` | Autor: @carlosschwalm*
* **Dependências:** Atualização da versão da biblioteca central `csctracker-py-core` no gerenciador de dependências (`requirements.txt`).
  * *Commit: `54dbcc0` | Autor: @carlosschwalm*
* **Automação de Build:** Avanço da numeração de build interna para a versão 24.02.009 (`version.txt`).
  * *Commit: `29c266c` | Autor: @krlsedu*
* **Versionamento de Código:** Integração de atualizações remotas via merge com o branch origin/master (`requirements.txt`).
  * *Commit: `21bc7be` | Autor: @carlosschwalm*
* **Refatoração:** Refatoração de rotinas no agendador de tarefas e atualização da biblioteca `csctracker-py-core` para padronização de interfaces (`ScheduleJobs.py`).
  * *Commit: `af6cd81` | Autor: @carlosschwalm*
* **Automação de Build:** Atualização do ponteiro de controle de build para 24.02.008 (`version.txt`).
  * *Commit: `99f7ffa` | Autor: @krlsedu*
* **Versionamento de Código:** Merge da branch master para sincronização de alterações locais (`requirements.txt`).
  * *Commit: `022a789` | Autor: @carlosschwalm*
* **Automação de Build:** Incremento de versão para execução da build 24.02.007 (`version.txt`).
  * *Commit: `aa8080b` | Autor: @krlsedu*
* **Versionamento de Código:** Sincronização de árvore de commits por meio de merge com o repositório remoto master (`requirements.txt`).
  * *Commit: `56ad11d` | Autor: @carlosschwalm*
* **Dependências:** Atualização da biblioteca `csctracker-py-core` para a versão 24.2.13 no manifesto de dependências (`requirements.txt`).
  * *Commit: `fa62611` | Autor: @carlosschwalm*
* **Automação de Build:** Atualização do identificador de versão de release para a build 24.02.006 (`version.txt`).
  * *Commit: `cd83778` | Autor: @krlsedu*
* **Versionamento de Código:** Realização de merge com as alterações da branch remota master (`requirements.txt`).
  * *Commit: `b1aa75d` | Autor: @carlosschwalm*
* **Automação de Build:** Incremento do número da build no arquivo de controle para a versão 24.02.005 (`version.txt`).
  * *Commit: `7766f1d` | Autor: @krlsedu*
* **Versionamento de Código:** Sincronização de branch via merge a partir de origin/master (`requirements.txt`).
  * *Commit: `b7cc4a0` | Autor: @carlosschwalm*
* **Dependências:** Atualização combinada dos pacotes fundamentais `csctracker-py-core` e `csctracker-queue-scheduler` (`requirements.txt`).
  * *Commit: `7196c4f` | Autor: @carlosschwalm*
* **Automação de Build:** Atualização de identificador de versão para execução da build 24.02.004 (`version.txt`).
  * *Commit: `aae03c9` | Autor: @krlsedu*
* **Versionamento de Código:** Incorporação de modificações remotas através de merge com a master (`requirements.txt`).
  * *Commit: `d5fa64f` | Autor: @carlosschwalm*
* **Dependências:** Ajuste e alinhamento da versão do pacote `csctracker-py-core` nas dependências do projeto (`requirements.txt`).
  * *Commit: `ba29bab` | Autor: @carlosschwalm*
* **Automação de Build:** Atualização de versão operacional da aplicação para a build 24.02.003 (`version.txt`).
  * *Commit: `3d052ac` | Autor: @krlsedu*
* **Dependências:** Atualização de versão da dependência compartilhada `csctracker-py-core` (`requirements.txt`).
  * *Commit: `f1e0a4e` | Autor: @carlosschwalm*
* **Automação de Build:** Incremento da versão da build para 24.02.002 (`version.txt`).
  * *Commit: `71d8f49` | Autor: @krlsedu*
* **Versionamento de Código:** Conflitos resolvidos e histórico sincronizado com o branch origin/master (`requirements.txt`).
  * *Commit: `aa37b02` | Autor: @krlsedu*
* **Dependências:** Atualização do versionamento de `csctracker-py-core` no requirements (`requirements.txt`).
  * *Commit: `efdf4b2` | Autor: @krlsedu*
* **Automação de Build:** Atualização de controle de versão da build para a numeração 24.02.001 (`version.txt`).
  * *Commit: `7d060f3` | Autor: @krlsedu*
* **Versionamento de Código:** Merge da branch de integração origin/master (`requirements.txt`).
  * *Commit: `5d0f6fa` | Autor: @krlsedu*
* **Dependências:** Adequação do apontamento da biblioteca `csctracker-py-core` no gerenciador de pacotes (`requirements.txt`).
  * *Commit: `45e9ae3` | Autor: @krlsedu*
* **Automação de Build:** Acionamento e atualização de tag para o build 24.01.008 (`version.txt`).
  * *Commit: `d872576` | Autor: @krlsedu*
* **Versionamento de Código:** Incorporação do estado recente do origin/master via merge (`requirements.txt`).
  * *Commit: `1289315` | Autor: @krlsedu*
* **Dependências:** Sincronização da versão de dependência de `csctracker-py-core` (`requirements.txt`).
  * *Commit: `dba3fe7` | Autor: @krlsedu*
* **Automação de Build:** Atualização do artefato de versão para acionamento do build 24.01.007 (`version.txt`).
  * *Commit: `30349e7` | Autor: @krlsedu*
* **Versionamento de Código:** Resolução e merge com as modificações de origin/master (`requirements.txt`).
  * *Commit: `e20b58a` | Autor: @krlsedu*
* **Dependências:** Bump de versão da biblioteca `csctracker-py-core` no arquivo de requirements (`requirements.txt`).
  * *Commit: `78296e2` | Autor: @krlsedu*
* **Automação de Build:** Registro e atualização da marcação de build 24.01.006 (`version.txt`).
  * *Commit: `f226946` | Autor: @krlsedu*
* **Versionamento de Código:** Sincronização do repositório local com a branch remota origin/master (`requirements.txt`).
  * *Commit: `5a2690c` | Autor: @krlsedu*
* **Dependências:** Atualização das referências do pacote `csctracker-py-core` no requirements (`requirements.txt`).
  * *Commit: `45df6bd` | Autor: @krlsedu*
* **Automação de Build:** Atualização de numeração de versão para a build 24.01.005 (`version.txt`).
  * *Commit: `2cdb646` | Autor: @krlsedu*
* **Dependências:** Bump de versão do `csctracker-py-core` para inclusão de correções upstream (`requirements.txt`).
  * *Commit: `1d9c9e8` | Autor: @krlsedu*
* **Automação de Build:** Ajuste da versão do aplicativo para acionamento da build 24.01.004 (`version.txt`).
  * *Commit: `c2c8e17` | Autor: @krlsedu*
* **Logging:** Atualização da dependência central e implementação de logs detalhados de monitoramento em ScheduleJobs (`ScheduleJobs.py`).
  * *Commit: `a476998` | Autor: @krlsedu*
* **Automação de Build:** Configuração do identificador de aplicação em app_name.txt e atualização para build 24.01.003 (`version.txt`).
  * *Commit: `b279606` | Autor: @krlsedu*
* **Pipeline de CI/CD:** Declaração do nome do repositório no Jenkinsfile e alinhamento de versão do core da aplicação (`Jenkinsfile`).
  * *Commit: `a07d00d` | Autor: @krlsedu*
* **Automação de Build:** Atualização do versionamento para a build 24.01.002 (`version.txt`).
  * *Commit: `ed74378` | Autor: @krlsedu*
* **Padronização:** Ajustes de formatação no arquivo principal de inicialização e sincronização da versão (`version.txt`).
  * *Commit: `44a0868` | Autor: @carlosschwalm*
* **Automação de Build:** Atualização da tag de versão para controle de build (`version.txt`).
  * *Commit: `9ade60c` | Autor: @krlsedu*
* **Padronização:** Refinamento de formatação de código no entrypoint app.py e consolidação de versão (`app.py`).
  * *Commit: `bdb9156` | Autor: @carlosschwalm*
* **Padronização:** Inserção de espaçamento estrutural antes da chamada da função principal no inicializador (`app.py`).
  * *Commit: `e9385e3` | Autor: @carlosschwalm*
* **Automação de Build:** Atualização do arquivo de versão para build inicial 24.01.001 (`version.txt`).
  * *Commit: `3778df1` | Autor: @krlsedu*
* **Arquitetura e Limpeza:** Refatoração profunda do projeto eliminando dependências desnecessárias, remoção dos módulos obsoletos Repository.py e Interceptor.py e consolidação das chamadas de agendamento (`ScheduleJobs.py`).
  * *Commit: `e9d2334` | Autor: @carlosschwalm*
* **Automação de Build:** Incremento de versão para geração do build 23.37.001 (`version.txt`).
  * *Commit: `51fa137` | Autor: @krlsedu*
* **Versionamento de Código:** Merge da branch de release com o ramo origin/master (`requirements.txt`).
  * *Commit: `36d58a1` | Autor: @krlsedu*
* **Automação de Build:** Atualização do marcador de versão para a build 23.35.004 (`version.txt`).
  * *Commit: `ffe048b` | Autor: @krlsedu*
* **Versionamento de Código:** Integração de modificações remotas a partir de origin/master (`requirements.txt`).
  * *Commit: `a3060c4` | Autor: @krlsedu*
* **Refatoração:** Ajuste técnico pontual no módulo de repositório (`Repository.py`).
  * *Commit: `3a8e55a` | Autor: @krlsedu*
* **Automação de Build:** Atualização de versão da build para 23.35.003 (`version.txt`).
  * *Commit: `ae2f9dc` | Autor: @krlsedu*
* **Versionamento de Código:** Mesclagem de alterações provenientes da branch master remota (`requirements.txt`).
  * *Commit: `fde59f2` | Autor: @krlsedu*
* **Configuração de CI/CD:** Atualização e padronização da variável SERVICE_NAME no pipeline de automação (`Jenkinsfile`).
  * *Commit: `8ad6a7c` | Autor: @krlsedu*
* **Automação de Build:** Registro de nova numeração para o build 23.35.002 (`version.txt`).
  * *Commit: `b556131` | Autor: @krlsedu*
* **Versionamento de Código:** Sincronização de branch via merge com a origem master (`requirements.txt`).
  * *Commit: `375b1d2` | Autor: @krlsedu*
* **Padronização:** Remoção de espaços em branco desnecessários para adequação às regras de linting (`Repository.py`).
  * *Commit: `806817a` | Autor: @krlsedu*
* **Automação de Build:** Atualização de tag interna de release para 23.35.001 (`version.txt`).
  * *Commit: `bbde5f9` | Autor: @krlsedu*
* **Versionamento de Código:** Merge de branches com atualizações de origin/master (`requirements.txt`).
  * *Commit: `ab4a388` | Autor: @krlsedu*
* **Pipeline de CI/CD:** Adequações nos passos de deploy automático e ajustes na camada de repositório (`Jenkinsfile`).
  * *Commit: `ac3da7c` | Autor: @krlsedu*
* **Automação de Build:** Criação e inicialização do arquivo version.txt para rastreamento de builds (`version.txt`).
  * *Commit: `4825c3a` | Autor: @krlsedu*
* **Pipeline de CI/CD:** Ajustes complementares na automação de versionamento e regras de publicação contínua (`Jenkinsfile`).
  * *Commit: `f6966fc` | Autor: @krlsedu*
* **Pipeline de CI/CD:** Implementação inicial da lógica de extração automática de versão e comandos de deploy (`Jenkinsfile`).
  * *Commit: `11d8ab7` | Autor: @krlsedu*