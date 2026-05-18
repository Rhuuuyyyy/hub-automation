# Prompt de Auditoria de Infraestrutura, Arquitetura e Cibersegurança

> Prompt reutilizável para auditar o Hub de Automações TI contra o OWASP Top 10,
> boas práticas de arquitetura e riscos de infraestrutura. Execute em qualquer
> sessão Claude com acesso ao repositório.

---

<Inputs>
<project_description>
Hub de Automações TI — Plataforma interna de monitoramento e automação de processos
de TI da Dexian, construída com Python 3.10+ / FastAPI / Uvicorn.

Módulos ativos:
1. **Contratações GLPI** (`automations/contratacoes/glpi_sync.py`)
   - Worker daemon que faz polling da API REST Verdanadesk (baseada em GLPI v2.3)
     a cada POLLING_INTERVAL segundos (padrão 300 s).
   - Autenticação: OAuth2 Password Grant — POST `/api.php/token` com client_id,
     client_secret, username e password. Token Bearer renovado automaticamente em 401.
   - Busca todos os tickets paginados (PAGE_SIZE=500), filtra por categoria e status
     em Python, persiste snapshot em `database/contratacoes.json` via escrita atômica
     (`.tmp` → `os.replace`).
   - Verificação read-only de tarefa de Termo de Responsabilidade por ticket
     (`verificar_tarefa_termo`), com cache em memória de termos já concluídos.
   - Histórico diário de KPIs persistido em `database/historico.json` (janela 60 dias).

2. **Análise de Usuários** (`automations/usuarios/usuarios_sync.py`)
   - Varredura manual (via POST `/api/analise-usuarios/sync`) do inventário Verdanadesk.
   - Produz 3 relatórios: inativos com máquinas, CC divergente, múltiplos responsáveis.
   - Reutiliza `ClienteGLPI` para autenticação OAuth2.
   - Persiste resultado em `database/usuarios_analise.json`.

Backend (`backend/api.py`):
- FastAPI `APIRouter` montado em `/hub/automacoes/api`.
- Lê arquivos JSON da pasta `database/` e os serve como respostas HTTP.
- Exportação de chamados em CSV, XLSX (openpyxl) e PDF (reportlab).
- Endpoint `POST /api/analise-usuarios/sync` dispara análise em `BackgroundTasks`.

Frontend (`frontend/index.html`):
- SPA em HTML5 + Tailwind CSS (CDN) + JavaScript Vanilla.
- Sem build step, sem Node.js, sem dependências de runtime.
- Polling leve (intervalo configurável: 10 s a 5 min) contra a API REST local.

Configuração:
- Todas as credenciais via variáveis de ambiente (`.env`, nunca commitado).
- `.env.example` com placeholders; `.gitignore` cobre `.env` e `database/*.json`.

Persistência:
- Arquivos JSON simples em `database/` (gerada em runtime, fora do git).
- Sem banco de dados relacional, sem Redis, sem fila de mensagens.

Estrutura de execução:
- `python run.py` instala deps, sobe daemon thread do worker GLPI e inicia
  `uvicorn main:app` na porta configurada (padrão 8000).
- Thread principal = Uvicorn/FastAPI; thread daemon = worker de sync.
- Sem Docker, sem orquestração, sem CI/CD pipeline formal.
</project_description>

<target_environment>
Implantação on-premises em servidor Linux interno da Dexian (rede corporativa privada).
- Acesso restrito à rede interna — não exposto à internet pública.
- Python 3.10+ em ambiente virtual (`.venv`).
- Porta 8000 (configurável via `WEB_PORT`), sem TLS/HTTPS na camada de aplicação
  (pressupõe terminação SSL no proxy reverso corporativo, se houver).
- Usuário único de integração com credenciais OAuth2 armazenadas em `.env`.
- Sistema operacional: Linux (distribuição não especificada); sem container runtime.
- Sem pipeline de CI/CD automatizado, sem scanner SAST/DAST integrado.
- Dependências Python instaladas via `pip` em tempo de execução pelo próprio `run.py`.
- Acesso ao Verdanadesk via HTTPS pela rede interna.
- Usuários do dashboard: equipe de TI interna (não usuários externos).
</target_environment>
</Inputs>

<Instructions>
Você é um Arquiteto de Software Sênior e Engenheiro Chefe de Cibersegurança (CISO) com décadas de experiência na construção, auditoria e proteção de sistemas de missão crítica em larga escala. Sua especialidade é dissecar bases de código completas, identificar gargalos arquitetônicos e descobrir vulnerabilidades de segurança complexas que ferramentas automatizadas geralmente não detectam.

Sua tarefa é realizar uma auditoria "cirúrgica" e abrangente da infraestrutura, arquitetura de software e postura de cibersegurança do projeto ao qual você tem acesso.

**DIRETRIZES DE ANÁLISE:**

1. **Investigação Profunda:** Não se limite a análises superficiais. Examine como os componentes se comunicam, como o estado é gerenciado, como a infraestrutura é provisionada (IaC) e como os dados fluem através do sistema.
2. **Foco em Cibersegurança:** Avalie o projeto contra o OWASP Top 10 e CWEs relevantes. Procure ativamente por falhas de injeção, quebra de autenticação, exposição de dados sensíveis, gerenciamento inseguro de segredos, e vulnerabilidades na cadeia de suprimentos (dependências).
3. **Foco em Arquitetura:** Avalie a escalabilidade, resiliência (pontos únicos de falha), modularidade (acoplamento e coesão), e a adequação do stack tecnológico para o ambiente alvo.
4. **Evidências Concretas:** Sempre que apontar uma falha ou oportunidade de melhoria, referencie arquivos específicos, trechos de código, ou configurações presentes no projeto.

**PROCESSO DE TRABALHO:**

Antes de gerar seu relatório final, você DEVE usar a tag `<analysis_process>` para estruturar seu raciocínio. Dentro desta tag:
- Mapeie mentalmente a arquitetura geral do sistema.
- Identifique as superfícies de ataque (APIs, endpoints públicos, integrações de banco de dados).
- Analise os fluxos de autenticação e autorização.
- Revise os arquivos de infraestrutura (Dockerfiles, Terraform, Kubernetes manifests, CI/CD pipelines).
- Debata consigo mesmo sobre os trade-offs das decisões arquitetônicas encontradas.

**FORMATO DE SAÍDA:**

Após concluir sua análise interna, apresente seu relatório dentro da tag `<audit_report>`, seguindo estritamente esta estrutura:

**1. Resumo Executivo**
Uma visão geral de alto nível sobre a saúde do projeto, destacando os riscos mais críticos e a viabilidade geral da arquitetura.

**2. Avaliação de Arquitetura e Infraestrutura**
- **Padrões e Design:** Análise do design atual (ex: microsserviços, monolito, serverless) e sua adequação.
- **Escalabilidade e Resiliência:** Identificação de gargalos e pontos de falha.
- **Qualidade do Código e Manutenibilidade:** Avaliação do acoplamento, coesão e dívida técnica.
- **Infraestrutura (IaC) e Deploy:** Avaliação de contêineres, orquestração e pipelines de CI/CD.

**3. Avaliação de Cibersegurança**
- Para cada vulnerabilidade ou risco encontrado, forneça:
  - **Justificativa/Descoberta:** Explique o que está errado e referencie o arquivo/código.
  - **Impacto:** O que um atacante poderia fazer explorando isso.
  - **Severidade:** (Crítica, Alta, Média, Baixa). *Nota: A justificativa deve sempre preceder a severidade.*
- **Gerenciamento de Segredos e Dados:** Como os dados sensíveis e chaves de API estão sendo tratados.
- **Cadeia de Suprimentos:** Riscos em dependências e bibliotecas de terceiros.

**4. Matriz de Ação e Recomendações**
Uma lista priorizada (do mais crítico para o menos crítico) de passos acionáveis que a equipe de engenharia deve tomar para corrigir os problemas encontrados, incluindo sugestões de refatoração de arquitetura e mitigação de segurança.

Comece sua resposta diretamente com a tag `<analysis_process>`.
</Instructions>
