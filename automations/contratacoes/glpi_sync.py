"""
automations/contratacoes/glpi_sync.py — Worker de sincronização Verdanadesk → database.

Polling contínuo da fila de chamados da API Verdanadesk (baseada em GLPI).
A cada ciclo, sobrescreve database/contratacoes.json com o snapshot atual dos
chamados ativos. O dashboard web lê esse arquivo via API REST (backend/api.py).

Autenticação: OAuth2 Password Grant (migração da autenticação nativa GLPI).
    O token Bearer é obtido em /api.php/token e enviado em todas as chamadas
    como "Authorization: Bearer <access_token>".

Variáveis de ambiente (.env na raiz do repositório):
    API_URL                              — URL base da API versionada (ex: /api.php/v2.3)
    OAUTH_CLIENT_ID                      — Client ID registrado na plataforma
    OAUTH_CLIENT_SECRET                  — Client Secret correspondente
    OAUTH_USERNAME                       — E-mail/login do usuário de integração
    OAUTH_PASSWORD                       — Senha do usuário de integração
    OAUTH_TOKEN_URL                      — (opcional) URL do endpoint de token OAuth2;
                                           derivado automaticamente de API_URL se omitido
    CATEGORIA_CONTRATACAO_IDS            — IDs de categoria (vírgula) a monitorar
    CATEGORIA_CONTRATACAO_ID_WITH_ASSETS — ID da categoria com envio de equipamento
    POLLING_INTERVAL                     — Segundos entre varreduras (padrão: 300)
"""

# ============================================================================
# Bootstrap — executa ANTES de qualquer import de terceiros.
# ============================================================================
import subprocess
import sys


def _modulo_ausente(nome: str) -> bool:
    """
    Verifica se um módulo Python pode ser importado sem de fato importá-lo
    de forma permanente.

    Usa __import__() (a função de baixo nível por trás do 'import') dentro de
    um try/except: se o import falhar com ImportError, o módulo não está
    instalado. Isso é feito ANTES de qualquer import de terceiros para que o
    bootstrap consiga identificar o que precisa instalar.

    Retorna True se o módulo estiver ausente, False se já estiver disponível.
    """
    try:
        __import__(nome)
        return False
    except ImportError:
        return True


def _bootstrap() -> None:
    """
    Garante que as dependências mínimas estejam instaladas antes de qualquer
    import de terceiros acontecer.

    Por que existe isso:
        Este script pode ser executado diretamente (python glpi_sync.py) em
        ambientes sem venv configurado. Sem o bootstrap, o Python lançaria
        ModuleNotFoundError imediatamente ao tentar importar 'requests' ou
        'dotenv', sem mensagem útil. O bootstrap resolve isso automaticamente.

    Como funciona:
        1. Verifica quais módulos da lista _DEPS estão ausentes usando
           _modulo_ausente() — que chama __import__ sem efeito colateral.
        2. Se tudo já estiver instalado, retorna imediatamente (zero custo).
        3. Se houver ausências, chama 'pip install' via subprocess.check_call(),
           usando sys.executable para garantir que o pip correto seja chamado
           (o do mesmo Python que está rodando este script, mesmo dentro de venv).
        4. Após a instalação, reinicia o processo inteiro com subprocess.run()
           passando sys.argv (os mesmos argumentos originais). Isso é necessário
           porque módulos instalados em runtime não ficam disponíveis na sessão
           atual de importação — o restart limpa o estado e permite o import.
        5. sys.exit() propaga o código de saída do processo filho.

    Por que reiniciar em vez de importar dinâmico:
        importlib.import_module() só funciona para módulos já presentes no
        sys.path no momento da chamada. Pip instala no site-packages, mas o
        interpretador já carregou o sys.path — o reinício garante que o novo
        pacote seja encontrado corretamente.
    """
    _DEPS = {
        "requests": "requests>=2.31.0",
        "dotenv":   "python-dotenv>=1.0.0",
    }
    ausentes = [pkg for mod, pkg in _DEPS.items() if _modulo_ausente(mod)]
    if not ausentes:
        return
    print(f"[bootstrap] Instalando dependências ausentes: {', '.join(ausentes)}")
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet"] + ausentes
        )
    except subprocess.CalledProcessError:
        print("[bootstrap] ERRO: falha ao instalar dependências.")
        sys.exit(1)
    print("[bootstrap] Instalação concluída. Reiniciando script...\n")
    resultado = subprocess.run([sys.executable] + sys.argv)
    sys.exit(resultado.returncode)


_bootstrap()

# ============================================================================
# Imports
# ============================================================================
import html as _html_mod   # stdlib — decodifica entidades HTML (&nbsp; &amp; etc.)
import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests             # HTTP client de terceiros (instalado pelo bootstrap)
from dotenv import load_dotenv  # leitura de arquivo .env (instalado pelo bootstrap)

# ============================================================================
# Paths — resolvidos relativos à raiz do repo, não ao cwd
# ============================================================================

# Path(__file__).resolve() retorna o caminho absoluto deste arquivo.
# .parents[2] sobe dois níveis: contratacoes/ → automations/ → raiz do repo.
# Isso garante que os caminhos funcionem independentemente do diretório de
# trabalho atual (cwd) de quem chamou o script.
_REPO_ROOT    = Path(__file__).resolve().parents[2]
DATABASE_PATH  = _REPO_ROOT / "database" / "contratacoes.json"
HISTORICO_PATH = _REPO_ROOT / "database" / "historico.json"

load_dotenv(_REPO_ROOT / ".env")

# ============================================================================
# Logging
# ============================================================================

# logging.basicConfig configura o logger raiz do Python com formato e nível
# globais. Nível INFO significa que DEBUG é suprimido por padrão — útil em
# produção. Para ver os logs de diagnóstico do GLPI (conteúdo bruto das
# tarefas), mude para logging.DEBUG aqui ou via variável de ambiente.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("glpi_sync")

HTTP_TIMEOUT = 30  # segundos — timeout para todas as chamadas HTTP ao GLPI

# ============================================================================
# Constantes de domínio
# ============================================================================

# O GLPI representa status de chamados como inteiros em sua API REST.
# Este mapeamento traduz os IDs para strings legíveis em PT-BR usadas no
# dashboard. IDs 2 e 3 ("Processing assigned" e "Processing planned") são
# tratados como o mesmo status visual "Em Atendimento" para simplificar a UI.
STATUS_MAP: dict[int, str] = {
    1: "Novo",
    2: "Em Atendimento",
    3: "Em Atendimento",
    4: "Pendente",
    5: "Solucionado",
    6: "Fechado",
}

# frozenset (conjunto imutável e hashável) para checagem O(1) de pertencimento.
# Apenas chamados com esses status aparecem no dashboard — Solucionado (5) e
# Fechado (6) são excluídos na filtragem dentro de buscar_chamados_ativos().
STATUSES_ATIVOS = frozenset({1, 2, 3, 4})

# Substring de busca intencionalmente curta para resistir a variações de
# maiúsculas, pontuação e prefixos que o GLPI possa adicionar ao texto da
# tarefa (ex: "Enviar termo de responsabilidade.", "<p>Termo de responsabilidade</p>").
# Busca por substring (in) em vez de igualdade exata para máxima tolerância.
_TEXTO_TAREFA_TERMO   = "termo de responsabilidade"
_GLPI_TASK_STATE_DONE = 2  # valor numérico que o GLPI usa para "tarefa concluída"

# Padrão para extração de "Data de início: DD/MM/YYYY" do HTML do chamado.
# Compilado uma vez no módulo para reutilização em todos os tickets.
_PADRAO_DATA_INICIO = re.compile(
    r"(?:Data\s+(?:de\s+)?)?\bin[íi]cio\b\s*:{0,2}\s*(?:<[^>]+>|&nbsp;|\s)*(\d{2}[-/]\d{2}[-/]\d{4})",
    re.IGNORECASE,
)


def _extrair_data_inicio(html: str) -> str:
    """Extrai DD/MM/YYYY de um bloco HTML, ou retorna ''."""
    m = _PADRAO_DATA_INICIO.search(html or "")
    return m.group(1).replace("-", "/") if m else ""


def _limpar_html(texto: str) -> str:
    """
    Remove marcação HTML de uma string e retorna texto plano normalizado.

    Por que é necessário:
        O GLPI armazena o conteúdo das tarefas como HTML rico — o editor
        visual do sistema envolve o texto em tags como <p>, <br>, <strong>
        e também usa entidades HTML (&nbsp;, &amp;, &lt;) para caracteres
        especiais. Se comparássemos diretamente com a string bruta, uma
        busca por "termo de responsabilidade" falharia em textos como
        "<p>Termo&nbsp;de responsabilidade.</p>".

    Pipeline de limpeza (ordem importa):
        1. html.unescape()  — converte entidades HTML em caracteres reais
           antes de qualquer remoção de tags, para que &nbsp; não vire
           espaço apenas depois de remover tags e cause comparações erradas.
        2. re.sub(<[^>]+>)  — remove todas as tags HTML restantes. O padrão
           [^>]+ captura tudo que está entre < e >, incluindo atributos.
           Substituído por " " (espaço) para não fundir palavras adjacentes
           como "<b>termo</b>de" → "termo de" (não "termode").
        3. re.sub(\\s+)     — colapsa sequências de espaços, tabs e quebras
           de linha em um único espaço e remove espaços nas bordas (.strip()).
           Necessário porque a remoção das tags deixa múltiplos espaços.
    """
    texto = _html_mod.unescape(texto)           # &nbsp; &amp; &lt; etc → caracteres reais
    texto = re.sub(r"<[^>]+>", " ", texto)      # remove tags restantes
    texto = re.sub(r"\s+", " ", texto).strip()  # colapsa espaços/quebras de linha
    return texto


# ============================================================================
# Configuração
# ============================================================================


def carregar_configuracoes() -> dict[str, Any]:
    """
    Lê as variáveis de ambiente (carregadas do .env pelo load_dotenv acima),
    valida as obrigatórias e retorna um dicionário de configuração normalizado.

    Por que usar variáveis de ambiente em vez de hardcode:
        Credenciais como tokens de API nunca devem estar no código-fonte. O
        padrão de "12-factor app" (https://12factor.net) define variáveis de
        ambiente como a forma correta de injetar configuração sensível. O
        arquivo .env é ignorado pelo git (.gitignore) e existe apenas localmente.

    Migração para OAuth2 (Verdanadesk v2.3):
        As variáveis legadas GLPI_URL/GLPI_USER_TOKEN/GLPI_APP_TOKEN foram
        substituídas pelo fluxo OAuth2 Password Grant:
            API_URL             — URL base da API versionada (ex: /api.php/v2.3)
            OAUTH_CLIENT_ID     — Client ID registrado na plataforma
            OAUTH_CLIENT_SECRET — Client Secret correspondente
            OAUTH_USERNAME      — E-mail/login do usuário de integração
            OAUTH_PASSWORD      — Senha do usuário de integração

    Derivação automática de OAUTH_TOKEN_URL:
        O endpoint de autenticação OAuth2 fica em /api.php/token — um nível
        acima da URL versionada. Derivamos automaticamente removendo o sufixo
        de versão (/vX.Y) com regex. O env var OAUTH_TOKEN_URL pode sobrescrever
        esse comportamento caso a instância use um caminho diferente.

    Validação de obrigatórias:
        Em vez de deixar o script falhar silenciosamente mais tarde com um
        AttributeError ou KeyError, verificamos explicitamente quais vars estão
        ausentes e encerramos com sys.exit(1) e mensagem clara.

    Tratamento de CATEGORIA_CONTRATACAO_IDS:
        Aceita múltiplos IDs separados por vírgula (ex: "152,153"). O split +
        strip remove espaços acidentais ao redor das vírgulas. O resultado é
        uma lista de strings — mantida como string porque a Search API do GLPI
        espera o valor como string nos query params.

    Tratamento de CATEGORIA_CONTRATACAO_ID_WITH_ASSETS:
        Variável opcional. Se ausente ou vazia, cat_id_com_ativo é None e o
        controle de Termo fica desativado para todos os chamados.

    rstrip("/") na URL:
        Evita URLs com barra dupla como ".../api.php/v2.3//Ticket"
        que causariam 404 na API.
    """
    api_url       = (os.getenv("API_URL") or "").strip().rstrip("/")
    client_id     = (os.getenv("OAUTH_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("OAUTH_CLIENT_SECRET") or "").strip()
    username      = (os.getenv("OAUTH_USERNAME") or "").strip()
    password      = (os.getenv("OAUTH_PASSWORD") or "").strip()

    ausentes = [
        nome
        for nome, val in [
            ("API_URL", api_url),
            ("OAUTH_CLIENT_ID", client_id),
            ("OAUTH_CLIENT_SECRET", client_secret),
            ("OAUTH_USERNAME", username),
            ("OAUTH_PASSWORD", password),
        ]
        if not val
    ]
    if ausentes:
        logger.critical("Variáveis de ambiente obrigatórias ausentes: %s", ", ".join(ausentes))
        sys.exit(1)

    # Deriva o token URL removendo o segmento de versão (/v2.3, /v10, etc.).
    # Pode ser sobrescrito explicitamente via OAUTH_TOKEN_URL no .env.
    token_url = (os.getenv("OAUTH_TOKEN_URL") or "").strip()
    if not token_url:
        token_url = re.sub(r"/v[\d.]+$", "/token", api_url)
        if token_url == api_url:
            # Nenhum sufixo de versão reconhecível — sobe um nível e adiciona /token
            token_url = api_url.rsplit("/", 1)[0] + "/token"
    logger.info("Token URL OAuth2: %s", token_url)

    cat_raw = os.getenv("CATEGORIA_CONTRATACAO_IDS", "")
    categoria_ids = [c.strip() for c in cat_raw.split(",") if c.strip()]
    if not categoria_ids:
        logger.critical("CATEGORIA_CONTRATACAO_IDS não definida no .env.")
        sys.exit(1)

    logger.info("Categorias configuradas: %s", categoria_ids)

    cat_ativo_raw = os.getenv("CATEGORIA_CONTRATACAO_ID_WITH_ASSETS", "").strip()
    cats_com_ativo: set[int] = set()
    if cat_ativo_raw:
        for parte in cat_ativo_raw.split(","):
            parte = parte.strip()
            if not parte:
                continue
            try:
                cats_com_ativo.add(int(parte))
            except ValueError:
                logger.warning(
                    "CATEGORIA_CONTRATACAO_ID_WITH_ASSETS: valor inválido '%s'. Ignorando.", parte
                )
    if cats_com_ativo:
        logger.info("Categorias com ativo (Termo): %s", sorted(cats_com_ativo))

    return {
        "API_URL":             api_url,
        "OAUTH_TOKEN_URL":     token_url,
        "OAUTH_CLIENT_ID":     client_id,
        "OAUTH_CLIENT_SECRET": client_secret,
        "OAUTH_USERNAME":      username,
        "OAUTH_PASSWORD":      password,
        "POLLING_INTERVAL":    os.getenv("POLLING_INTERVAL", "300"),
        "CATEGORIA_IDS":       categoria_ids,
        "CATS_COM_ATIVO":      cats_com_ativo,
    }


# ============================================================================
# Cliente GLPI
# ============================================================================

class ClienteGLPI:
    """
    Cliente REST para a API Verdanadesk (GLPI v2.3) com gerenciamento automático
    de sessão OAuth2.

    Fluxo de autenticação OAuth2 Password Grant:
        1. POST para o token endpoint (/api.php/token) com client_id, client_secret,
           username e password no corpo application/x-www-form-urlencoded.
        2. A API retorna {"access_token": "...", "token_type": "Bearer", "expires_in": N}.
        3. O access_token é enviado em todas as chamadas subsequentes via header
           "Authorization: Bearer <access_token>".
        4. Quando a API retorna 401 ou 403, o token é descartado e um novo é
           obtido automaticamente via _renovar_sessao(), sem interromper o ciclo.
    """

    def __init__(
        self,
        base_url: str,
        token_url: str,
        client_id: str,
        client_secret: str,
        username: str,
        password: str,
        categoria_ids: list[str],
        cats_com_ativo: set[int] | None = None,
    ) -> None:
        """
        Inicializa o cliente e abre imediatamente uma sessão autenticada via OAuth2.

        Por que requests.Session() em vez de requests.get() direto:
            Session() reutiliza a mesma conexão TCP (HTTP keep-alive) entre
            chamadas consecutivas, reduzindo latência e overhead de handshake
            TLS. Também permite definir headers padrão uma única vez via
            session.headers, que são enviados automaticamente em todas as
            requisições — conveniente para o Bearer token que é fixo durante
            toda a sessão.

        Caches em memória:
            _cache_usuarios:    dict[int, str] — evita chamar GET /User/{id}
                                repetidamente para o mesmo requerente ao longo
                                de um ciclo. Reset natural ao reiniciar o worker.
            _termos_concluidos: set[int]       — tickets cujo termo já foi
                                confirmado como concluído. Evita re-chamar a
                                API de TicketTask a cada varredura para tickets
                                que já estão OK. set usa hashing O(1) para lookup.

        _autenticar_oauth() é chamado no __init__ para que o objeto já esteja
        pronto para uso imediatamente após a construção.
        """
        self.base_url = base_url
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.categoria_ids = categoria_ids
        self.cats_com_ativo: set[int] = cats_com_ativo or set()
        self.access_token: str | None = None
        self._session = requests.Session()
        self._cache_usuarios: dict[int, str] = {}
        self._termos_concluidos: set[int] = set()
        self._autenticar_oauth()

    # --- Gerenciamento de sessão -------------------------------------------

    def _atualizar_headers(self) -> None:
        """
        Reconstrói os headers padrão da Session com o Bearer token atual.

        Por que reconstruir em vez de atualizar:
            session.headers.clear() + update() garante que nenhum header
            obsoleto persista de um estado anterior. Isso é especialmente
            importante após renovação do token: o token expirado é descartado
            completamente antes de o novo ser inserido.

        Por que NÃO definir Content-Type aqui:
            Content-Type: application/json em requests GET faz servidores como
            o Verdanadesk v2.3 tentarem parsear um corpo JSON inexistente,
            retornando 400 "Corpo JSON inválido". O header só faz sentido em
            POST/PUT/PATCH e é definido explicitamente em _post().

        Authorization: Bearer é adicionado condicionalmente:
            Durante a autenticação inicial, access_token ainda é None. Nesse
            ponto a Session não deve carregar um header Authorization — o
            próprio request de token OAuth2 usa client_id/secret no body,
            não no header. Após a autenticação, o Bearer é inserido e
            propagado automaticamente para todas as chamadas subsequentes.
        """
        self._session.headers.clear()
        if self.access_token:
            self._session.headers["Authorization"] = f"Bearer {self.access_token}"

    def _autenticar_oauth(self) -> None:
        """
        Autentica com a API via OAuth2 Password Grant e armazena o access_token.

        Fluxo OAuth2 Password Grant:
            Este fluxo é o adequado para integrações server-to-server onde as
            credenciais do usuário de serviço são conhecidas antecipadamente.
            O script nunca redireciona o usuário para login — ele envia as
            credenciais diretamente no corpo do request ao token endpoint.

        Por que application/x-www-form-urlencoded em vez de JSON:
            O padrão OAuth2 (RFC 6749 §4.3) define que o token endpoint DEVE
            aceitar os parâmetros no corpo como form-encoded. O argumento
            `data=payload` no requests.post() serializa automaticamente o dict
            nesse formato. Usar `json=payload` seria fora do padrão e a maioria
            dos servidores OAuth2 rejeita.

        Limpeza de Content-Type antes do POST:
            _atualizar_headers() define Content-Type: application/json para
            as chamadas de API normais. Aqui sobrescrevemos para application/
            x-www-form-urlencoded só para este request de autenticação,
            sem chamar _atualizar_headers() (que exigiria access_token pronto).

        Tratamento de falha crítica:
            Se a autenticação falhar (credenciais inválidas, serviço offline,
            rede indisponível), o worker não conseguirá fazer absolutamente
            nada. Por isso encerra com sys.exit(1) em vez de lançar uma
            exceção que seria silenciada pelo loop principal.
        """
        payload = {
            "grant_type":    "password",
            "client_id":     self.client_id,
            "client_secret": self.client_secret,
            "username":      self.username,
            "password":      self.password,
            "scope":         "api",  # obrigatório para acessar endpoints REST da API v2.3
        }
        # Headers temporários apenas para o request de token (sem Bearer ainda)
        auth_headers = {"Content-Type": "application/x-www-form-urlencoded"}
        try:
            response = self._session.post(
                self.token_url,
                data=payload,  # form-encoded conforme RFC 6749
                headers=auth_headers,
                timeout=HTTP_TIMEOUT,
            )
            if not response.ok:
                logger.error(
                    "Falha no login OAuth2. Status %s: %s",
                    response.status_code, response.text[:200],
                )
            response.raise_for_status()
            data = response.json()
            self.access_token = data.get("access_token")
            if not self.access_token:
                raise RuntimeError(
                    f"Resposta OAuth2 não contém access_token. Resposta: {data}"
                )
            self._atualizar_headers()
            logger.info("Autenticação OAuth2 bem-sucedida.")
        except requests.exceptions.RequestException as exc:
            logger.critical("Falha crítica ao autenticar na API: %s", exc)
            sys.exit(1)

    def _renovar_sessao(self) -> None:
        """
        Descarta o access_token atual e obtém um novo via OAuth2.

        Quando é chamado:
            _get() e _post() chamam este método ao receber HTTP 401 ou 403,
            que indicam que o access_token expirou. A duração padrão de um
            token OAuth2 Bearer é de 1 hora (expires_in=3600), após a qual
            qualquer chamada retorna 401 até que um novo token seja obtido.

        Por que zerar access_token antes:
            _atualizar_headers() verifica se access_token é None para decidir
            se inclui o header Authorization. Se não zerássemos antes,
            _autenticar_oauth() poderia enviar o Bearer expirado no request
            de login, dependendo da ordem das chamadas internas.
        """
        logger.warning("Token OAuth2 expirado. Renovando...")
        self.access_token = None
        self._autenticar_oauth()

    # --- Requisições --------------------------------------------------------

    def _get(self, url_completa: str) -> Any:
        """
        Executa um GET autenticado com uma tentativa automática de renovação
        de sessão em caso de falha de autenticação.

        Padrão de retry com auto-healing:
            A lógica é intencionalmente simples: tenta uma vez, verifica se
            o token expirou (401), renova e tenta mais uma vez. Não há backoff
            exponencial nem loop de retry — se falhar duas vezes, a exceção sobe
            para quem chamou. 403 NÃO dispara renovação: significa falta de
            permissão/scope, não token expirado — renovar o token não ajudaria
            e causaria um loop de re-autenticação inútil.

        Resposta vazia:
            O GLPI às vezes retorna HTTP 200 com body vazio para endpoints
            que não têm dados (ex: lista de tarefas de um ticket sem tarefas).
            response.text.strip() == "" é tratado como {} em vez de tentar
            response.json() que lançaria JSONDecodeError.

        raise_for_status():
            Transforma qualquer status 4xx/5xx em uma exceção
            requests.exceptions.HTTPError, que é capturada pelo chamador.
            Logar antes de lançar garante que o erro apareça no terminal
            com contexto da URL e do status code.
        """
        response = self._session.get(url_completa, timeout=HTTP_TIMEOUT)
        if response.status_code == 401:  # token expirado → renova; 403 = sem permissão, não renova
            self._renovar_sessao()
            response = self._session.get(url_completa, timeout=HTTP_TIMEOUT)
        if not response.ok:
            logger.error(
                "GET falhou [%s] %s: %s",
                url_completa, response.status_code, response.text[:300],
            )
        response.raise_for_status()
        return {} if not response.text.strip() else response.json()

    def _post(self, url_completa: str, payload: dict) -> Any:
        """
        Executa um POST autenticado com o mesmo padrão de auto-healing de _get().

        Formato do body:
            A API REST do GLPI exige que o payload de criação/atualização
            seja encapsulado dentro de uma chave "input":
                {"input": {campos do recurso}}
            Isso é uma convenção própria do GLPI, diferente do REST padrão.
            O encapsulamento é feito aqui para que os chamadores passem apenas
            o payload limpo sem se preocupar com esse detalhe de protocolo.

        raise_for_status() no bloco de erro:
            Diferente de _get(), o POST tem raise_for_status() dentro do
            if not response.ok para garantir que a exceção só seja lançada
            após o log — assim o erro sempre aparece com contexto antes de
            propagar para o chamador.
        """
        body = {"input": payload}
        response = self._session.post(url_completa, json=body, timeout=HTTP_TIMEOUT)
        if response.status_code == 401:  # token expirado → renova; 403 = sem permissão, não renova
            self._renovar_sessao()
            response = self._session.post(url_completa, json=body, timeout=HTTP_TIMEOUT)
        if not response.ok:
            logger.error(
                "POST falhou [%s] %s: %s",
                url_completa, response.status_code, response.text[:300],
            )
            response.raise_for_status()
        return {} if not response.text.strip() else response.json()

    # --- Endpoints de negócio -----------------------------------------------

    def buscar_chamados_ativos(self) -> list[dict[str, Any]]:
        """
        Busca todos os chamados ativos das categorias configuradas.

        O filtro RSQL da API v2.3 mostrou-se ineficaz (o mesmo total de 11.489
        tickets é retornado independentemente do filtro de categoria). Por isso,
        buscamos TODOS os tickets de uma só vez e filtramos por categoria e status
        em Python. Isso reduz as chamadas HTTP de ~690 (100/página × 6 categorias)
        para ~24 (500/página × 1 passagem), um ganho de ~30×.

        Paginação:
            start/limit com PAGE_SIZE=500. HTTP 206 é a resposta normal para
            resultados paginados. Total vem em Content-Range: "s-e/total".

        Filtragem em Python:
            - status: STATUSES_ATIVOS (frozenset O(1))
            - category.id: deve estar em cat_ids_int (set de IDs configurados)
        """
        PAGE_SIZE = 500
        cat_ids_int = {int(c) for c in self.categoria_ids}
        todos: dict[int, dict] = {}
        start = 0

        while True:
            params = {"start": start, "limit": PAGE_SIZE}
            url = f"{self.base_url}/Assistance/Ticket"
            logger.debug("Buscando tickets (start=%d)", start)
            try:
                response = self._session.get(url, params=params, timeout=HTTP_TIMEOUT)
                if response.status_code == 401:
                    self._renovar_sessao()
                    response = self._session.get(url, params=params, timeout=HTTP_TIMEOUT)
                logger.debug("HTTP %s | %d bytes", response.status_code, len(response.content))

                if not response.ok:
                    logger.error("Erro ao buscar tickets: %.300s", response.text)
                    break
                if not response.text.strip():
                    break

                items: list[dict] = response.json()
                if not isinstance(items, list):
                    logger.error("Resposta inesperada: %r", items)
                    break

                # Total vem em Content-Range: "0-499/11489"
                content_range = response.headers.get("Content-Range", "")
                total = 0
                if "/" in content_range:
                    try:
                        total = int(content_range.split("/")[-1])
                    except ValueError:
                        pass

                ativos_pagina = 0
                for item in items:
                    tid = item.get("id")
                    if tid is None:
                        continue

                    # Ignora chamados deletados
                    if item.get("is_deleted"):
                        continue

                    # Filtro de status em Python
                    status_obj = item.get("status") or {}
                    if isinstance(status_obj, dict):
                        status_id = status_obj.get("id", 0)
                    else:
                        try:
                            status_id = int(status_obj)
                        except (ValueError, TypeError):
                            status_id = 0
                    if status_id not in STATUSES_ATIVOS:
                        continue

                    # Filtro de categoria em Python
                    cat_obj = item.get("category") or {}
                    if isinstance(cat_obj, dict):
                        cat_id = cat_obj.get("id")
                    else:
                        try:
                            cat_id = int(cat_obj)
                        except (ValueError, TypeError):
                            cat_id = None
                    if cat_id not in cat_ids_int:
                        continue

                    item["_cat_id"] = cat_id
                    todos[int(tid)] = item
                    ativos_pagina += 1

                logger.debug(
                    "start=%d: %d recebidos | %d ativos nas categorias alvo | total=%s",
                    start, len(items), ativos_pagina, total or "?",
                )

                if not items or (total > 0 and start + len(items) >= total):
                    break
                start += PAGE_SIZE

            except requests.exceptions.RequestException as exc:
                logger.error("Falha de rede (start=%d): %s", start, exc)
                break
            except ValueError as exc:
                logger.error("JSON inválido (start=%d): %s", start, exc)
                break

        logger.info("Total de chamados ativos: %d", len(todos))
        return list(todos.values())

    def buscar_data_inicio_completo(self, ticket_id: int) -> str:
        """
        Busca "Data de início: DD/MM/YYYY" em todas as fontes disponíveis:
            1. GET /Assistance/Ticket/{id} — conteúdo completo do ticket
               (o endpoint de lista pode truncar o campo content)
            2. GET /Assistance/Ticket/{id}/Timeline/ITILFollowup — comentários
        """
        try:
            dados = self._get(f"{self.base_url}/Assistance/Ticket/{ticket_id}")
            for campo in ("content", "description"):
                result = _extrair_data_inicio(dados.get(campo, "") or "")
                if result:
                    logger.debug("Ticket #%d: data de início encontrada no campo '%s': %s", ticket_id, campo, result)
                    return result
            logger.debug(
                "Ticket #%d: data de início não encontrada no ticket completo. "
                "content[:200]=%r",
                ticket_id, (dados.get("content") or "")[:200],
            )
        except Exception as exc:
            logger.warning("Ticket #%d: erro ao buscar ticket completo: %s", ticket_id, exc)

        try:
            resp = self._session.get(
                f"{self.base_url}/Assistance/Ticket/{ticket_id}/Timeline/ITILFollowup",
                timeout=HTTP_TIMEOUT,
            )
            if resp.status_code == 401:
                self._renovar_sessao()
                resp = self._session.get(
                    f"{self.base_url}/Assistance/Ticket/{ticket_id}/Timeline/ITILFollowup",
                    timeout=HTTP_TIMEOUT,
                )
            if resp.ok and resp.text.strip():
                followups = resp.json()
                if isinstance(followups, list):
                    for fu in followups:
                        item_data = fu.get("item") if isinstance(fu.get("item"), dict) else fu
                        for campo in ("content", "name", "description"):
                            result = _extrair_data_inicio(item_data.get(campo, "") or "")
                            if result:
                                return result
            # 404 = ticket sem followups — comportamento normal, sem log
        except Exception as exc:
            logger.warning("Ticket #%d: erro ao buscar followups: %s", ticket_id, exc)

        logger.debug("Ticket #%d: data de início não encontrada em nenhuma fonte.", ticket_id)
        return ""

    def verificar_tarefa_termo(self, ticket_id: int) -> str:
        """
        Verifica (SOMENTE LEITURA) se existe uma tarefa de Termo de
        Responsabilidade associada ao ticket e retorna seu status atual.

        Nunca cria nem altera dados no GLPI. Política estrita de read-only.

        Retorna um de quatro valores canônicos:
            "Termo OK"          — tarefa encontrada e concluída (state >= 2)
            "Pendente"          — tarefa encontrada mas ainda em aberto
            "Sem tarefa"        — nenhuma tarefa de termo localizada no ticket
            "Erro ao verificar" — falha de comunicação ou exceção inesperada

        Cache de termos concluídos (_termos_concluidos: set[int]):
            Uma vez que um termo é confirmado como concluído, o ticket_id é
            adicionado ao set em memória. Nas varreduras seguintes, o método
            retorna "Termo OK" imediatamente sem fazer chamada à API. Isso é
            seguro porque um termo concluído nunca volta ao estado pendente.
            O cache é volátil (em memória) — ao reiniciar o worker, é
            reconstruído na primeira varredura.

        Tratamento do endpoint /TicketTask:
            GET /Ticket/{id}/TicketTask retorna uma lista JSON de tarefas ou
            um dict de erro como {"ERROR": "ERROR_ITEM_NOT_FOUND"} quando não
            há tarefas. O isinstance(resp, list) distingue os dois casos sem
            precisar inspecionar o conteúdo do dict de erro.

        Busca resiliente por substring:
            Em vez de comparar o texto exato da tarefa, busca por
            _TEXTO_TAREFA_TERMO ("termo de responsabilidade") como substring
            case-insensitive no texto limpo. Isso tolera variações como:
            - "Enviar termo de responsabilidade."
            - "TERMO DE RESPONSABILIDADE"
            - "<p>Envio do <b>termo de responsabilidade</b></p>"
            _limpar_html() é chamado em 'content' + 'name' concatenados para
            cobrir o caso em que o texto está no campo de nome da tarefa.

        Logs de diagnóstico (DEBUG):
            O conteúdo bruto (raw) e limpo de cada tarefa é logado em DEBUG
            para facilitar diagnóstico quando o matching falha. Em produção
            com nível INFO, esses logs não aparecem — mudar para DEBUG no
            topo do arquivo os torna visíveis.

        Log de missedas tarefas (INFO):
            Se nenhuma tarefa bateu, loga os primeiros 60 caracteres do nome
            de cada tarefa existente. Isso permite identificar rapidamente se
            o problema é o texto estar diferente do esperado, sem precisar
            ativar DEBUG completo.
        """
        if ticket_id in self._termos_concluidos:
            return "Termo OK"
        try:
            resp = self._get(f"{self.base_url}/Assistance/Ticket/{ticket_id}/Timeline/Task")

            if not isinstance(resp, list):
                return "Sem tarefa"

            if not resp:
                return "Sem tarefa"

            logger.debug(
                "Ticket #%d: campos da tarefa[0]: %s",
                ticket_id, list(resp[0].keys()),
            )

            for i, tarefa in enumerate(tarefas := resp):
                # v2.3 Timeline envolve o objeto real sob a chave "item"
                item_data = tarefa.get("item") if isinstance(tarefa.get("item"), dict) else tarefa

                # Agrega todos os campos textuais conhecidos
                partes = []
                for campo in ("content", "name", "description", "text"):
                    partes.append(item_data.get(campo, "") or "")
                    partes.append(tarefa.get(campo, "") or "")     # fallback no envelope

                texto_limpo = _limpar_html(" ".join(partes))
                logger.debug(
                    "Ticket #%d | tarefa[%d] texto_limpo=%.200r",
                    ticket_id, i, texto_limpo,
                )

                if _TEXTO_TAREFA_TERMO.lower() in texto_limpo.lower():
                    try:
                        state = int(
                            item_data.get("state", tarefa.get("state", 0)) or 0
                        )
                    except (ValueError, TypeError):
                        state = 0

                    logger.debug(
                        "Ticket #%d: tarefa de termo encontrada (state=%d) → %s",
                        ticket_id, state,
                        "Termo OK" if state >= _GLPI_TASK_STATE_DONE else "Pendente",
                    )
                    if state >= _GLPI_TASK_STATE_DONE:
                        self._termos_concluidos.add(ticket_id)
                        return "Termo OK"
                    return "Pendente"

            logger.debug(
                "Ticket #%d: tarefa de termo NÃO encontrada entre %d tarefa(s).",
                ticket_id, len(tarefas),
            )
            return "Sem tarefa"

        except Exception as exc:
            logger.error("Ticket #%d: erro ao verificar termo: %s", ticket_id, exc)
            return "Erro ao verificar"

    def buscar_nome_usuario(self, user_id: int) -> str:
        """
        Retorna o nome completo do usuário GLPI a partir do seu ID numérico,
        com cache em memória para evitar chamadas repetidas à API.

        Por que o ID vem em vez do nome:
            A Search API do GLPI, por padrão sem expand_dropdowns=1, retorna
            o campo 4 (requester) como um ID numérico inteiro. Isso é mais
            eficiente para a API mas exige um segundo lookup para obter o nome.

        Cache (padrão memoization):
            _cache_usuarios é um dict[int, str] iniciado vazio no __init__.
            O padrão "verifica cache → busca API → salva cache → retorna"
            garante que cada user_id seja buscado no máximo uma vez por ciclo
            de vida do worker. Em um ciclo com 37 chamados para o mesmo
            requerente, a API seria chamada apenas 1 vez.

        Composição do nome:
            O GLPI separa nome e sobrenome em "firstname" e "realname".
            f"{firstname} {realname}".strip() monta o nome completo.
            O fallback para dados.get("name") usa o login do usuário (menos
            legível, mas melhor que o ID numérico bruto) caso firstname/realname
            estejam vazios.

        Fallback silencioso para str(user_id):
            Se a chamada à API falhar (rede, permissão), usa o ID numérico como
            nome. O dashboard exibe o número em vez de travar ou mostrar vazio.
            Apenas RequestException é capturada aqui — erros de parse de JSON
            subiriam como ValueError, indicando um problema mais grave.
        """
        if user_id in self._cache_usuarios:
            return self._cache_usuarios[user_id]
        try:
            dados = self._get(f"{self.base_url}/Administration/User/{user_id}")
            firstname = dados.get("firstname", "")
            realname  = dados.get("realname", "")
            nome = f"{firstname} {realname}".strip() or dados.get("name", str(user_id))
        except Exception:
            nome = str(user_id)
        self._cache_usuarios[user_id] = nome
        return nome


# ============================================================================
# Helpers
# ============================================================================

def _formatar_datetime(valor: Any) -> str:
    """
    Converte uma string de datetime no formato do GLPI para o formato
    brasileiro exibido no dashboard.

    Formato de entrada v2.3: "2023-09-18T08:10:42-03:00" (ISO 8601 com timezone)
    Formato de entrada legado: "2023-09-18 08:10:42" (sem timezone)
    Formato de saída (dashboard): "DD/MM/YYYY HH:MM" ou "DD/MM/YYYY" (só data)

    Retorna "" em todos os casos de falha para que o chamador possa tentar
    outros fallbacks (ex: _extrair_data_inicio no corpo do chamado).
    """
    if not valor:
        return ""
    s = str(valor).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%d/%m/%Y %H:%M") if fmt not in ("%Y-%m-%d",) else dt.strftime("%d/%m/%Y")
        except ValueError:
            continue
    return ""


# ============================================================================
# Sincronizador → Database JSON
# ============================================================================

class SincronizadorDB:
    """
    Transforma os chamados brutos da API do GLPI para o schema do dashboard
    e persiste o resultado em database/contratacoes.json.

    Arquitetura de arquivo JSON como banco de dados:
        Em vez de um banco de dados relacional (SQLite, PostgreSQL), usamos
        um arquivo JSON simples. Isso é intencional:
        - Zero dependências de infraestrutura (sem servidor de banco).
        - O dashboard lê o arquivo inteiro via API REST a cada polling de 10s
          — para dezenas/centenas de chamados, isso é instantâneo.
        - O arquivo é versionável, inspecionável e debugável com qualquer
          editor de texto.
        - A escrita atômica (write .tmp → os.replace) garante consistência
          mesmo com falhas no meio da escrita.

    Por que sobrescrever o arquivo inteiro a cada ciclo:
        O modelo de "snapshot completo" é mais simples e robusto que um modelo
        de "diff incremental". Com snapshot: se um chamado for fechado no GLPI,
        ele desaparece automaticamente na próxima varredura. Com diff: seria
        necessário controlar estado anterior e calcular deltas, introduzindo
        complexidade e possíveis inconsistências.
    """

    def __init__(self, db_path: Path, glpi: ClienteGLPI) -> None:
        """
        Inicializa o sincronizador garantindo que o diretório de destino existe.

        db_path.parent.mkdir(parents=True, exist_ok=True):
            parents=True  → cria diretórios intermediários se necessário
                             (ex: "database/" se ainda não existir).
            exist_ok=True → não lança exceção se o diretório já existir.
            Isso permite que o worker funcione em um ambiente limpo (clone
            recente) sem precisar de setup manual.
        """
        self.db_path = db_path
        self.glpi = glpi
        db_path.parent.mkdir(parents=True, exist_ok=True)

    def _chamado_para_dict(self, chamado: dict) -> dict | None:
        """
        Transforma um ticket bruto da API v2.3 para o schema do dashboard.

        Schema v2.3:
            id           — int
            name         — str (título)
            status       — {"id": N, "name": "..."} (objeto, não int raw)
            category     — {"id": N, "name": "..."} (objeto)
            date_creation — ISO 8601 com timezone
            sla_ttr      — ISO 8601 com timezone ou None
            team         — lista de {role, name, firstname, realname}
            content      — HTML do corpo do chamado
            _cat_id      — injetado por buscar_chamados_ativos()
        """
        ticket_id = chamado.get("id")
        if ticket_id is None:
            return None
        try:
            ticket_id = int(ticket_id)
        except (ValueError, TypeError):
            return None

        # status é {"id": N, "name": "..."} em v2.3
        status_obj = chamado.get("status") or {}
        if isinstance(status_obj, dict):
            status_id = status_obj.get("id", 0)
        else:
            try:
                status_id = int(status_obj)
            except (ValueError, TypeError):
                status_id = 0

        # Requerente vem no array team (role == "requester") — sem lookup extra
        requerente = ""
        for member in (chamado.get("team") or []):
            if isinstance(member, dict) and member.get("role") == "requester":
                firstname = (member.get("firstname") or "").strip()
                realname  = (member.get("realname")  or "").strip()
                requerente = f"{firstname} {realname}".strip() or (member.get("name") or "")
                break

        # SLA deadline: sla_ttr → corpo do chamado (em memória) → followups
        tempo_solucao = _formatar_datetime(chamado.get("sla_ttr"))
        if not tempo_solucao:
            tempo_solucao = _extrair_data_inicio(chamado.get("content", "") or "")
        if not tempo_solucao:
            tempo_solucao = self.glpi.buscar_data_inicio_completo(ticket_id)

        # cat_id injetado por buscar_chamados_ativos; fallback para campo category
        cat_id = chamado.get("_cat_id")
        if cat_id is None:
            cat_obj = chamado.get("category") or {}
            if isinstance(cat_obj, dict):
                cat_id = cat_obj.get("id")
            else:
                try:
                    cat_id = int(cat_obj)
                except (ValueError, TypeError):
                    cat_id = None

        if self.glpi.cats_com_ativo and cat_id in self.glpi.cats_com_ativo:
            termo_status = self.glpi.verificar_tarefa_termo(ticket_id)
        else:
            termo_status = "Sem tarefa"

        return {
            "ID_do_Chamado": ticket_id,
            "Titulo":        chamado.get("name") or "",
            "Status":        STATUS_MAP.get(status_id, f"Status {status_id}"),
            "Tempo_Solucao": tempo_solucao,
            "Data_Abertura": _formatar_datetime(chamado.get("date_creation")),
            "Requerente":    requerente,
            "Termo_Status":  termo_status,
        }

    def sincronizar(self, chamados_api: list[dict]) -> None:
        """
        Executa o pipeline completo de transformação e persistência:
        GLPI bruto → dicts normalizados → JSON no disco.

        Pipeline:
            1. Transforma cada chamado com _chamado_para_dict().
            2. Filtra Nones (chamados inválidos/sem ID).
            3. Ordena por ID_do_Chamado para que o arquivo JSON seja
               determinístico — mesma entrada, mesma saída, facilitando
               diff e debug.
            4. Monta o envelope snapshot com timestamp e total.
            5. Persiste atomicamente (ver abaixo).
            6. Atualiza o histórico diário de KPIs.

        Escrita atômica com .tmp + os.replace():
            O padrão "write to temp file, then rename" é a forma correta de
            atualizar um arquivo que pode ser lido por outro processo (o
            servidor web FastAPI) a qualquer momento:
            - Se escrevêssemos diretamente em contratacoes.json e o processo
              fosse interrompido no meio, o arquivo ficaria corrompido/parcial.
            - com .tmp → os.replace(): o arquivo antigo permanece intacto até
              que o novo esteja completamente escrito. os.replace() é uma
              operação atômica no nível do sistema operacional (rename syscall)
              — do ponto de vista de qualquer leitor, o arquivo passa do estado
              antigo para o novo sem jamais estar incompleto.

        ensure_ascii=False:
            Preserva caracteres UTF-8 (acentos, ç) no JSON em vez de
            escapá-los como \\uXXXX. Reduz o tamanho do arquivo e torna-o
            legível diretamente em qualquer editor.

        indent=2:
            Formata o JSON com indentação para facilitar inspeção manual.
            O overhead de espaço é irrelevante para o volume de dados
            (dezenas de chamados).
        """
        linhas = [self._chamado_para_dict(c) for c in chamados_api]
        linhas = [l for l in linhas if l is not None]
        linhas.sort(key=lambda x: x["ID_do_Chamado"])

        snapshot = {
            "ultima_atualizacao": datetime.now().isoformat(),
            "total":    len(linhas),
            "chamados": linhas,
        }

        tmp = self.db_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.db_path)
        logger.info("Database atualizada: %d chamado(s) em '%s'.", len(linhas), self.db_path)

        self._atualizar_historico(linhas)

    def _atualizar_historico(self, linhas: list[dict]) -> None:
        """
        Registra um snapshot diário de KPIs em database/historico.json,
        mantendo os últimos 60 dias de histórico.

        Estrutura do historico.json:
            Lista de dicts, um por dia, com contagens agregadas de status
            e Termo. O dashboard usa essa série para desenhar os sparklines
            (mini-gráficos de 14 dias) nos cards de KPI.

        Por que agregar aqui e não no frontend:
            O histórico é salvo uma vez por ciclo pelo worker. Se a agregação
            fosse feita no frontend, ele precisaria receber todos os chamados
            históricos (dados que já não existem — chamados fechados saem do
            snapshot). Armazenar KPIs diários no worker é a única forma de
            manter o histórico de tendência.

        Lógica de upsert por data:
            hist = [h for h in hist if h.get("data") != hoje]
            Remove a entrada do dia atual (se já existir de uma execução
            anterior no mesmo dia) e insere a versão atualizada. Isso garante
            que o último ciclo do dia seja o valor registrado — sem duplicatas.

        hist[-60:] — janela deslizante de 60 dias:
            Mantém o arquivo compacto e evita crescimento indefinido. 60 dias
            é mais que suficiente para os sparklines (que usam 14 dias) e para
            análise manual de tendências mensais.

        Mesmo padrão de escrita atômica (.tmp + os.replace):
            Mesma justificativa que em sincronizar() — garante consistência
            mesmo com interrupções abruptas do processo.

        Tolerância a histórico corrompido:
            O try/except ao redor do json.loads() inicializa hist como [] se
            o arquivo existir mas estiver corrompido ou vazio. O histórico é
            reconstruído gradualmente nas próximas execuções.
        """
        hoje = datetime.now().strftime("%Y-%m-%d")
        contagens: dict[str, int] = {}
        for l in linhas:
            ts = l.get("Termo_Status", "")
            contagens[ts] = contagens.get(ts, 0) + 1

        entrada = {
            "data":             hoje,
            "total":            len(linhas),
            "em_atendimento":   sum(1 for l in linhas if l.get("Status") == "Em Atendimento"),
            "pendente":         sum(1 for l in linhas if l.get("Status") == "Pendente"),
            "termo_ok":         contagens.get("Termo OK", 0),
            "termo_pendente":   contagens.get("Pendente", 0),
            "sem_tarefa":       contagens.get("Sem tarefa", 0),
            "erro_verificar":   contagens.get("Erro ao verificar", 0),
        }

        hist_path = HISTORICO_PATH
        hist: list[dict] = []
        if hist_path.exists():
            try:
                hist = json.loads(hist_path.read_text(encoding="utf-8"))
            except Exception:
                hist = []

        hist = [h for h in hist if h.get("data") != hoje]
        hist.append(entrada)
        hist = hist[-60:]  # janela deslizante: mantém apenas os últimos 60 dias

        tmp = hist_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(hist, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, hist_path)
        logger.debug("Histórico atualizado: %d dia(s) registrado(s).", len(hist))


# ============================================================================
# Entrypoint
# ============================================================================

def main() -> None:
    """
    Ponto de entrada do worker. Configura os objetos principais e executa
    o loop de polling indefinidamente.

    Por que um loop while True com time.sleep() em vez de um scheduler:
        Soluções como APScheduler ou schedule adicionam dependências e
        complexidade de threading desnecessárias para um worker simples de
        polling. O padrão while True + sleep() é explícito, previsível e
        fácil de entender — cada iteração é exatamente um ciclo de varredura.

    Separação de erros de rede vs. erros gerais:
        requests.exceptions.RequestException cobre falhas de rede esperadas
        (timeout, DNS, conexão recusada) que devem ser logadas como ERROR
        e recuperadas na próxima iteração — o GLPI pode estar
        temporariamente indisponível.
        Exception genérico captura qualquer outro erro inesperado como
        CRITICAL com exc_info=True (inclui stack trace completo no log)
        para facilitar diagnóstico, mas também não mata o processo — o
        worker continua tentando no próximo ciclo.

    Por que não sys.exit() em erros do loop:
        O worker roda como daemon thread dentro de run.py (junto com o
        servidor web). Um sys.exit() aqui encerraria o processo inteiro,
        incluindo o dashboard. Preferimos logar o erro e aguardar o próximo
        ciclo — uma falha temporária não deve derrubar a interface.

    POLLING_INTERVAL como int:
        Os valores do .env sempre chegam como string. A conversão para int
        aqui (em vez de em carregar_configuracoes) mantém o dict de config
        com tipos homogêneos (todas strings) e move a conversão para o
        local de uso.
    """
    config = carregar_configuracoes()
    polling_interval = int(config["POLLING_INTERVAL"])

    logger.info(
        "Verdanadesk Sync iniciado. API: %s | DB: '%s' | Ciclo: %ds (%d min).",
        config["API_URL"], DATABASE_PATH, polling_interval, polling_interval // 60,
    )

    glpi = ClienteGLPI(
        base_url=config["API_URL"],
        token_url=config["OAUTH_TOKEN_URL"],
        client_id=config["OAUTH_CLIENT_ID"],
        client_secret=config["OAUTH_CLIENT_SECRET"],
        username=config["OAUTH_USERNAME"],
        password=config["OAUTH_PASSWORD"],
        categoria_ids=config["CATEGORIA_IDS"],
        cats_com_ativo=config["CATS_COM_ATIVO"],
    )
    sync = SincronizadorDB(db_path=DATABASE_PATH, glpi=glpi)

    while True:
        try:
            logger.info("Varredura iniciada.")
            chamados = glpi.buscar_chamados_ativos()
            sync.sincronizar(chamados)
        except requests.exceptions.RequestException as exc:
            logger.error("Erro de rede no ciclo de varredura: %s", exc)
        except Exception as exc:
            logger.critical("Erro não tratado no ciclo principal: %s", exc, exc_info=True)

        logger.info("Aguardando %ds para a próxima varredura...", polling_interval)
        time.sleep(polling_interval)


if __name__ == "__main__":
    main()
