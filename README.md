# Vende+ Data Platform

Plataforma de dados da Vende+ (empresa fictícia) orquestrada com **Apache Airflow 3**, rodando localmente em Docker. Projeto desenvolvido durante o curso **Engenharia de Dados: Orquestração de Pipelines com Apache Airflow**, com dois pipelines: um de vendas e um de cotação do dólar.

## O que foi construído

| Pipeline | O que faz |
|---|---|
| **Vendas** | Espera o CSV de pedidos do dia, valida, agrupa por categoria e grava o resumo em `data/output` |
| **Cotação** | Busca a cotação USD-BRL na AwesomeAPI, salva o JSON bruto e consolida um histórico de 7 dias |

## O que o projeto aborda

**Ambiente**
- Airflow em Docker Compose, com seus serviços (apiserver, scheduler, dag-processor, worker, postgres, redis).
- Volumes para ligar as pastas do projeto ao container, e `PYTHONPATH` para os imports de `helpers/` funcionarem.

**Escrita de DAGs**
- TaskFlow API com `@dag` e `@task`.
- Parâmetros da DAG: `start_date`, `schedule` (incluindo cron, como `0 7 * * *`), `catchup`, `tags`.
- `default_args`: `owner`, `retries`, `retry_delay`, `depends_on_past`.
- Separação de responsabilidades: a DAG só orquestra, a lógica de negócio fica em `helpers/`.

**Dependências e fluxo**
- Ordem com `>>`, tasks em paralelo com `[]` e bifurcações.
- Sensores: o `FileSensor` aguarda o arquivo de vendas chegar antes de extrair.

**Dados entre tasks**
- XCom para passar apenas metadados pequenos (caminhos, contagens), nunca os dados em si.
- Contexto da execução (`**context`, `ds`) para trabalhar com a data da run.

**Configuração fora do código**
- Variables para caminhos e parâmetros.
- Connections e `BaseHook` para sistemas externos, como a API.
- Templates Jinja, como `{{ var.value.vendas_dir_input }}` e `{{ ds }}`.

**Monitoramento e alertas**
- Callback `notificar_falha` (`helpers/callbacks.py`), usado como `on_failure_callback`: quando uma task falha, envia um e-mail com a DAG, a task, a data da execução e o erro.
- O envio usa a Connection SMTP e o destinatário vem da Variable `email_alertas`.
- Se o e-mail falhar, o erro é apenas registrado no log, para não mascarar a falha original da task.

**Boas práticas**
- Idempotência: usar a data da execução, sobrescrever em vez de acrescentar, nomear saídas pela data.
- Ambientes de dev e prod separados, com o mesmo código e configurações diferentes.
- Assets, para disparar DAGs quando um dado é atualizado.

**Fluxo de trabalho com IA**
- `CLAUDE.md` com os padrões do projeto e skills do Claude Code para gerar spec a partir de contexto, gerar DAG a partir de spec, revisar DAG, explicar erro e gerar dados de teste.

## Estrutura do projeto

```
.
├── CLAUDE.md                # Padrões do projeto para a IA
├── docker-compose.yaml      # Ambiente Airflow local
├── dags/
│   ├── pipeline_vendas.py       # Versão 1: dados passados entre tasks via XCom
│   ├── pipeline_vendas_v2.py    # Versão 2: sensor, agendamento, Variables e data da execução
│   └── pipeline_cotacao.py
├── helpers/                 # Funções de negócio
│   ├── vendas.py
│   ├── cotacao.py
│   └── callbacks.py
├── data/                    # Arquivos de dados (não versionados)
│   ├── input/
│   ├── temp/
│   ├── output/
│   └── cotacao/
│       ├── raw/
│       └── consolidated/
├── docs/
│   ├── contexto/            # Documentos de origem (e-mails, histórias)
│   └── specs/               # Especificações técnicas validadas
└── scripts/
    └── gerar_dados.py       # Geração de dados de teste
```

## Como rodar

**1. Criar o `.env`** na raiz do projeto:

```
AIRFLOW_UID=50000
PYTHONPATH=/opt/airflow
```

**2. Subir o ambiente:**

```bash
docker compose up airflow-init
docker compose up -d
```

Acesso: http://localhost:8080 (usuário `airflow`, senha `airflow`)

**3. Cadastrar as Variables** (Admin → Variables):

| Key | Value |
|---|---|
| `vendas_dir_input` | `/opt/airflow/data/input` |
| `vendas_dir_temp` | `/opt/airflow/data/temp` |
| `vendas_dir_output` | `/opt/airflow/data/output` |
| `cotacao_dir_raw` | `/opt/airflow/data/cotacao/raw` |
| `cotacao_caminho_consolidado` | `/opt/airflow/data/cotacao/consolidated/historico_7dias.csv` |

**4. Cadastrar as Connections** (Admin → Connections):

| Connection Id | Tipo | Host | Schema |
|---|---|---|---|
| `awesomeapi_http` | HTTP | `economia.awesomeapi.com.br` | `https` |
| `fs_default` | File (path) | | |

**5. Gerar dados de teste:**

```bash
python scripts/gerar_dados.py
```

Gera os CSVs de vendas dos últimos 3 dias em `data/input/`.

**6. Ativar e disparar as DAGs** na interface. Na busca, procure por `pipeline`.

## Configuração de imports

A pasta `helpers/` fica na raiz do projeto, fora de `dags/`. Para o Airflow resolver `from helpers import vendas` dentro do container, o `docker-compose.yaml` define, no bloco `x-airflow-common`:

```yaml
environment:
  PYTHONPATH: '/opt/airflow'
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/data:/opt/airflow/data
  - ${AIRFLOW_PROJ_DIR:-.}/helpers:/opt/airflow/helpers
```

Sem isso as DAGs falham com `ModuleNotFoundError`.

## Tecnologias

Apache Airflow 3 · Python 3.10+ · Docker Compose · pandas · PostgreSQL · Redis
