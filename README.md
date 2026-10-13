# Vende+ Data Platform

Plataforma de dados da Vende+ (empresa fictícia) orquestrada com **Apache Airflow 3**, rodando localmente em Docker. Projeto desenvolvido durante o curso **Engenharia de Dados: Orquestração de Pipelines com Apache Airflow**, com dois pipelines: um de vendas e um de cotação do dólar.

## O que foi construído

| Pipeline | O que faz |
|---|---|
| **Vendas** | Espera o CSV de pedidos do dia, valida, agrupa por categoria e grava o resumo em `data/output` |
| **Cotação** | Busca a cotação USD-BRL na AwesomeAPI, salva o JSON bruto e consolida um histórico de 7 dias |

### Exemplo de entrada e saída

A pasta [docs/exemplos](docs/exemplos) traz uma execução real do pipeline de vendas.

**Entrada** (`vendas_2026-10-12.csv`, 80 pedidos):

| data | pedido_id | produto | categoria | quantidade | valor_unitario |
|---|---|---|---|---|---|
| 2026-10-12 | PED-20261012-0001 | Bola de futebol | Esporte e Lazer | 3 | 80.0 |
| 2026-10-12 | PED-20261012-0002 | Teclado mecânico | Eletrônicos | 3 | 459.23 |
| 2026-10-12 | PED-20261012-0003 | Carregador USB-C | Eletrônicos | 1 | 56.73 |
| ... | | | | | |

**Saída** (`resumo_categoria_2026-10-12.csv`):

| categoria | total_pedidos | valor_total_brl | ticket_medio_brl |
|---|---|---|---|
| Beleza | 16 | 2423.19 | 151.45 |
| Casa e Decoração | 19 | 5990.42 | 315.29 |
| Eletrônicos | 20 | 6540.41 | 327.02 |
| Esporte e Lazer | 10 | 10900.46 | 1090.05 |
| Vestuário | 15 | 2612.02 | 174.13 |

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
- Callback `notificar_falha` (`helpers/callbacks.py`), ligado como `on_failure_callback` no `pipeline_vendas_v3`: quando uma task falha, envia um e-mail com a DAG, a task, a data da execução e o erro.
- O envio usa a Connection SMTP e o destinatário vem da Variable `email_alertas`.
- Se o e-mail falhar, o erro é apenas registrado no log, para não mascarar a falha original da task.

**Boas práticas**
- Idempotência no pipeline de vendas: usa a data da execução, sobrescreve em vez de acrescentar e nomeia as saídas pela data.

**Fluxo de trabalho com IA**
- `CLAUDE.md` com os padrões do projeto e skills do Claude Code para gerar spec a partir de contexto, gerar DAG a partir de spec, revisar DAG, explicar erro e gerar dados de teste.

## Conceitos estudados no curso

Vistos no curso, mas não implementados neste repositório:

- **Ambientes de dev e prod separados:** duas instalações independentes do Airflow, com o mesmo código e Variables e Connections de mesmo nome e valores diferentes.
- **Assets:** disparar uma DAG quando um dado é atualizado por outra, em vez de depender só de horário.

## Estrutura do projeto

```
.
├── CLAUDE.md                # Padrões do projeto para a IA
├── docker-compose.yaml      # Ambiente Airflow local
├── dags/
│   ├── pipeline_vendas.py       # Versão 1: primeira versão de estudo
│   ├── pipeline_vendas_v2.py    # Versão 2: sensor, agendamento, Variables e data da execução
│   ├── pipeline_vendas_v3.py    # Versão 3 (final): v2 + alerta de falha por e-mail
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
│   ├── exemplos/            # Exemplo de entrada e saída do pipeline de vendas
│   └── specs/               # Especificações técnicas validadas
└── scripts/
    └── gerar_dados.py       # Geração de dados de teste
```

### As três versões do pipeline de vendas

O pipeline de vendas foi mantido em três arquivos para mostrar a evolução ao longo do curso:

| Versão | O que mostra |
|---|---|
| `pipeline_vendas` (v1) | Primeira versão de estudo: caminhos fixos no código e dados passados entre as tasks por retorno e parâmetro (XCom). Mantida como histórico, fora do fluxo de execução: lê um `vendas.csv` de nome fixo, que o script de dados não gera. |
| `pipeline_vendas_v2` | Sensor de arquivo, agendamento diário, Variables e arquivos nomeados pela data da execução. |
| `pipeline_vendas_v3` | Versão final: a v2 com o alerta de falha por e-mail. |

A v2 e a v3 têm o mesmo agendamento e gravam nos mesmos arquivos, então basta manter uma das duas ativa.

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

**7. (Opcional) Configurar o alerta de falha** do `pipeline_vendas_v3`. O envio depende de um servidor de e-mail; sem estes cadastros a DAG roda normalmente, apenas não envia o alerta.

| Tipo | Nome | Valor |
|---|---|---|
| Variable | `email_alertas` | e-mail que recebe os alertas |
| Connection | `smtp_default` (tipo SMTP) | host, porta, login e senha do seu servidor de e-mail |

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
