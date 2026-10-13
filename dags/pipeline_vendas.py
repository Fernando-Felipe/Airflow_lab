from datetime import datetime, timedelta
from airflow.decorators import dag, task
from helpers import vendas as vendas_helper

DIR_INPUT = "/opt/airflow/data/input"
DIR_OUTPUT = "/opt/airflow/data/output"
DIR_TEMP = "/opt/airflow/data/temp"
DATA_INICIO = datetime(2026, 11, 10)

ARGS_PADRAO = {
    'owner': 'airflow',
    'depends_on_past': False, ##dependencia de execução de DAGs anteriores (recomendado = True)
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='pipeline_vendas',
    tags=["vendas"],
    default_args=ARGS_PADRAO,
    start_date=DATA_INICIO,
    schedule=None,
    catchup=False,  ##rodar a partir da data de início, ou seja, não rodar DAGs anteriores(False)
)
def pipeline_vendas():
    dados_extraios = extrair_vendas()
    dados_transformados = transformar_vendas(dados_extraios)
    carregar_vendas(dados_transformados)


@task
def extrair_vendas():
    caminho_arquivo = f"{DIR_INPUT}/vendas.csv"
    dados = vendas_helper.extrair_vendas(caminho_arquivo)
    return dados

@task
def transformar_vendas(dados):
    caminho_arquivo = dados['caminho']
    caminho_saida = f"{DIR_TEMP}/resumo_categoria.csv"
    resultado = vendas_helper.transformar_vendas(caminho_arquivo, caminho_saida)
    return resultado
    
@task
def carregar_vendas(resultado):
    caminho_transformado = resultado
    caminho_final = f"{DIR_OUTPUT}/resumo_categoria.csv"
    vendas_helper.carregar_resumo(caminho_transformado, caminho_final)

pipeline_vendas()