from datetime import datetime, timedelta
from airflow.sdk import Variable, dag, task
from helpers import vendas as vendas_helper
from airflow.sensors.filesystem import FileSensor

DATA_INICIO = datetime(2026, 10, 11)

ARGS_PADRAO = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='pipeline_vendas_v2',
    tags=["vendas_v2"],
    default_args=ARGS_PADRAO,
    start_date=DATA_INICIO,
    schedule="0 7 * * *",
    catchup=False,
)
def pipeline_vendas_v2():
    @task
    def extrair_vendas(**context):
        ds = context["ds"]  ##data da execução, ex.: 2026-10-11
        dir_input = Variable.get("vendas_dir_input")
        caminho = f"{dir_input}/vendas_{ds}.csv"
        resultado = vendas_helper.extrair_vendas(caminho)
        print(f"Arquivo lido: {resultado['caminho']}")
        return resultado  ##vira XCom: {"caminho": ..., "linhas": ...}

    @task
    def transformar_vendas(**context):
        ds = context["ds"]
        dir_input = Variable.get("vendas_dir_input")
        dir_temp = Variable.get("vendas_dir_temp")
        caminho_entrada = f"{dir_input}/vendas_{ds}.csv"
        caminho_saida = f"{dir_temp}/resumo_categoria_{ds}.csv"
        vendas_helper.transformar_vendas(caminho_entrada, caminho_saida)
        print(f"Resumo intermediário salvo em: {caminho_saida}")

    @task
    def carregar_resumo(metadados, **context):
        ds = context["ds"]
        dir_temp = Variable.get("vendas_dir_temp")
        dir_output = Variable.get("vendas_dir_output")
        caminho_transformado = f"{dir_temp}/resumo_categoria_{ds}.csv"
        caminho_final = f"{dir_output}/resumo_categoria_{ds}.csv"
        vendas_helper.carregar_resumo(caminho_transformado, caminho_final)
        print(f"Resumo final disponível em: {caminho_final}")
        print(f"Total de linhas processadas: {metadados['linhas']}")

    t_aguardar = FileSensor(
        task_id="aguardar_arquivo_vendas",
        fs_conn_id="fs_default",
        filepath="{{ var.value.vendas_dir_input }}/vendas_{{ ds }}.csv",
        timeout=3 * 3600,
        poke_interval=60,
    )

    t_extrair = extrair_vendas()
    t_transformar = transformar_vendas()
    t_carregar = carregar_resumo(t_extrair)  ##recebe o XCom do extrair

    ##o transformar não recebe dado de ninguém, então a ordem dele é definida com >>
    t_aguardar >> t_extrair >> t_transformar >> t_carregar


pipeline_vendas_v2()
