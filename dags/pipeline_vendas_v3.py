"""DAG de processamento diário de vendas da Vende+."""

from datetime import datetime, timedelta

import helpers.vendas as vendas_helper
from airflow.sdk import Variable, dag, task
from airflow.sensors.filesystem import FileSensor
from helpers.callbacks import notificar_falha


ARGS_PADRAO = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notificar_falha,
}


@dag(
    dag_id="pipeline_vendas_v3",
    start_date=datetime(2025, 1, 14),
    schedule="0 7 * * *",
    catchup=False,
    tags=["vendas_v3"],
    default_args=ARGS_PADRAO,
)
def pipeline_vendas_v3():
    @task
    def extrair_vendas(**context):
        ds = context["ds"]
        dir_input = Variable.get("vendas_dir_input")
        caminho = f"{dir_input}/vendas_{ds}.csv"
        resultado = vendas_helper.extrair_vendas(caminho)
        print(f"Arquivo lido: {resultado['caminho']} — {resultado['linhas']} linhas")
        return resultado

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
    t_carregar = carregar_resumo(t_extrair)

    t_aguardar >> t_extrair >> t_transformar >> t_carregar


pipeline_vendas_v3()
