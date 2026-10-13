"""DAG de cotação USD-BRL - busca diária via AwesomeAPI e consolida histórico de 7 dias."""

from datetime import datetime, timedelta

from airflow.sdk import BaseHook, Variable, dag, task

from helpers import cotacao as cotacao_helper

ARGS_PADRAO = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="pipeline_cotacao",
    description="Busca cotação USD-BRL e consolida histórico dos últimos 7 dias.",
    start_date=datetime(2026, 10, 10),
    schedule=None,
    catchup=False,
    default_args=ARGS_PADRAO,
    tags=["cotacao", "financeiro"],
)
def pipeline_cotacao():
    @task()
    def buscar_e_salvar_cotacao():
        """Busca cotação na API e salva o JSON bruto na pasta raw."""
        conn = BaseHook.get_connection("awesomeapi_http")
        url_api = f"{conn.schema}://{conn.host}/last/USD-BRL"
        pasta_raw = Variable.get("cotacao_dir_raw")
        cotacao = cotacao_helper.buscar_cotacao(url_api)
        cotacao_helper.salvar_json_bruto(cotacao, pasta_raw)

    @task()
    def consolidar():
        """Lê os JSONs dos últimos 7 dias e gera o CSV consolidado."""
        pasta_raw = Variable.get("cotacao_dir_raw")
        caminho_consolidado = Variable.get("cotacao_caminho_consolidado")
        cotacao_helper.consolidar_historico(pasta_raw, caminho_consolidado)

    buscar_e_salvar_cotacao() >> consolidar()


pipeline_cotacao()