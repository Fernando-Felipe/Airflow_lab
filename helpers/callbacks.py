"""Callbacks reutilizáveis para as DAGs da Vende+."""

import logging

from airflow.models import Variable
from airflow.utils.email import send_email

log = logging.getLogger(__name__)


def notificar_falha(context):
    """Envia e-mail de alerta via Connection SMTP ao detectar falha em uma task."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exec_date = context.get("logical_date") or context.get("data_interval_start")
    exception = context.get("exception")

    destinatario = Variable.get("email_alertas")

    assunto = f"[Airflow] Falha em {dag_id}.{task_id}"
    corpo = f"""
    <h3>Falha detectada no pipeline da Vende+</h3>
    <ul>
      <li><b>DAG:</b> {dag_id}</li>
      <li><b>Task:</b> {task_id}</li>
      <li><b>Data de execução:</b> {exec_date}</li>
      <li><b>Erro:</b> {exception}</li>
    </ul>
    """
    try:
        send_email(to=destinatario, subject=assunto, html_content=corpo)
    except Exception as e:
        # Loga sem relançar para não mascarar o erro original da task
        log.error("Falha ao enviar e-mail de alerta: %s", e)
