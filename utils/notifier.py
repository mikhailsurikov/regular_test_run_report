import logging
from typing import Any
import requests
import urllib3
from utils.youtrack_client import headers, base_url, issue_id

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger("airflow.task")


def send_failure_alert(context: dict[str, Any]) -> None:
    ti = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = ti.task_id
    run_id = context.get("run_id", "unknown")
    logical_date = context.get("logical_date", "unknown")
    log_url = ti.log_url
    if log_url and not log_url.startswith("http"):
        log_url = "Ссылка на лог недоступна (проверьте AIRFLOW__WEBSERVER__BASE_URL)"
    message = (
        f"**DAG Failed**\n\n"
        f"* **DAG:** `{dag_id}`\n"
        f"* **Task:** `{task_id}`\n"
        f"* **Run ID:** `{run_id}`\n"
        f"* **Logical Date:** `{logical_date}`\n"
        f"* **Logs:** [Open log]({log_url})\n\n"
        f"_This is an automated alert from Airflow._"
    )

    url = f"{base_url()}/api/issues/{issue_id()}/comments"
    logger.info("Sending failure alert to %s", url)

    try:
        # Отправляем POST-запрос, используя заголовки из youtrack_client
        response = requests.post(
            url,
            json={"text": message},
            headers=headers(),
            timeout=30,
            verify=False
        )
        logger.info("Alert response status=%s", response.status_code)
        response.raise_for_status()
        logger.info("Failure alert delivered to YouTrack")
    except requests.RequestException as e:
        # Логируем ошибку, но не роняем сам колбэк, чтобы не скрыть исходную причину падения DAG
        logger.error("Failed to send alert to YouTrack: %s", e)
