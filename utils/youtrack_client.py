import logging
import urllib3
from airflow.models import Variable

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger("airflow.task")


def headers() -> dict:
    """Возвращает заголовки для авторизации в YouTrack."""
    return {
        "Authorization": f"Bearer {Variable.get('YOUTRACK_TOKEN')}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def base_url() -> str:
    """Возвращает базовый URL YouTrack без слэша на конце."""
    return Variable.get("YOUTRACK_URL").rstrip("/")


def issue_id() -> str:
    """Возвращает ID задачи, с которой мы работаем."""
    return Variable.get("YOUTRACK_ISSUE_ID")
