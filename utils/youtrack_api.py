import requests
from datetime import datetime
from utils.youtrack_client import *

logger = logging.getLogger("airflow.task")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def headers() -> dict:
    return {
        "Authorization": f"Bearer {Variable.get('YOUTRACK_TOKEN')}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def base_url() -> str:
    return Variable.get("YOUTRACK_URL").rstrip("/")


def issue_id() -> str:
    return Variable.get("YOUTRACK_ISSUE_ID")


def get_and_update_youtrack_comment(**context):
    """
    Формирует и отправляет НОВЫЙ комментарий в YouTrack.
    """
    ti = context["ti"]
    summary = ti.xcom_pull(task_ids="parse_allure_stats", key="allure_summary")
    screenshot_urls = ti.xcom_pull(task_ids="take_screenshot", key="screenshots") or {}
    if not summary:
        raise ValueError("No allure_summary in XCom")
    date_str = summary["date"]  # Формат DD/MM/YYYY
    # Формируем шапку комментария
    final_text = f"**Date:** {date_str}\n\n"
    # Формируем блок с деталями по каждой джобе
    job_blocks = []
    for job_name, job_data in summary["per_job"].items():
        allure_url = job_data.get("plugin_url", "")
        allure_link = f"[Allure Report]({allure_url})" if allure_url else "_No Allure Link_"
        stats = job_data["stats"]
        total = stats["total"]
        passed = stats["passed"]
        failed = stats["failed"]
        errors = stats.get("broken", 0)  # В Allure "broken" = errors
        failed_suites = job_data.get("failed_suites_str", "")
        # Берем ссылку на скриншот
        shot_url = screenshot_urls.get(job_name, "")
        # Если скриншот есть, вшиваем ссылку в название джобы, иначе просто текст
        if shot_url:
            job_title = f"### [{job_name}]({shot_url})"
        else:
            job_title = f"### {job_name}"
        # Формируем блок для одной джобы
        block = (
            f"{job_title}\n"
            f"{allure_link}\n"
            f"Cases total: {total}\n"
            f"<span style=\"color:green;\">{passed} passed</span>\n"
            f"<span style=\"color:red;\">{failed} failed</span> ({failed_suites})\n"
            f"{errors} errors\n"
        )
        job_blocks.append(block)
    # Объединяем блоки
    final_text += "\n---\n".join(job_blocks)
    # Отправляем НОВЫЙ комментарий
    url = f"{base_url()}/api/issues/{issue_id()}/comments"
    payload = {"text": final_text}
    logger.info("Creating new YouTrack comment for issue %s", issue_id())
    logger.debug("Comment text:\n%s", final_text)

    r = requests.post(url, json=payload, headers=headers(), timeout=30, verify=False)
    logger.info("YouTrack response status=%s", r.status_code)
    if r.status_code >= 400:
        logger.error("YouTrack API Error: %s", r.text)
    r.raise_for_status()
    logger.info("Comment created successfully")


TABLE_START_MARKER = "<!-- HISTORY_TABLE_START -->"
TABLE_END_MARKER = "<!-- HISTORY_TABLE_END -->"
JOBS = ["QA110_regular_run_1", "QA110_regular_run_2", "QA110_regular_run_3", "QA110_regular_run_6_SMSSW"]


def get_issue_description() -> str:
    """Получает текущее описание задачи."""
    url = f"{base_url()}/api/issues/{issue_id()}?fields=description"
    r = requests.get(url, headers=headers(), timeout=30, verify=False)
    r.raise_for_status()
    return r.json().get("description", "") or ""


def update_issue_description(new_description: str):
    """Обновляет описание задачи."""
    url = f"{base_url()}/api/issues/{issue_id()}"
    payload = {"description": new_description}
    r = requests.post(url, json=payload, headers=headers(), timeout=30, verify=False)
    r.raise_for_status()


def build_matrix_row(summary: dict) -> str:
    """Формирует новую строку для таблицы."""
    date_str = summary["date"]
    row_parts = [f"| {date_str} |"]

    for job in JOBS:
        job_data = summary["per_job"].get(job)
        if not job_data:
            row_parts.append(" |")
            continue

        stats = job_data["stats"]
        cell = (
            f"T={stats['total']} "
            f"P={stats['passed']} "
            f"F={stats['failed']} "
            f"E={stats.get('broken', 0)} "
            f"S={stats['skipped']}"
        )
        row_parts.append(f" {cell} |")

    return "".join(row_parts)


def get_row_date(row: str) -> datetime:
    """Парсит дату из строки для сортировки."""
    try:
        parts = row.split('|')
        if len(parts) >= 2:
            date_str = parts[1].strip()
            return datetime.strptime(date_str, "%d/%m/%Y")
    except Exception:
        pass
    return datetime.min


def _process_tables(existing_text: str, new_row: str, header: str, sep: str) -> str:
    """Обновляет таблицу, оставляя 10 строк наверху и пряча остальные в <details>."""

    # Ищем существующий блок с таблицей
    start_idx = existing_text.find(TABLE_START_MARKER)
    end_idx = existing_text.find(TABLE_END_MARKER)

    if start_idx != -1 and end_idx != -1:
        # Если маркеры есть, берем текст до и после них
        text_before = existing_text[:start_idx]
        text_after = existing_text[end_idx + len(TABLE_END_MARKER):]
        table_block = existing_text[start_idx + len(TABLE_START_MARKER):end_idx]
    else:
        # Если маркеров нет (первый запуск), добавляем в конец
        text_before = existing_text + "\n\n"
        text_after = ""
        table_block = ""

    # Собираем все существующие строки данных
    all_rows = []
    for line in table_block.splitlines():
        if line.strip().startswith("|") and not line.strip().startswith("|---") and not line.strip().startswith(
                "| Date"):
            all_rows.append(line.strip())

    # Добавляем новую строку
    all_rows.append(new_row)

    # Сортируем (новые сверху)
    all_rows.sort(key=get_row_date, reverse=True)

    # Разделяем на топ-10 и остальные
    top_10 = all_rows[:10]
    hidden = all_rows[10:]

    # Формируем видимую таблицу
    result_table = f"{header}\n{sep}\n" + "\n".join(top_10) + "\n\n"

    # Формируем скрытую таблицу
    if hidden:
        result_table += (
                f"<details>\n"
                f"<summary><b>Показать все прошлые запуски ({len(hidden)})</b></summary>\n\n"
                f"{header}\n{sep}\n" + "\n".join(hidden) + "\n\n"
                                                           f"</details>\n"
        )

    # Собираем всё вместе с маркерами
    return f"{text_before}{TABLE_START_MARKER}\n{result_table}{TABLE_END_MARKER}{text_after}"


def update_youtrack_history_table(**context):
    """Новая таска для обновления исторической таблицы в описании задачи."""
    ti = context["ti"]
    summary = ti.xcom_pull(task_ids="parse_allure_stats", key="allure_summary")
    if not summary:
        raise ValueError("No allure_summary in XCom")

    # Формируем заголовок таблицы
    header = f"| Date | {' | '.join(JOBS)} |"
    sep = f"|------|{'|'.join(['---------------------'] * len(JOBS))}|"

    # Собираем новую строку
    new_row = build_matrix_row(summary)
    logger.info("New matrix row: %s", new_row)

    # Получаем текущее описание задачи
    description = get_issue_description()

    # Обновляем таблицу
    updated_description = _process_tables(description, new_row, header, sep)

    # Отправляем обратно
    update_issue_description(updated_description)
    logger.info("Successfully updated YouTrack history table in Issue Description")
