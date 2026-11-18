import logging
import re
import urllib3
from datetime import datetime
import requests
from utils.youtrack_client import headers, base_url, issue_id

logger = logging.getLogger("airflow.task")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_and_update_youtrack_comment(**context):
    """
    Формирует и отправляет новый комментарий в YouTrack со статистикой по джобам.
    """
    ti = context["ti"]
    summary = ti.xcom_pull(task_ids="parse_allure_stats", key="allure_summary")
    screenshot_urls = ti.xcom_pull(task_ids="take_screenshot", key="screenshots") or {}
    if not summary:
        raise ValueError("No allure_summary in XCom")

    date_str = summary["date"]  # Формат DD/MM/YYYY
    final_text = f"**Date:** {date_str}\n\n"
    job_blocks = []

    for job_name, job_data in summary["per_job"].items():
        # 1. Формируем заголовок со ссылкой на скриншот (работает для всех джоб)
        shot_url = screenshot_urls.get(job_name, "")
        if shot_url:
            job_title = f"### [{job_name}]({shot_url})"
        else:
            job_title = f"### {job_name}"

        # 2. Формируем ссылку на Allure (работает для всех джоб)
        allure_url = job_data.get("plugin_url", "")
        allure_link = f"[Allure Report]({allure_url})" if allure_url else "_No Allure Link_"

        # 3. Проверяем статус: если джоба упала или 0 кейсов
        if job_data.get("status") == "FAILED/NO REPORT":
            block = (
                f"{job_title}\n"
                f"{allure_link}\n"
                f"<span style=\"color:red;\">**FAILED/NO REPORT**</span>\n"
            )
            job_blocks.append(block)
            continue  # Идем к следующей джобе

        # 4. Если джоба успешна, берем статистику (используем .get для безопасности)
        stats = job_data.get("stats", {})
        total = stats.get("total", 0)
        passed = stats.get("passed", 0)
        failed = stats.get("failed", 0)
        errors = stats.get("broken", 0)
        failed_suites = job_data.get("failed_suites_str", "")

        block = (
            f"{job_title}\n"
            f"{allure_link}\n"
            f"Cases total: {total}\n"
            f"<span style=\"color:green;\">{passed} passed</span>\n"
            f"<span style=\"color:red;\">{failed} failed</span> ({failed_suites})\n"
            f"{errors} errors\n"
        )
        job_blocks.append(block)

    final_text += "\n---\n".join(job_blocks)

    url = f"{base_url()}/api/issues/{issue_id()}/comments"
    payload = {"text": final_text}
    logger.info("Creating new YouTrack comment for issue %s", issue_id())

    r = requests.post(url, json=payload, headers=headers(), timeout=30, verify=False)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error("YouTrack API Error (Body): %s", r.text)
        raise e  # Пробрасываем ошибку дальше, чтобы Airflow пометил таску как Failed
    response_data = r.json()
    comment_id = response_data.get("id")
    ti.xcom_push(key="latest_comment_id", value=comment_id)

    logger.info("Comment created successfully with ID: %s", comment_id)


TABLE_START_MARKER = "<!-- HISTORY_TABLE_START -->"
TABLE_END_MARKER = "<!-- HISTORY_TABLE_END -->"


def get_issue_description() -> str:
    """Получает текущее описание задачи."""
    url = f"{base_url()}/api/issues/{issue_id()}?fields=description"
    r = requests.get(url, headers=headers(), timeout=30, verify=False)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error("YouTrack API Error (Body): %s", r.text)
        raise e
    return r.json().get("description", "") or ""


def update_issue_description(new_description: str):
    """Обновляет описание задачи."""
    url = f"{base_url()}/api/issues/{issue_id()}"
    payload = {"description": new_description}
    r = requests.post(url, json=payload, headers=headers(), timeout=30, verify=False)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error("YouTrack API Error (Body): %s", r.text)
        raise e


def format_metric(current: int, prev_col_str: str, prefix: str, higher_is_better: bool) -> str:
    """
    Раскрашивает ТОЛЬКО число, сравнивая его с предыдущим значением.
    """
    if not prev_col_str:
        return f"{prefix}={current}"

    # Ищем паттерн: prefix=123 или prefix=<span ...>123
    match = re.search(rf'{prefix}=(?:<span[^>]*>)?(\d+)', prev_col_str)
    if not match:
        return f"{prefix}={current}"

    previous = int(match.group(1))

    if current == previous:
        color = "lightcyan"  # Синий (без изменений)
    elif (current > previous and higher_is_better) or (current < previous and not higher_is_better):
        color = "palegreen"  # Зеленый (стало лучше)
    else:
        color = "mistyrose"  # Красный (стало хуже)

    return f'{prefix}=<span style="background-color:{color};">{current}</span>'


def build_matrix_row(summary: dict, header_columns: list, prev_row_str: str) -> str:
    """Формирует новую строку для таблицы."""
    date_str = summary["date"]

    # Просто дата, без ссылки
    row_parts = [f"| {date_str} |"]

    # Разбиваем предыдущую строку, чтобы брать данные из конкретной колонки
    prev_parts = [p.strip() for p in prev_row_str.split('|')] if prev_row_str else []

    # Идем по колонкам шапки (пропуская Date)
    for i, col_name in enumerate(header_columns[1:], start=1):
        job_data = summary["per_job"].get(col_name)
        if not job_data or job_data.get("status") == "FAILED/NO REPORT":
            row_parts.append(" <span style=\"color:red;\">FAILED/NO REPORT</span> |")
            continue

        stats = job_data["stats"]

        # ИСПРАВЛЕНИЕ: prev_parts[0] - это пустая строка, prev_parts[1] - это дата.
        # Поэтому для первой джобы (i=1) нам нужен prev_parts[2], то есть prev_parts[i + 1]
        prev_col_data = prev_parts[i + 1] if (i + 1) < len(prev_parts) else ""

        # Применяем логику раскрашивания
        t_str = format_metric(stats['total'], prev_col_data, "T", True)
        p_str = format_metric(stats['passed'], prev_col_data, "P", True)
        f_str = format_metric(stats['failed'], prev_col_data, "F", False)
        e_str = format_metric(stats.get('broken', 0), prev_col_data, "E", False)
        s_str = format_metric(stats['skipped'], prev_col_data, "S", False)

        cell = f"{t_str} {p_str} {f_str} {e_str} {s_str}"
        row_parts.append(f" {cell} |")

    return "".join(row_parts)


def get_row_date(row: str) -> datetime:
    """Парсит дату из строки для сортировки."""
    try:
        # Берем второй элемент после разбивки по | (первый пустой, второй - дата)
        parts = row.split('|')
        if len(parts) >= 2:
            date_str = parts[1].strip()
            return datetime.strptime(date_str, "%d/%m/%Y")
    except Exception:
        pass
    return datetime.min


def process_tables(existing_text: str, summary: dict) -> str:
    """Обновляет таблицу."""
    start_idx = existing_text.find(TABLE_START_MARKER)
    end_idx = existing_text.find(TABLE_END_MARKER)

    if start_idx != -1 and end_idx != -1:
        text_before = existing_text[:start_idx]
        text_after = existing_text[end_idx + len(TABLE_END_MARKER):]
        table_block = existing_text[start_idx + len(TABLE_START_MARKER):end_idx]
    else:
        text_before = existing_text + "\n\n"
        text_after = ""
        table_block = ""

    lines = table_block.splitlines()
    header_line = ""
    sep_line = ""
    data_rows = []

    # Извлекаем существующую шапку и данные
    for i, line in enumerate(lines):
        if line.strip().startswith("| Date"):
            header_line = line.strip()
            if i + 1 < len(lines) and lines[i + 1].strip().startswith("|---"):
                sep_line = lines[i + 1].strip()
            break

    if not header_line:
        actual_jobs = list(summary["per_job"].keys())
        header_line = f"| Date | {' | '.join(actual_jobs)} |"
        sep_line = f"|------|{'|'.join(['---------------------'] * len(actual_jobs))}|"

    header_columns = [c.strip() for c in header_line.split('|')[1:-1]]

    for line in lines:
        if line.strip().startswith("|") and not line.strip().startswith("|---") and not line.strip().startswith(
                "| Date"):
            data_rows.append(line.strip())

    prev_row_str = data_rows[0] if data_rows else ""

    new_row = build_matrix_row(summary, header_columns, prev_row_str)
    new_row_date = get_row_date(new_row)
    cleaned_data_rows = []
    for row in data_rows:
        if get_row_date(row) != new_row_date:
            cleaned_data_rows.append(row)
        else:
            logger.info("Found duplicate row for date %s. Replacing it.", summary['date'])
    all_rows = [new_row] + cleaned_data_rows
    all_rows.sort(key=get_row_date, reverse=True)

    top_10 = all_rows[:10]
    hidden = all_rows[10:]

    result_table = f"{header_line}\n{sep_line}\n" + "\n".join(top_10) + "\n\n"

    if hidden:
        result_table += (
                f"<details>\n"
                f"<summary><b>Показать все прошлые запуски ({len(hidden)})</b></summary>\n\n"
                f"{header_line}\n{sep_line}\n" + "\n".join(hidden) + "\n\n"
                                                                     f"</details>\n"
        )

    return f"{text_before}{TABLE_START_MARKER}\n{result_table}{TABLE_END_MARKER}{text_after}"


def update_youtrack_history_table(**context):
    """Обновляет историческую таблицу в описании задачи."""
    ti = context["ti"]
    summary = ti.xcom_pull(task_ids="parse_allure_stats", key="allure_summary")
    if not summary:
        raise ValueError("No allure_summary in XCom")

    description = get_issue_description()
    updated_description = process_tables(description, summary)

    update_issue_description(updated_description)
    logger.info("Successfully updated YouTrack history table in Issue Description")
