import json
import logging
import os
import pytz

logger = logging.getLogger("airflow.task")
# Часовой пояс для правильного формирования даты
MSK = pytz.timezone("Europe/Moscow")


def find_file(base_dir: str, filename: str) -> str | None:
    """Рекурсивно ищет файл по имени внутри base_dir (любой уровень вложенности)."""
    for root, _, files in os.walk(base_dir):
        if filename in files:
            return os.path.join(root, filename)
    return None


def read_json(path: str) -> dict:
    """Читает JSON-файл."""
    logger.info("Reading %s", path)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_single_report(extract_dir: str) -> dict:
    """
    Парсит один распакованный Allure-отчёт.
    Использует рекурсивный поиск find_file, чтобы находить файлы,
    даже если они лежат в папке allure-report/ или data/.
    """
    summary_path = find_file(extract_dir, "summary.json")
    suites_path = find_file(extract_dir, "suites.json")
    # Дефолтная структура статистики
    stats = {"total": 0, "passed": 0, "failed": 0,
             "broken": 0, "skipped": 0, "unknown": 0}
    if summary_path:
        summary = read_json(summary_path)
        s = summary.get("statistic", {})
        for key in stats:
            stats[key] = s.get(key, 0)
        logger.info("Parsed summary: %s", stats)
    else:
        logger.warning("No summary.json anywhere inside %s", extract_dir)

    suites = []
    if suites_path:
        suites_data = read_json(suites_path)
        for item in suites_data.get("items", []):
            item_stat = item.get("statistic", {})
            suites.append({
                "name": item.get("name", "unknown"),
                "total": item_stat.get("total", 0),
                "passed": item_stat.get("passed", 0),
                "failed": item_stat.get("failed", 0),
                "broken": item_stat.get("broken", 0),
                "skipped": item_stat.get("skipped", 0)})
        logger.info("Parsed %d suites", len(suites))
    else:
        logger.warning("No suites.json anywhere inside %s", extract_dir)
    return {"stats": stats, "suites": suites}


def parse_allure_stats(**context):
    """
    Агрегирует статистику по всем 4 джобам.
    Формирует данные для шаблона комментария в YouTrack.
    """
    ti = context["ti"]
    allure_paths = ti.xcom_pull(task_ids="download_allure_reports", key="allure_paths")
    if not allure_paths:
        raise ValueError("No allure_paths in XCom — check download_allure_reports")

    logger.info("Aggregating stats across %d jobs", len(allure_paths))

    # Формируем дату в формате DD/MM/YYYY ---
    logical_date = context["logical_date"]
    msk_date = logical_date.astimezone(MSK)
    date_str = msk_date.strftime("%d/%m/%Y")  # 07/11/2026
    logger.info("Run date (MSK): %s", date_str)
    per_job = {}
    for job, info in allure_paths.items():
        # Парсим Allure ---
        parsed = parse_single_report(info["extract_dir"])
        stats = parsed["stats"]
        suites = parsed["suites"]
        # Формируем списки сьютов с упавшими тестами и ошибками ---
        failed_suites = [s["name"] for s in suites if s.get("failed", 0) > 0]
        failed_suites_str = ", ".join(failed_suites)
        error_suites = [s["name"] for s in suites if s.get("broken", 0) > 0]
        error_suites_str = ", ".join(error_suites)

        # Формируем итоговую структуру для одной джобы ---
        per_job[job] = {
            "build": info["build"],
            "plugin_url": info["plugin_url"],
            "stats": stats,
            "suites": suites,
            "failed_suites_str": failed_suites_str,
            "error_suites_str": error_suites_str,
        }
    # Собираем финальный словарь для XCom ---
    result = {
        "date": date_str,
        "per_job": per_job,
    }
    ti.xcom_push(key="allure_summary", value=result)
    logger.info("allure_summary pushed to XCom")

    return result
