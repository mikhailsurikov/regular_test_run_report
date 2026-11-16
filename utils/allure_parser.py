import json
import logging
import os
import pytz
from datetime import datetime

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
    Агрегирует статистику по всем джобам.
    Если данных по джобе нет, ставит статус FAILED/NO REPORT.
    """
    ti = context["ti"]
    allure_paths = ti.xcom_pull(task_ids="download_allure_reports", key="allure_paths") or {}
    jobs = context["params"].get("jobs", [])
    if not jobs:
        raise ValueError("Received empty 'jobs' list in params. Check DAG configuration.")

    if not allure_paths:
        raise ValueError("No Allure reports were downloaded (allure_paths is empty). Marking DAG as failed.")

    logger.info("Aggregating stats across %d expected jobs", len(jobs))

    # --- 1. Берем фактическую дату запуска в формате DD/MM/YYYY ---
    msk_date = datetime.now(pytz.utc).astimezone(MSK)
    date_str = msk_date.strftime("%d/%m/%Y")
    logger.info("Actual run date (MSK): %s", date_str)

    per_job = {}

    # Идем по полному списку джоб, который передали в params
    for job in jobs:
        if job not in allure_paths:
            logger.warning("[%s] No report found. Setting status to FAILED/NO REPORT", job)
            per_job[job] = {
                "status": "FAILED/NO REPORT",
                "stats": {},
                "plugin_url": "",
            }
            continue

        # --- 2. Парсим Allure ---
        info = allure_paths[job]
        parsed = parse_single_report(info["extract_dir"])
        stats = parsed["stats"]
        suites = parsed["suites"]
        if stats.get("total", 0) == 0:
            logger.warning("[%s] Report has 0 cases. Marking as FAILED/NO REPORT.", job)
            per_job[job] = {
                "status": "FAILED/NO REPORT",
                "stats": {},
                "plugin_url": info["plugin_url"],
            }
            continue
        # --- 3. Формируем списки сьютов ---
        failed_suites = [s["name"] for s in suites if s.get("failed", 0) > 0]
        failed_suites_str = ", ".join(failed_suites)

        error_suites = [s["name"] for s in suites if s.get("broken", 0) > 0]
        error_suites_str = ", ".join(error_suites)

        # --- 4. Формируем итоговую структуру ---
        per_job[job] = {
            "status": "SUCCESS",
            "build": info["build"],
            "plugin_url": info["plugin_url"],
            "stats": stats,
            "suites": suites,
            "failed_suites_str": failed_suites_str,
            "error_suites_str": error_suites_str,
        }

    result = {
        "date": date_str,
        "per_job": per_job,
    }

    ti.xcom_push(key="allure_summary", value=result)
    logger.info("allure_summary pushed to XCom")
    return result
