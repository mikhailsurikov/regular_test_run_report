import logging
import os
import shutil
import zipfile
from typing import Optional
import requests
from airflow.exceptions import AirflowException
from airflow.models import Variable

logger = logging.getLogger("airflow.task")

ALLURE_BASE_DIR = "/tmp/allure-reports"


class JenkinsJobNotFound(AirflowException):
    """Фатальная ошибка: джобы с таким именем в Jenkins нет."""


def auth() -> tuple[str, str]:
    return (Variable.get("JENKINS_USER"), Variable.get("JENKINS_TOKEN"))


def jenkins_url() -> str:
    return Variable.get("JENKINS_URL").rstrip("/")


def get_last_build_number(job_name: str) -> Optional[int]:
    url = f"{jenkins_url()}/job/{job_name}/lastBuild/api/json"
    logger.info("[%s] GET %s", job_name, url)
    try:
        r = requests.get(url, auth=auth(), timeout=30, verify=False)
    except requests.RequestException as e:
        logger.warning("[%s] Jenkins unreachable: %s", job_name, e)
        return None
    logger.info("[%s] Response: status=%s", job_name, r.status_code)
    if r.status_code == 404:
        raise JenkinsJobNotFound(
            f"Jenkins job '{job_name}' not found at {url}. "
            f"Check JENKINS_JOBS in DAG.")
    if r.status_code == 403:
        logger.error("[%s] Forbidden (403). Check JENKINS_USER/JENKINS_TOKEN.", job_name)
        return None
    if r.status_code != 200:
        logger.warning("[%s] Unexpected status %s", job_name, r.status_code)
        return None
    build = r.json().get("number")
    logger.info("[%s] lastBuild = #%s", job_name, build)
    return build


def has_allure_plugin_report(job_name: str, build_number: int) -> bool:
    url = f"{jenkins_url()}/job/{job_name}/{build_number}/allure/"
    logger.info("[%s#%s] Checking plugin page: %s", job_name, build_number, url)
    try:
        r = requests.get(url, auth=auth(), timeout=30, allow_redirects=True, verify=False)
    except requests.RequestException as e:
        logger.warning("[%s#%s] Plugin page request failed: %s", job_name, build_number, e)
        return False
    logger.info("[%s#%s] Plugin page status=%s", job_name, build_number, r.status_code)
    return r.status_code == 200


def get_allure_artifact_path(job_name: str, build_number: int) -> Optional[str]:
    url = (
        f"{jenkins_url()}/job/{job_name}/{build_number}"
        f"/api/json?tree=artifacts[fileName,relativePath]"
    )
    logger.info("[%s#%s] Listing artifacts: %s", job_name, build_number, url)
    try:
        r = requests.get(url, auth=auth(), timeout=30, verify=False)
    except requests.RequestException as e:
        logger.warning("[%s#%s] Artifact listing failed: %s", job_name, build_number, e)
        return None
    if r.status_code != 200:
        logger.warning("[%s#%s] Artifact listing status=%s", job_name, build_number, r.status_code)
        return None

    artifacts = r.json().get("artifacts", [])
    names = [a["fileName"] for a in artifacts]
    logger.info("[%s#%s] Artifacts present: %s", job_name, build_number, names)

    for art in artifacts:
        name = art["fileName"].lower()
        if "allure" in name and name.endswith(".zip"):
            logger.info("[%s#%s] Found Allure zip: %s", job_name, build_number, art["relativePath"])
            return art["relativePath"]

    logger.info(
        "[%s#%s] No '*allure*.zip' among artifacts. Waiting for it to appear.",
        job_name, build_number,
    )
    return None


def wait_for_allure(job_name: str, **context) -> bool:
    logger.info("=== Poke: checking Allure for '%s' ===", job_name)
    try:
        build_num = get_last_build_number(job_name)
    except JenkinsJobNotFound:
        logger.error("[%s] Job not found — failing immediately (no point in waiting).", job_name)
        raise
    if build_num is None:
        logger.warning("[%s] No build number available. Will retry at next poke.", job_name)
        return False
    if not has_allure_plugin_report(job_name, build_num):
        logger.info(
            "[%s#%s] Plugin report NOT ready yet. Will retry.",
            job_name, build_num)
        return False
    artifact = get_allure_artifact_path(job_name, build_num)
    if artifact is None:
        logger.info(
            "[%s#%s] Plugin page OK, but zip artifact is not yet uploaded. Will retry.",
            job_name, build_num)
        return False
    logger.info(
        "[%s#%s] ALLURE READY (artifact=%s). Sensor returns True.",
        job_name, build_num, artifact)
    task_instance = context["ti"]
    task_instance.xcom_push(key=f"build_{job_name}", value=build_num)
    task_instance.xcom_push(key=f"artifact_{job_name}", value=artifact)
    return True


def flatten_extracted(extract_dir: str) -> None:
    """
    Если внутри extract_dir единственная папка (например, allure-report/),
    поднимаем её содержимое на уровень выше.
    """
    entries = os.listdir(extract_dir)
    # Если внутри больше 1 элемента (или пусто),
    # значит архив распаковался нормально, и выравнивание не требуется
    if len(entries) != 1:
        return
    single = os.path.join(extract_dir, entries[0])
    if not os.path.isdir(single):
        return
    logger.info("Smoothing nested folder '%s' inside %s", entries[0], extract_dir)
    # Перебираем всё содержимое вложенной папки (index.html, widgets, data)
    for item in os.listdir(single):
        # Перемещаем каждый элемент на уровень выше (в корень extract_dir)
        shutil.move(os.path.join(single, item), extract_dir)
    os.rmdir(single)


def download_allure_artifacts(**context):
    task_instance = context["ti"]
    jobs = context["params"]["jobs"]
    if os.path.exists(ALLURE_BASE_DIR):
        logger.info("Cleaning up previous allure reports at %s", ALLURE_BASE_DIR)
        shutil.rmtree(ALLURE_BASE_DIR)
    os.makedirs(ALLURE_BASE_DIR, exist_ok=True)
    logger.info("Starting download of %d reports: %s", len(jobs), jobs)
    result = {}
    for job in jobs:
        build_num = task_instance.xcom_pull(task_ids=f"wait_allure_{job}", key=f"build_{job}")
        artifact_path = task_instance.xcom_pull(task_ids=f"wait_allure_{job}", key=f"artifact_{job}")
        if not build_num or not artifact_path:
            logger.warning("[%s] No XCom data found (sensor was likely skipped). Ignoring this job.", job)
            continue
        url = f"{jenkins_url()}/job/{job}/{build_num}/artifact/{artifact_path}"
        logger.info("[%s#%s] Downloading %s", job, build_num, url)

        r = requests.get(url, auth=auth(), timeout=180, verify=False)
        r.raise_for_status()
        logger.info("[%s#%s] Downloaded %d bytes", job, build_num, len(r.content))

        zip_path = os.path.join(ALLURE_BASE_DIR, f"{job}.zip")
        with open(zip_path, "wb") as f:
            f.write(r.content)
        extract_dir = os.path.join(ALLURE_BASE_DIR, job)
        os.makedirs(extract_dir, exist_ok=True)
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(extract_dir)
        logger.info("[%s#%s] Extracted to %s", job, build_num, extract_dir)
        flatten_extracted(extract_dir)
        index_html = os.path.join(extract_dir, "index.html")
        if not os.path.exists(index_html):
            logger.warning(
                "[%s#%s] index.html not found at %s — check zip structure",
                job, build_num, index_html)
        else:
            logger.info("[%s#%s] index.html OK at %s", job, build_num, index_html)
        result[job] = {
            "build": build_num,
            "extract_dir": extract_dir,
            "index_html": index_html,
            "plugin_url": f"{jenkins_url()}/job/{job}/{build_num}/allure/",
        }
    if not result:
        raise ValueError("All sensors were skipped or failed. No reports downloaded.")
    task_instance.xcom_push(key="allure_paths", value=result)
    logger.info("Allure download complete: %d reports in XCom 'allure_paths'", len(result))
    return result
