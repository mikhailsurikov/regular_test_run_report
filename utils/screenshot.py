import logging
import os
import threading
import time
from contextlib import contextmanager
from ftplib import FTP, error_perm
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from typing import Iterator
from airflow.models import Variable
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from utils.jenkins_api import ALLURE_BASE_DIR

logger = logging.getLogger("airflow.task")
SCREENSHOT_TEMP_DIR = "/tmp/screenshots"


@contextmanager
def local_http_server(directory: str) -> Iterator[str]:
    handler = partial(SimpleHTTPRequestHandler, directory=directory)
    server = HTTPServer(("127.0.0.1", 0), handler)
    actual_port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{actual_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)


def make_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def upload_to_ftp(local_path: str, filename: str) -> str:
    """
    Загружает скриншот на FTP-сервер.
    Возвращает публичный HTTP-URL файла (или пустую строку при ошибке).
    """
    ftp_host = Variable.get("FTP_HOST")
    ftp_port = int(Variable.get("FTP_PORT", default_var="21"))
    ftp_user = Variable.get("FTP_USER")
    ftp_password = Variable.get("FTP_PASSWORD")
    ftp_remote_dir = Variable.get("FTP_REMOTE_DIR", default_var="/").strip("/")
    public_base = Variable.get("FTP_PUBLIC_URL").rstrip("/")

    logger.info(
        "Uploading %s to FTP %s:%d (dir=%s)",
        filename, ftp_host, ftp_port, ftp_remote_dir)

    ftp = FTP()
    try:
        ftp.connect(ftp_host, ftp_port, timeout=30)
        ftp.login(ftp_user, ftp_password)
        ftp.set_pasv(True)  # пассивный режим (обычно нужен за NAT/firewall)

        # Переходим в целевую папку. Если её нет — создаём по сегментам.
        if ftp_remote_dir:
            for part in ftp_remote_dir.split("/"):
                if not part:
                    continue
                try:
                    ftp.cwd(part)
                except error_perm:
                    logger.info("Creating remote directory: %s", part)
                    ftp.mkd(part)
                    ftp.cwd(part)

        # Загружаем файл в бинарном режиме
        with open(local_path, "rb") as f:
            ftp.storbinary(f"STOR {filename}", f)

        public_url = f"{public_base}/{filename}"
        logger.info("FTP upload OK -> %s", public_url)
        return public_url

    except Exception as e:
        logger.error("Failed to upload %s to FTP: %s", filename, e)
        return ""
    finally:
        try:
            ftp.quit()
        except Exception:
            ftp.close()


def take_screenshot(**context):
    ti = context["ti"]
    allure_paths = ti.xcom_pull(task_ids="download_allure_reports", key="allure_paths")
    if not allure_paths:
        raise ValueError("No allure_paths in XCom")

    os.makedirs(SCREENSHOT_TEMP_DIR, exist_ok=True)
    screenshot_urls = {}

    with local_http_server(ALLURE_BASE_DIR) as base_url:
        driver = make_driver()
        try:
            for job in allure_paths:
                url = f"{base_url}/{job}/index.html"
                logger.info("[%s] Opening %s", job, url)
                driver.get(url)
                time.sleep(2)

                date_str = context["ds_nodash"]
                filename = f"{job}_{date_str}.png"
                local_path = os.path.join(SCREENSHOT_TEMP_DIR, filename)

                driver.save_screenshot(local_path)
                logger.info("[%s] Local screenshot saved: %s", job, local_path)

                public_url = upload_to_ftp(local_path, filename)
                screenshot_urls[job] = public_url

                os.remove(local_path)
        finally:
            logger.info("Quitting Chrome driver")
            driver.quit()

    ti.xcom_push(key="screenshots", value=screenshot_urls)
    return screenshot_urls
