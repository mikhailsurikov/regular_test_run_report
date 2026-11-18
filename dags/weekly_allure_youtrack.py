from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from utils.allure_parser import parse_allure_stats
from utils.jenkins_api import download_allure_artifacts, wait_for_allure
from utils.screenshot import take_screenshot
from utils.youtrack_api import get_and_update_youtrack_comment, update_youtrack_history_table
from utils.notifier import send_failure_alert
from airflow.models import Variable

# Список джоб из Jenkins для сбора отчетов.
JENKINS_JOBS = Variable.get(
    "JENKINS_JOBS",
    deserialize_json=True,
    default_var=[
        "QA110_regular_run_1",
        "QA110_regular_run_2",
        "QA110_regular_run_3",
        "QA110_regular_run_6_SMSSW"
    ]
)

default_args = {
    "owner": "qa",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_failure_alert,
    "start_date": datetime(2026, 1, 1),
}

with DAG(
        dag_id="weekly_allure_youtrack",
        description="Еженедельный сбор Allure отчётов из четырёх Jenkins-джоб и публикация сводки в существующий issue YouTrack",
        default_args=default_args,
        schedule_interval="0 2 * * 1",  # каждый понедельник 02:00 UTC = 05:00 MSK
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        dagrun_timeout=timedelta(hours=2),
        tags=["qa", "allure", "youtrack", "weekly"],
) as dag:
    # 1. Четыре параллельных сенсора. Каждый ждёт, пока в «своей» джобе появится Allure-отчёт (и плагин, и артефакт).
    wait_tasks = [
        PythonSensor(
            task_id=f"wait_allure_{job}",
            python_callable=wait_for_allure,
            op_kwargs={"job_name": job},
            poke_interval=120,  # проверяем jenkins каждые 2 минуты
            timeout=600,  # ждём максимум 10 минут
            mode="reschedule",  # освобождаем слот воркера между проверками
            soft_fail=True
        )
        for job in JENKINS_JOBS]

    # 2. Скачиваем и распаковываем архивы артефактов.
    #    jobs передаём в params — внутри download_allure_artifacts
    #    читается context["params"]["jobs"].
    download = PythonOperator(
        task_id="download_allure_reports",
        python_callable=download_allure_artifacts,
        params={"jobs": JENKINS_JOBS},
        trigger_rule="none_failed"
    )

    # 3. Парсим распакованные отчёты → статистика по отдельной джобе.
    parse = PythonOperator(
        task_id="parse_allure_stats",
        python_callable=parse_allure_stats,
        params={"jobs": JENKINS_JOBS}
    )

    # 4. Создание скриншота страницы Allure (по одному PNG на джобу).
    screenshot = PythonOperator(
        task_id="take_screenshot",
        python_callable=take_screenshot,
        params={"jobs": JENKINS_JOBS}
    )

    # 5. Добавляем комментарий со статистикой по тест-рану в issue YouTrack.
    create_comment = PythonOperator(
        task_id="get_and_update_youtrack_comment",
        python_callable=get_and_update_youtrack_comment,
    )

    # 6. Обновляем таблицу в issue YouTrack.
    update_table = PythonOperator(
        task_id="update_youtrack_history_table",
        python_callable=update_youtrack_history_table,
    )

    # Граф зависимостей:
    #   все 4 сенсора (параллельно) >> download >> parse >> screenshot >> create_comment >> update_table

    wait_tasks >> download >> parse >> screenshot >> create_comment >> update_table
