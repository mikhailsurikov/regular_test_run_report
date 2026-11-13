
***

# QA Airflow Reporter — Weekly Allure Report to YouTrack

Автоматизированный еженедельный сбор отчётов Allure из четырех Jenkins-джоб и публикация сводки в существующий issue YouTrack.

## О проекте

В процессе регулярного регрессионного тестирования результаты прогонов автотестов накапливаются в CI/CD-системе (Jenkins). По завершении каждой джобы формируется Allure-отчёт, который публикуется двумя способами:

Плагин Allure — для визуального просмотра в браузере.

Zip-архив в артефактах — содержит сырые JSON-файлы для машинной обработки.

Проблема: При наличии нескольких параллельных джоб (например, разделенных по функциональным блокам), процесс сбора статистики становится рутиной. QA-инженеру приходится вручную открывать каждую джобу, выписывать общее количество тестов, считать упавшие кейсы, фиксировать, в каких Suites произошли сбои, делать скриншоты и вручную переносить это в таск-трекер (YouTrack). Это отнимает время, подвержено человеческому фактору и не позволяет вести наглядную историю регрессионных прогонов (тренды по неделям).

Решение: Автоматизированный ETL-пайплайн на базе Apache Airflow. DAG решает задачу «под ключ»:

Извлекает (Extract) — отслеживает появление свежих отчётов в Jenkins, скачивает и распаковывает архивы.

Трансформирует (Transform) — программно парсит summary.json и suites.json, агрегируя статистику (Total/Passed/Failed/Broken) и формируя списки упавших сьютов.

Загружает (Load) — делает скриншоты отчётов, выгружает их на удалённый FTP-хостинг и публикует результаты в YouTrack. Помимо детального комментария по текущему запуску, скрипт автоматически поддерживает актуальную историческую таблицу в описании задачи, скрывая старые данные под спойлер.

Таким образом, система избавляет команду от ручного сбора метрик и предоставляет единую точку правды по статусу регресса с удобной историей.

## Ключевые требования

| # | Требование | Реализация |
|---|---|---|
| 1 | Проверить, что в 4 джобах появились Allure-отчёты | `PythonSensor` × 4 (параллельно, с таймаутом) |
| 2 | Скачать архивы артефактов, распаковать и выровнять структуру | `download_allure_artifacts` + `flatten_extracted` |
| 3 | Распарсить статистику (total/passed/failed/broken/skipped) + разбивка по сьютам | `parse_allure_stats` |
| 4 | Сделать скриншот страницы Allure и загрузить на FTP | `take_screenshot` (Selenium + локальный HTTP-сервер) |
| 5 | Создать новый комментарий с деталями по каждой джобе и ссылками на скриншоты | `get_and_update_youtrack_comment` |
| 6 | Обновить историческую таблицу в описании задачи (топ-10 + скрытые под спойлером) | `update_youtrack_history_table` |
| 7 | Отправка алертов при падении | `notifier.py` (через `on_failure_callback`) |

## Архитектура

```text
                           +-----------------------------+
                           |    weekly_allure_youtrack   |
                           +-----------------------------+
                                        |
             +--------------+-----------+-----------+--------------+
             |              |                       |              |
             v              v                       v              v
    +----------------+ +----------------+ +----------------+ +----------------+
    | wait_allure_1  | | wait_allure_2  | | wait_allure_3  | | wait_allure_4  | (Sensors)
    +----------------+ +----------------+ +----------------+ +----------------+
             |              |                       |              |
             +--------------+-----------+-----------+--------------+
                                        |
                                        v
                           +-----------------------------+
                           |    download_allure_reports  | (Download, Unzip, Flatten)
                           +-----------------------------+
                                        |
                                        v
                           +-----------------------------+
                           |      parse_allure_stats     | (Parse JSON, Format Date)
                           +-----------------------------+
                                        |
                                        v
                           +-----------------------------+
                           |       take_screenshot       | (Selenium + Local HTTP + FTP)
                           +-----------------------------+
                                        |
                     +------------------+------------------+
                     |                                     |
                     v                                     v
        +-----------------------------+       +-----------------------------+
        | get_and_update_youtrack_    |       |  update_youtrack_history_   |
        |         comment             |       |          table              |
        +-----------------------------+       +-----------------------------+
        (Publishes new comment with           (Updates Issue Description
         screenshot links & details)           with top-10 rows + <details>)
```

**Ключевые архитектурные решения:**
*   **Изоляция бизнес-логики (Separation of Concerns)**: DAG-файл содержит только конфигурацию, расписание и связи между задачами. Вся бизнес-логика инкапсулирована в изолированных модулях пакета `utils`, что упрощает тестирование, поддержку и переиспользование кода.
*   **Динамическая генерация задач (Dynamic Task Generation)**: Ожидание готовности четырех независимых джоб реализовано через List Comprehension в Python. Это позволяет на этапе парсинга DAG автоматически создавать 4 параллельных `PythonSensor`. При добавлении новой джобы в константу `JENKINS_JOBS` пайплайн масштабируется без изменения основного кода.
*   **Обход ограничений CORS при рендеринге (Local HTTP Server)**: Для создания скриншотов используется Selenium. Чтобы браузер Chrome мог корректно отрисовать JavaScript-графики Allure-отчета, вместо открытия локального файла (протокол `file://`, который блокируется политикой безопасности) в фоновом потоке поднимается временный HTTP-сервер на `127.0.0.1` со случайным свободным портом.
*   **Идемпотентность при обновлении YouTrack (Safe Updates via Markers)**: Для ведения исторической таблицы используется описание задачи (Issue Description). Чтобы скрипт не затер случайный текст, написанный QA-инженером вручную, применяется паттерн идемпотентного обновления на основе скрытых HTML-маркеров (`<!-- HISTORY_TABLE_START -->` и `<!-- HISTORY_TABLE_END -->`). Скрипт обновляет только контент между ними, оставляя остальное описание нетронутым.
*   **Отделение тяжелых медиа-данных (XCom for Metadata)**: В XCom (метабазу Airflow) передаются только пути к файлам и агрегированная статистика. Сами архивы Allure и PNG-скриншоты не хранятся в контейнерах, а сразу выгружаются на выделенный FTP-хостинг. Это предотвращает раздувание базы данных Airflow и переполнение дискового пространства воркеров.
## Технологический стек

| Компонент | Версия | Назначение |
|---|---|---|
| Apache Airflow | 2.8.1 | Оркестратор |
| Python | 3.10 | Язык |
| PostgreSQL | 13+ | Метабаза Airflow |
| Redis | 7+ | Брокер (CeleryExecutor) |
| Docker / Docker Compose | latest | Контейнеризация |
| Selenium + Chrome | latest | Скриншоты Allure-отчётов |
| Jenkins API | — | Источник артефактов |
| YouTrack REST API | — | Целевая система |


## Структура проекта

```text
qa-airflow-reporter/
│
├── dags/
│   └── weekly_allure_youtrack.py     # Объявление DAG, расписание, связи
│
├── utils/                            # Логика реализации
│   ├── __init__.py
│   ├── fs_utils.py                   # Утилиты файловой системы (рекурсивный поиск)
│   ├── jenkins_api.py                # Jenkins: сенсоры, скачивание, распаковка
│   ├── allure_parser.py              # Парсинг Allure JSON → статистика
│   ├── screenshot.py                 # Selenium, локальный сервер, загрузка на FTP
│   ├── youtrack_client.py            # Базовые настройки YouTrack (URL, токены)
│   ├── youtrack_api.py               # YouTrack: комментарии, обновление описания
│   └── notifier.py                   # Отправка алертов при падении DAG
│
├── .env                              # AIRFLOW_UID (инфраструктура)
├── Dockerfile                        # Кастомный образ с Chrome
├── docker-compose.yaml               # Локальный запуск Airflow
├── requirements.txt
└── README.md                         # Этот файл
```

## Модули пакета utils

### `utils/fs_utils.py`
Содержит функцию `find_file(base_dir, filename)`. Использует `os.walk` для рекурсивного поиска файлов (например, `summary.json`) в любых подпапках, что делает парсер устойчивым к изменениям структуры архива.

### `utils/jenkins_api.py`
Единственная точка контакта с Jenkins.
*   `wait_for_allure(job_name, **context)`: Callable для PythonSensor. Возвращает `True`, когда готов и плагин, и артефакт. Сохраняет данные в XCom.
*   `download_allure_artifacts(**context)`: Скачивает zip-архивы всех джоб, распаковывает, применяет `flatten_extracted` (выравнивает структуру, если внутри архива была лишняя папка), кладет `allure_paths` в XCom.

### `utils/allure_parser.py`
Парсинг Allure JSON → агрегированная статистика.
*   `parse_allure_stats(**context)`: Читает `allure_paths` из XCom, находит `summary.json` и `suites.json`, высчитывает количество упавших тестов по сьютам (`failed_suites_str`) и ошибок (`error_suites_str`). Формирует `allure_summary` и пушит в XCom.

### `utils/screenshot.py`
Снятие скриншотов Allure через Selenium + локальный HTTP-сервер.
*   `local_http_server(directory)`: Поднимает http-сервер на свободном порту. Это нужно, чтобы Chrome не ругался на CORS-ошибки при открытии отчета по `file://`.
*   `take_screenshot(**context)`: Открывает отчеты, делает PNG, загружает их на FTP-сервер (`_upload_to_ftp`) и сохраняет публичные ссылки в XCom под ключом `screenshots`. Локальные файлы удаляются.

### `utils/youtrack_client.py` и `utils/youtrack_api.py`
`youtrack_client.py` хранит базовые функции (`_headers`, `_base_url`, `_issue_id`), чтобы избежать дублирования кода.
`youtrack_api.py` содержит:
*   `get_and_update_youtrack_comment()`: Создает новый комментарий с деталями по каждой джобе, ссылками на скриншоты и цветовой индикацией (passed/failed).
*   `update_youtrack_history_table()`: Ищет специальные маркеры в **описании задачи** (Issue Description) и генерирует таблицу. Оставляет 10 последних запусков видимыми, а остальные скрывает под тег `<details>`.

### `utils/notifier.py`
*   `send_failure_alert(context)`: Отправляет комментарий в YouTrack при падении любой таски (используется в `on_failure_callback`).

## Жизненный цикл данных (XCom)

| Шаг | Что кладёт в XCom | Кто читает |
|---|---|---|
| `wait_allure_<job>` ×4 | `build_<job>`, `artifact_<job>` | `download_allure_reports` |
| `download_allure_reports` | `allure_paths` | `parse_allure_stats`, `take_screenshot` |
| `parse_allure_stats` | `allure_summary` (dict: date, per_job) | `get_and_update_youtrack_comment`, `update_youtrack_history_table` |
| `take_screenshot` | `screenshots` (dict: job → public_ftp_url) | `get_and_update_youtrack_comment` |

*Правило: XCom — только для маленьких метаданных. Тяжёлые артефакты — на диске или на FTP.*

## Конфигурация

Все секреты хранятся в Airflow Variables (UI → Admin → Variables). 

| Variable | Пример | Описание |
|---|---|---|
| `JENKINS_URL` | `http://jenkins.local` | Базовый URL Jenkins |
| `JENKINS_USER` | `svc-airflow` | Логин сервисного аккаунта |
| `JENKINS_TOKEN` | `11a2b...` | API token |
| `YOUTRACK_URL` | `https://youtrack.local` | Базовый URL YouTrack |
| `YOUTRACK_TOKEN` | `perm:xxx...` | Permanent Token |
| `YOUTRACK_ISSUE_ID` | `QA-75` | ID существующего issue |
| `FTP_HOST` | `screenshot.local` | Хост FTP-сервера для скриншотов |
| `FTP_PORT` | `21` | Порт FTP |
| `FTP_USER` | `user` | Логин FTP |
| `FTP_PASSWORD` | `pass` | Пароль FTP |
| `FTP_REMOTE_DIR` | `folder/2024` | Папка на FTP |
| `FTP_PUBLIC_URL` | `http://screenshot.local/folder/2024` | HTTP-префикс для ссылок |

**Расписание:** `schedule_interval="0 2 * * 1"` (02:00 UTC = 05:00 MSK, понедельник).

**Список джоб** (в `dags/weekly_allure_youtrack.py`):
```python
JENKINS_JOBS = [
    "QA110_regular_run_1",
    "QA110_regular_run_2",
    "QA110_regular_run_3",
    "QA110_regular_run_6_SMSSW",
]
```

## Запуск проекта (локально)

1. Клонирование: `git clone <repo> && cd qa-airflow-reporter`
2. Создать `.env` с `AIRFLOW_UID=50000`
3. Запуск: `docker compose build --no-cache && docker compose up -d`
4. Заполнить Variables (см. таблицу выше).
5. В описании задачи YouTrack (Issue Description) добавить маркеры:
   `<!-- HISTORY_TABLE_START -->` и `<!-- HISTORY_TABLE_END -->` (между ними скрипт будет генерировать таблицу).
6. Запустить DAG (Trigger DAG).

## Ограничения и подводные камни

1. **LocalExecutor vs CeleryExecutor**: Файлы распаковываются в `/tmp/allure-reports/`. В CeleryExecutor таски могут выполняться на разных воркерах, где `/tmp` не общий. Для production рекомендуется использовать общий Volume (NFS) или S3. Скриншоты этой проблемы лишены, так как сразу загружаются на FTP.
2. **start_date**: `datetime(2026, 1, 1)` — намеренный хардкод в прошлом, необходимая точка отсчета для расписания (не путать с датой отчета, которая берется из контекста).
3. **Таймауты**: `timeout=600` в сенсорах установлен для тестов. В production увеличьте его до реального времени выполнения джоб (например, `7200`).
4. **Память Docker**: Airflow требует минимум 4 ГБ ОЗУ, выделенной для Docker Desktop.


## Ссылки

- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [Jenkins Remote API](https://www.jenkins.io/doc/book/using/remote-access-api/)
- [Allure Report Structure](https://docs.qameta.io/allure/#_report_structure)
- [YouTrack REST API](https://www.jetbrains.com/help/youtrack/devportal/youtrack-rest-api.html)