# QA Airflow Reporter — Weekly Allure Report to YouTrack

Автоматизированный еженедельный сбор отчётов Allure из четырех Jenkins-джоб и публикация сводки в существующий issue YouTrack.

## О проекте

В процессе регулярного регрессионного тестирования результаты прогонов автотестов накапливаются в CI/CD-системе (Jenkins). По завершении каждой джобы формируется Allure-отчёт, который публикуется двумя способами:
* Плагин Allure — для визуального просмотра в браузере.
* Zip-архив в артефактах — содержит сырые JSON-файлы для машинной обработки.

**Проблема:** При наличии нескольких параллельных джоб процесс сбора статистики становится рутиной. QA-инженеру приходится вручную открывать каждую джобу, выписывать общее количество тестов, считать упавшие кейсы, делать скриншоты и переносить это в таск-трекер. Это отнимает время, подвержено человеческому фактору и не позволяет вести наглядную историю регрессионных прогонов.

**Решение:** Автоматизированный ETL-пайплайн на базе Apache Airflow. DAG решает задачу «под ключ»:
1. **Извлекает (Extract)** — отслеживает появление свежих отчётов в Jenkins, скачивает и распаковывает архивы.
2. **Трансформирует (Transform)** — парсит JSON-файлы, агрегирует статистику и формирует списки упавших сьютов.
3. **Загружает (Load)** — делает скриншоты, выгружает их на FTP и публикует результаты в YouTrack (комментарий и историческая таблица в описании).

## Ключевые требования

| # | Требование | Реализация |
|---|---|---|
| 1 | Проверить наличие Allure-отчётов в джобах | `PythonSensor` × N (динамически, с `soft_fail`) |
| 2 | Скачать архивы, распаковать и выровнять структуру | `download_allure_artifacts` + `flatten_extracted` |
| 3 | Распарсить статистику + разбивка по сьютам | `parse_allure_stats` |
| 4 | Сделать скриншот страницы Allure и загрузить на FTP | `take_screenshot` (Selenium + Explicit Wait) |
| 5 | Создать новый комментарий со статистикой | `get_and_update_youtrack_comment` |
| 6 | Обновить историческую таблицу (топ-10 + `<details>`) | `update_youtrack_history_table` |
| 7 | Отправка алертов при падении | `notifier.py` (через `on_failure_callback`) |
| 8 | **Отказоустойчивость**: продолжать работу, если джоба упала | Статус `FAILED/NO REPORT`, `trigger_rule="none_failed"` |

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
                                        v
                           +-----------------------------+
                           | get_and_update_youtrack_    |
                           |         comment             | (Publishes new comment)
                           +-----------------------------+
                                        |
                                        v
                           +-----------------------------+
                           |  update_youtrack_history_   |
                           |          table              | (Updates Issue Description
                           +-----------------------------+  with top-10 rows + <details>)
```

**Ключевые архитектурные решения:**

* **Изоляция логики (Separation of Concerns)**: DAG-файл содержит только конфигурацию и связи задач. Вся логика инкапсулирована в модулях пакета `utils`.
* **Динамическая генерация задач (Dynamic Task Generation)**: Список джоб читается из Airflow Variables (JSON). Сенсоры создаются автоматически через List Comprehension, что позволяет масштабировать пайплайн без изменения кода.
* **Отказоустойчивость (Soft-fail)**: Если джоба в Jenkins упала и не сгенерировала Allure-отчёт, сенсор завершается со статусом `skipped` (`soft_fail=True`). Пайплайн продолжает работу, а в отчётности эта джоба помечается как `FAILED / NO REPORT`.
* **Обход ограничений CORS при рендеринге**: Для скриншотов используется Selenium. Вместо открытия файла (`file://`), который блокируется браузером, поднимается локальный HTTP-сервер на `127.0.0.1`.
* **Идемпотентность в YouTrack**: Обновление исторической таблицы происходит строго между скрытыми HTML-маркерами (`<!-- HISTORY_TABLE_START -->`), что защищает остальной текст описания задачи от затирания.
* **Отделение тяжелых медиа-данных**: В XCom передаются только пути к файлам и агрегированная статистика. Архивы и PNG-скриншоты выгружаются на FTP-хостинг.

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

### `utils/jenkins_api.py`
Единственная точка контакта с Jenkins.
* `wait_for_allure()`: Callable для PythonSensor. Возвращает `True`, когда готов и плагин, и артефакт.
* `download_allure_artifacts()`: Скачивает zip-архивы, распаковывает, применяет `flatten_extracted` (выравнивает структуру). В случае ошибки скачивания конкретной джобы — логирует её и пропускает.

### `utils/allure_parser.py`
Парсинг Allure JSON → агрегированная статистика.
* `parse_allure_stats()`: Читает список джоб из `context["params"]` и `allure_paths` из XCom. Если отчета нет или в нём 0 кейсов — присваивает статус `"FAILED/NO REPORT"`. Иначе агрегирует статистику и списки упавших сьютов.

### `utils/screenshot.py`
Снятие скриншотов Allure через Selenium + локальный HTTP-сервер.
* `local_http_server()`: Поднимает http-сервер на свободном порту для обхода CORS.
* `upload_to_ftp()`: Загружает PNG на FTP и возвращает публичный URL.
* `take_screenshot()`: Использует `WebDriverWait` (Explicit Wait) для ожидания отрисовки DOM-элементов Allure (`.widget`), делает снимок и отправляет на FTP.

### `utils/youtrack_client.py` и `utils/youtrack_api.py`
`youtrack_client.py` хранит базовые функции (`headers`, `base_url`, `issue_id`).
`youtrack_api.py` содержит:
* `get_and_update_youtrack_comment()`: Создает новый комментарий с деталями и ссылками. Если статус `"FAILED/NO REPORT"`, выводит ссылки и красную надпись.
* `update_youtrack_history_table()`: Обновляет таблицу в описании задачи. Сохраняет 10 последних запусков видимыми, остальные скрывает под тег `<details>`. Умное окрашивание (зеленый/красный/синий) на основе сравнения с предыдущим запуском.

### `utils/notifier.py`
* `send_failure_alert()`: Отправляет комментарий в YouTrack при падении таски (вызывается через `on_failure_callback`).

## Жизненный цикл данных (XCom)

| Шаг | Что кладёт в XCom | Кто читает |
|---|---|---|
| `wait_allure_<job>` | `build_<job>`, `artifact_<job>` | `download_allure_reports` |
| `download_allure_reports` | `allure_paths` | `parse_allure_stats`, `take_screenshot` |
| `parse_allure_stats` | `allure_summary` | `get_and_update_youtrack_comment`, `update_youtrack_history_table` |
| `take_screenshot` | `screenshots` (dict: job → public_url) | `get_and_update_youtrack_comment` |

## Конфигурация

Все секреты и настройки хранятся в Airflow Variables (UI → Admin → Variables).

| Variable | Пример | Описание |
|---|---|---|
| `JENKINS_URL` | `http://jenkins.local` | Базовый URL Jenkins |
| `JENKINS_USER` | `svc-airflow` | Логин сервисного аккаунта |
| `JENKINS_TOKEN` | `11a2b...` | API token |
| `YOUTRACK_URL` | `https://youtrack.local` | Базовый URL YouTrack |
| `YOUTRACK_TOKEN` | `perm:xxx...` | Permanent Token |
| `YOUTRACK_ISSUE_ID` | `QA-75` | ID существующего issue |
| `FTP_HOST` | `screenshot.local` | Хост FTP-сервера |
| `FTP_PORT` | `21` | Порт FTP |
| `FTP_USER` | `user` | Логин FTP |
| `FTP_PASSWORD` | `pass` | Пароль FTP |
| `FTP_REMOTE_DIR` | `folder/2024` | Папка на FTP |
| `FTP_PUBLIC_URL` | `http://screenshot.local/folder/2024` | HTTP-префикс для ссылок |
| `JENKINS_JOBS` | `["QA110_regular_run_1", ...]` | **JSON-массив** с именами джоб |

**Расписание:** `schedule_interval="0 2 * * 1"` (02:00 UTC = 05:00 MSK, понедельник).

## Запуск проекта (локально)

1. Клонирование: `git clone <repo> && cd qa-airflow-reporter`
2. Создать `.env` с `AIRFLOW_UID=50000`
3. Запуск: `docker compose build --no-cache && docker compose up -d`
4. Заполнить Variables (см. таблицу выше). Для `JENKINS_JOBS` использовать формат JSON.
5. В описании задачи YouTrack добавить маркеры: `<!-- HISTORY_TABLE_START -->` и `<!-- HISTORY_TABLE_END -->`.
6. Запустить DAG (Trigger DAG).

## Ограничения и подводные камни

1. **LocalExecutor vs CeleryExecutor**: Файлы распаковываются в `/tmp/allure-reports/`. В CeleryExecutor таски могут выполняться на разных воркерах. Для production рекомендуется общий Volume (NFS) или S3. Скриншоты этой проблемы лишены (сразу летят на FTP).
2. **`start_date` и дата отчета**: `start_date=datetime(2026, 1, 1)` — намеренный хардкод в прошлом для расписания. **Дата самого отчета** берется из `datetime.now()` в момент выполнения, чтобы при ручных перезапусках старых DAG-ранов не записывались старые даты.
3. **Таймауты**: `timeout=600` в сенсорах установлен для тестов. В production увеличьте до реального времени выполнения джоб.
4. **Память Docker**: Airflow требует минимум 4 ГБ ОЗУ, выделенной для Docker Desktop.

## Ссылки
- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [Jenkins Remote API](https://www.jenkins.io/doc/book/using/remote-access-api/)
- [Allure Report Structure](https://docs.qameta.io/allure/#_report_structure)
- [YouTrack REST API](https://www.jetbrains.com/help/youtrack/devportal/youtrack-rest-api.html)