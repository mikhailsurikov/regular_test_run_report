FROM apache/airflow:2.8.1-python3.10

# ---------- 1. Устанавливаем Chrome и системные зависимости для Selenium ----------
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget \
        gnupg2 \
        unzip \
        ca-certificates \
        fonts-liberation \
        libasound2 \
        libatk-bridge2.0-0 \
        libatk1.0-0 \
        libatspi2.0-0 \
        libcups2 \
        libdbus-1-3 \
        libdrm2 \
        libgbm1 \
        libgtk-3-0 \
        libnspr4 \
        libnss3 \
        libx11-6 \
        libxcomposite1 \
        libxdamage1 \
        libxfixes3 \
        libxkbcommon0 \
        libxrandr2 \
        xdg-utils && \
    # Добавляем официальный репозиторий Google Chrome
    wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list' && \
    apt-get update && \
    apt-get install -y --no-install-recommends google-chrome-stable && \
    # Чистим кэш, чтобы образ не раздувался
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# ---------- 2. Возвращаемся к пользователю airflow (ОБЯЗАТЕЛЬНО до pip install) ----------
USER airflow

# ---------- 3. Устанавливаем Python-зависимости ----------
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt