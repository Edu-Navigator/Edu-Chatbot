FROM apache/airflow:2.9.1-python3.10

USER root

# 아키텍처 자동 감지 및 대응
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    fonts-nanum \
    fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

# 플랫폼별 Chromium 설치
RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "amd64" ]; then \
        apt-get update && apt-get install -y chromium chromium-driver; \
    elif [ "$ARCH" = "arm64" ]; then \
        apt-get update && apt-get install -y chromium chromium-driver; \
    fi && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Selenium이 Chromium을 찾을 수 있도록 환경변수 설정
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver