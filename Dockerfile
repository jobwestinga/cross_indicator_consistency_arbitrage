FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.txt requirements-test.txt ./

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements-test.txt

COPY pyproject.toml README.md ./
COPY src ./src
COPY sql ./sql
COPY samples ./samples
COPY tests ./tests

ENTRYPOINT ["python", "-m", "forecast_collector.cli"]
