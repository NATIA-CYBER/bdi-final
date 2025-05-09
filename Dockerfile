FROM python:3.9

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

ENV PYTHONPATH=/app
ENV POSTGRES_HOST=airflow_2061a7-postgres-1
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=airflow
ENV POSTGRES_USER=airflow
ENV POSTGRES_PASSWORD=airflow

CMD ["uvicorn", "bdi_api.app:app", "--host", "0.0.0.0", "--port", "8000", "--reload-dir", "/app"]
