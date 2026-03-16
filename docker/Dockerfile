FROM apache/airflow:2.11.0-python3.12

USER root

COPY requirements.txt /requirements.txt

USER airflow

RUN pip install --no-cache-dir -r /requirements.txt