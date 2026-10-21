# Official multi-arch image (linux/arm64 works on MacBook M1/M2)
ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

USER root
RUN pip install --no-cache-dir \
    "apache-airflow-providers-cncf-kubernetes==10.22.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"
USER airflow

COPY --chown=airflow:root dags/ /opt/airflow/dags/
