# Official multi-arch image (linux/arm64 works on MacBook M1/M2)
ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12
COPY --chown=airflow:root dags/ /opt/airflow/dags/
