# Graphify code-analysis of Apache Airflow (arm64/amd64 multi-arch base). No API keys / no LLM.
FROM python:3.12-slim

ARG GRAPHIFY_VERSION=0.9.92
ARG AIRFLOW_REF=main
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    AIRFLOW_REF=${AIRFLOW_REF} \
    ANALYZE_AT_BUILD=true

# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir "graphifyy==${GRAPHIFY_VERSION}" \
    && useradd -m -u 10001 graphify

COPY entrypoint.sh analyze.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/analyze.sh \
    && mkdir -p /srv/site /work && chown -R graphify /srv /work
USER 10001
WORKDIR /work

# Run the analysis at build time so the pod starts instantly (set --build-arg to skip: see README)
RUN if [ "$ANALYZE_AT_BUILD" = "true" ]; then /usr/local/bin/analyze.sh; fi

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
