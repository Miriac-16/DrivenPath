# Syntax=docker/dockerfile:1.4
ARG AIRFLOW_VERSION=2.10.2
ARG PYTHON_VERSION=3.10
ARG BUILD_DATE=latest

# Use the official Airflow image as a template.
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Switch to airflow user to install packages.
USER airflow

COPY requrements.txt /requrements.txt
RUN pip install --no-cache-dir -r /requrements.txt

# Puedes eliminar o dejar comentadas las líneas de instalación individual si prefieres usar solo requirements.txt
# RUN pip install --no-cache-dir \
#     "apache-airflow-providers-postgres" \
#     "dbt-postgres==1.6.0" \
#     "faker"

LABEL org.opencontainers.image.authors="your_email@example.com"
LABEL org.opencontainers.image.url="https://github.com/your_github_repo"
LABEL org.opencontainers.image.documentation="https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html"
LABEL org.opencontainers.image.source="https://github.com/apache/airflow-docker-examples"
LABEL org.opencontainers.image.version="${AIRFLOW_VERSION}"
LABEL org.opencontainers.image.created="${BUILD_DATE}"