FROM python:3.10-slim

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=2.7.0
ENV PYTHON_VERSION=3.10
ENV AIRFLOW_USER_HOME=/home/airflow

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    libpq-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash airflow \
    && mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/plugins \
    && chown -R airflow: ${AIRFLOW_HOME}

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER airflow
WORKDIR ${AIRFLOW_HOME}

COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY profiles.yml ${AIRFLOW_USER_HOME}/.dbt/profiles.yml

EXPOSE 8080
ENTRYPOINT ["airflow", "standalone"]
CMD ["webserver"]
