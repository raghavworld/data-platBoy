FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    boto3==1.35.99 \
    pymongo==4.10.1 \
    psycopg2-binary==2.9.10 \
    python-dotenv==1.0.1 \
    trino==0.336.0 \
    pyspark==3.5.5
