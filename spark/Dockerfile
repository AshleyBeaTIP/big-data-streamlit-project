FROM python:3.10-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pyspark==3.5.1 pymongo
WORKDIR /app
COPY spark_job.py .
CMD spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_job.py