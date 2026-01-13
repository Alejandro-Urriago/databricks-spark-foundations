FROM python:3.10-slim-bookworm

# System dependencies
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    curl \
    git \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Spark installation
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

ENV PATH="$SPARK_HOME/bin:$PATH"

# Python dependencies
WORKDIR /app
COPY docker/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy local scripts
COPY local/ /app/local/
