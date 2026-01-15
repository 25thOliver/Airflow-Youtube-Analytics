# Base Airflow image (Python 3.12)
FROM apache/airflow:3.0.6-python3.12

# -----------------------------------------------------
# Install Java 17 (required for Spark)
# -----------------------------------------------------
USER root

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        wget \
        curl \
        ca-certificates \
        procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# -----------------------------------------------------
# Install Python dependencies
# -----------------------------------------------------
USER airflow

COPY requirements.txt .

# Install your Python requirements
RUN PIP_DEFAULT_TIMEOUT=120 \
    PIP_RETRIES=10 \
    pip install --no-cache-dir -r requirements.txt

# IMPORTANT: Do NOT install pyspark via pip!
# Spark binary already includes PySpark.

# -----------------------------------------------------
# Install Apache Spark 3.5.0 (Hadoop 3)
# -----------------------------------------------------
USER root

RUN mkdir -p /opt && \
    echo "Downloading Apache Spark 3.5.0 (Hadoop 3)..." && \
    wget -q --show-progress -O /opt/spark.tgz \
        https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf /opt/spark.tgz -C /opt/ && \
    mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm /opt/spark.tgz && \
    ln -s /opt/spark/bin/spark-submit /usr/local/bin/spark-submit && \
    ln -s /opt/spark/bin/pyspark /usr/local/bin/pyspark && \
    ln -s /opt/spark/bin/spark-shell /usr/local/bin/spark-shell && \
    \

# --- Add Hadoop AWS + AWS Java SDK for S3 (MinIO) ---
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.6.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazon/aws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar


# Hadoop S3A configuration for Spark
RUN mkdir -p /opt/spark/conf && \
    echo "fs.s3a.endpoint=${MINIO_ENDPOINT}" >> /opt/spark/conf/core-site.xml && \
    echo "fs.s3a.access.key=${MINIO_ACCESS_KEY}" >> /opt/spark/conf/core-site.xml && \
    echo "fs.s3a.secret.key=${MINIO_SECRET_KEY}" >> /opt/spark/conf/core-site.xml && \
    echo "fs.s3a.path.style.access=true" >> /opt/spark/conf/core-site.xml && \
    echo "fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" >> /opt/spark/conf/core-site.xml && \
    echo "fs.s3a.connection.ssl.enabled=false" >> /opt/spark/conf/core-site.xml


# Spark environment
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=/usr/local/bin/python
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python

# Ensure Python can import pyspark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# -----------------------------------------------------
# Fix SQLAlchemy version for Airflow 3.x (optional)
# -----------------------------------------------------
USER airflow
RUN pip install --no-cache-dir --force-reinstall \
    "SQLAlchemy==1.4.54"

# -----------------------------------------------------
# Install optional extras
# -----------------------------------------------------
RUN pip install --no-cache-dir s3fs pymongo

# -----------------------------------------------------
# Validate the installation during build (optional)
# -----------------------------------------------------
USER root
RUN java -version && python --version && spark-submit --version || true

# Back to airflow user
USER airflow