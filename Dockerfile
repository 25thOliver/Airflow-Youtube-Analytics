FROM apache/airflow:3.0.6-python3.12

# Switch to root to install Java (required for PySpark)
USER root

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless \
      wget \
      procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable for Java 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Switch back to airflow user
USER airflow

# Copy requirements files
COPY requirements.txt .
COPY pyspark-requirements.txt .

# Install requirements separately with higher timeout/retries (pyspark is large)
RUN PIP_DEFAULT_TIMEOUT=120 PIP_RETRIES=15 pip install --no-cache-dir -r requirements.txt && \
    PIP_DEFAULT_TIMEOUT=120 PIP_RETRIES=15 pip install --no-cache-dir -r pyspark-requirements.txt

# Add Hadoop S3A connector jars for Spark (as airflow user, after pyspark is installed)
# --- Install Apache Spark runtime binaries ---
USER root
RUN mkdir -p /opt && \
    echo "Downloading Apache Spark 3.5.0 (Hadoop 3)..." && \
    (wget --progress=dot:giga -t 3 -O /opt/spark.tgz https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz \
     || wget --progress=dot:giga -t 3 -O /opt/spark.tgz https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz) && \
    tar -xzf /opt/spark.tgz -C /opt/ && \
    mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm /opt/spark.tgz && \
    ln -s /opt/spark/bin/spark-submit /usr/local/bin/spark-submit && \
    ln -s /opt/spark/bin/pyspark /usr/local/bin/pyspark && \
    ln -s /opt/spark/bin/spark-shell /usr/local/bin/spark-shell

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:/opt/spark/bin

USER airflow


# Pin SQLAlchemy to version compatible with Airflow 3.0.6 (must be < 2.0)
# Airflow 3.0.6 doesn't support SQLAlchemy 2.x - force downgrade after all installs
RUN pip install --no-cache-dir --force-reinstall "sqlalchemy<2.0.0,>=1.4.0"

# Set environment variables for PySpark
ENV PYSPARK_PYTHON=/usr/local/bin/python
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python

# Optionally install s3fs if still needed
RUN pip install s3fs

RUN pip install pymongo
