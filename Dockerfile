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
# PySpark 3.5.0 uses Hadoop 3.3.4, so we need matching JARs
USER root
RUN mkdir -p /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars && \
    # Hadoop AWS 3.3.4 to match PySpark's Hadoop version
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/ && \
    # AWS Java SDK bundle (compatible version)
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -P /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/ && \
    chown -R airflow: /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars
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
