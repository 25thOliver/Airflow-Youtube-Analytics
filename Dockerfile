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

# Add Hadoop S3A connector jars for Spark
# Adjust versions as needed; these are example versions
RUN mkdir -p /opt/spark/jars && \
    # Hadoop AWS, matching a typical Spark/Hadoop 3.x environment
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar -P /opt/spark/jars/ && \
    # AWS Java SDK bundle, must be compatible
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -P /opt/spark/jars/ && \
    # Optional: ensure ownership/permissions if needed
    chown -R airflow: /opt/spark/jars

# Switch back to airflow user
USER airflow

# Copy requirements files
COPY requirements.txt .
COPY pyspark-requirements.txt .

# Install requirements separately with higher timeout/retries (pyspark is large)
RUN PIP_DEFAULT_TIMEOUT=120 PIP_RETRIES=15 pip install --no-cache-dir -r requirements.txt && \
    PIP_DEFAULT_TIMEOUT=120 PIP_RETRIES=15 pip install --no-cache-dir -r pyspark-requirements.txt

# Pin SQLAlchemy to version compatible with Airflow 3.0.6 (must be < 2.0)
# Airflow 3.0.6 doesn't support SQLAlchemy 2.x - force downgrade after all installs
RUN pip install --no-cache-dir --force-reinstall "sqlalchemy<2.0.0,>=1.4.0"

# Set environment variables for PySpark
ENV PYSPARK_PYTHON=/usr/local/bin/python
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python

# Optionally install s3fs if still needed
RUN pip install s3fs

RUN pip install pymongo

# Copy user creation script (switch to root to set permissions)
USER root
COPY create_airflow_user.py /opt/airflow/create_airflow_user.py
RUN chmod +x /opt/airflow/create_airflow_user.py && \
    chown airflow: /opt/airflow/create_airflow_user.py
USER airflow
