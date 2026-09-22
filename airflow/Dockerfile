FROM apache/airflow:2.10.3

# Switch to root to install system-level packages (Java for PySpark)
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark to find it
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Switch back to the airflow user for Python package installs
USER airflow

# Install Python dependencies your pipeline scripts need
RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    pandas \
    pyarrow \
    psycopg2-binary \
    requests \
    python-dotenv \
    dbt-postgres==1.11.0