FROM apache/airflow:3.0.3

USER root
# Install OpenJDK 17
RUN apt-get update && apt-get install -y openjdk-17-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Install PySpark
USER airflow
RUN pip install pyspark==3.5.6

USER airflow