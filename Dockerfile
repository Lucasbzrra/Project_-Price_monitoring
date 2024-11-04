# Use a imagem do Apache Airflow como base
FROM apache/airflow:2.10.1

# Execute como root para instalar Java e bibliotecas necessárias
USER root

# Instale o OpenJDK 17 e o procps
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk procps curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Crie o diretório de JARs e baixe o driver PostgreSQL


# No Dockerfile do Airflow
RUN mkdir -p /opt/airflow/spark/jars && \
    curl -L -o /opt/airflow/spark/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -L -o /opt/airflow/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    chmod 777 /opt/airflow/spark/jars/*

# Configura as variáveis de ambiente para o Spark e Java
ENV SPARK_JARS /opt/airflow/spark/jars/
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Instale bibliotecas Python para integração do Airflow com Spark, PostgreSQL, e Amazon S3
USER airflow

RUN pip install selenium \
    pyspark==3.5.2 \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-postgres \
    apache-airflow-providers-amazon