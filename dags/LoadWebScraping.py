from airflow.models import DAG
from airflow import Dataset
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



with DAG('LoadWebScraping',
         schedule=None,
         catchup=False,
         start_date=None,
         description='Dag responsável por criação de tabelas e inserção de dados'
) as dag:
        


        CREATE_TABLES=PostgresOperator(
                task_id='create_tables',
                postgres_conn_id='postgres-airflow',
                sql = """ 

                CREATE TABLE IF NOT EXISTS TB_DIM_SITE
                (
                ID_SITE SERIAL PRIMARY KEY,
                NAME_SITE VARCHAR(255) UNIQUE
                );

                CREATE TABLE IF NOT EXISTS TB_DIM_CATEGORY 
                (   
                    ID_CATEGORY SERIAL PRIMARY KEY,
                    NAME_CATEGORY VARCHAR(255)  UNIQUE
                );

                CREATE TABLE IF NOT EXISTS TB_DIM_MARK
                (
                    ID_MARK SERIAL PRIMARY KEY,
                    NAME_MARK VARCHAR(255) UNIQUE
                );

                CREATE TABLE IF NOT EXISTS TB_FACT_MEMORY 
                (
                    ID_MEMORY SERIAL PRIMARY KEY,
                    MEMORY_NAME VARCHAR(200),
                    MHZ BIGINT,
                    DDR BIGINT,
                    GB BIGINT,
                    LATENCY BIGINT,
                    VALUE DECIMAL(10,2),
                    URI VARCHAR(255),
                    DATACOLECT VARCHAR(255),
                    ID_CATEGORY_FK BIGINT,
                    ID_SITE_FK BIGINT,
                    ID_MARK_FK BIGINT,

                    FOREIGN KEY (ID_MARK_FK) REFERENCES TB_DIM_MARK(ID_MARK) ON DELETE CASCADE, 
                    FOREIGN KEY (ID_SITE_FK) REFERENCES TB_DIM_SITE(ID_SITE) ON DELETE CASCADE,
                    FOREIGN KEY (ID_CATEGORY_FK) REFERENCES TB_DIM_CATEGORY(ID_CATEGORY) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS TB_FACT_PROCESSOR
                (
                    ID_PROCESSOR SERIAL PRIMARY KEY,
                    MODEL VARCHAR(255),
                    FREQUENCY_BASE FLOAT,
                    CACHE BIGINT,
                    VALUE DECIMAL(10,2),
                    URI VARCHAR(255),
                    DATACOLECT VARCHAR(255),
                    ID_CATEGORY_FK BIGINT,
                    ID_SITE_FK BIGINT,
                    ID_MARK_FK BIGINT,

                    FOREIGN KEY (ID_MARK_FK) REFERENCES TB_DIM_MARK(ID_MARK) ON DELETE CASCADE, 
                    FOREIGN KEY (ID_SITE_FK) REFERENCES TB_DIM_SITE(ID_SITE) ON DELETE CASCADE,
                    FOREIGN KEY (ID_CATEGORY_FK) REFERENCES TB_DIM_CATEGORY(ID_CATEGORY) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS TB_FACT_VIDEO_CARD
                (
                    ID_VIDEO_CARD SERIAL PRIMARY KEY,
                    MODEL VARCHAR(255),
                    GB INT,
                    MEMORY VARCHAR(255),
                    VALUE DECIMAL(10,2),
                    URI VARCHAR(255),
                    DATACOLECT VARCHAR(255),
                    ID_CATEGORY_FK BIGINT,
                    ID_SITE_FK BIGINT,
                    ID_MARK_FK BIGINT,

                    FOREIGN KEY (ID_MARK_FK) REFERENCES TB_DIM_MARK(ID_MARK) ON DELETE CASCADE, 
                    FOREIGN KEY (ID_SITE_FK) REFERENCES TB_DIM_SITE(ID_SITE) ON DELETE CASCADE,
                    FOREIGN KEY (ID_CATEGORY_FK) REFERENCES TB_DIM_CATEGORY(ID_CATEGORY) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS TB_FACT_MOTHERBOARD
                (
                    ID_MOTHERBOARD SERIAL PRIMARY KEY,
                    SOCKET VARCHAR(255),
                    DDR BIGINT,
                    MODEL VARCHAR(255),
                    VALUE DECIMAL(10,2),
                    URI VARCHAR(255),
                    DATACOLECT VARCHAR(255),
                    ID_CATEGORY_FK BIGINT,
                    ID_SITE_FK BIGINT,
                    ID_MARK_FK BIGINT,

                   FOREIGN KEY (ID_MARK_FK) REFERENCES TB_DIM_MARK(ID_MARK) ON DELETE CASCADE, 
                    FOREIGN KEY (ID_SITE_FK) REFERENCES TB_DIM_SITE(ID_SITE) ON DELETE CASCADE,
                    FOREIGN KEY (ID_CATEGORY_FK) REFERENCES TB_DIM_CATEGORY(ID_CATEGORY) ON DELETE CASCADE
                    
                );""")

        INSERT_TABLES = SparkSubmitOperator(
        task_id='insert',
        application='/opt/bitnami/spark/src/scripts_spark/LoadFileToDB.py',
        conn_id='spark_default',  
        packages='org.apache.hadoop:hadoop-aws:3.3.1,org.postgresql:postgresql:42.5.0',  
        driver_class_path='/opt/airflow/jars/postgresql-42.5.0.jar')


CREATE_TABLES>>INSERT_TABLES