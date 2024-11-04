from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import datetime
import re
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


spark = SparkSession.builder \
    .appName("Insert_data") \
    .master("local") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "AVcyU1dIcrfI88EqIa7p") \
    .config("spark.hadoop.fs.s3a.secret.key", "Wf2SYhz7K7LmsjSgFVW1Bbdtclakn98ujl66AcO4") \
    .config("fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

password = 'airflow'
url = "jdbc:postgresql://host.docker.internal:5432/db_webscraping?ssl=false"
db_properties={
    'user':'airflow',
    'password':password,
    'driver':'org.postgresql.Driver'
}

datefull = datetime.datetime.now()

date = datefull.strftime("%Y-%m-%d")

listNameFiles = ['processador_k','memoria_k','placa_mae','placa_video_k']


for namefile in listNameFiles:

    
    local_path=f's3a://silverzone/*/*_{date}/{namefile}.parquet'
    print(local_path)

    if namefile =='memoria_k':


        df_memoria = spark.read.parquet(local_path)


        df_memoria=df_memoria.withColumn('ID_CATEGORY_FK',f.lit(1))\
                         .withColumn('ID_SITE_FK', f.when(df_memoria['uri'].contains('kabum'), 1).otherwise(2))\
                         .withColumnRenamed('data_collect','datacolect')
       
        table_mark_m = spark.read.jdbc(url=url,table='tb_dim_mark',properties=db_properties)

        table_memoria_mark=table_mark_m.join(df_memoria,'name_mark')

        table_memoria_mark=table_memoria_mark.withColumnRenamed('id_mark','id_mark_fk')

        table_memoria_mark=table_memoria_mark.drop('id_memoria_ram','name_mark','category')

        table_memoria_mark.show()

        table_memoria_mark.write.jdbc(url=url,table='tb_fact_memory',mode='append',properties=db_properties)

    if namefile =='processador_k':


        df_processador = spark.read.parquet(local_path)



        df_processador=df_processador.withColumn('ID_CATEGORY_FK',f.lit(2))\
                       .withColumn('ID_SITE_FK', f.when(df_processador['uri'].contains('kabum'), 1).otherwise(2))\
                       .withColumnRenamed('data_collect','datacolect')

                        


        table_mark_p = spark.read.jdbc(url=url,table='tb_dim_mark',properties=db_properties)

        table_processador_mark = table_mark_p.join(df_processador,'name_mark')

        table_processador_mark=table_processador_mark.withColumnRenamed('id_mark','id_mark_fk')

        table_processador_mark=table_processador_mark.drop('name_mark','id_processador','category')

        table_processador_mark.show()

        table_processador_mark.write.jdbc(url=url,table='tb_fact_processor',mode='append',properties=db_properties)

    if namefile == 'placa_video_k':
    

        df_placa_video = spark.read.parquet(local_path)


        df_placa_video=df_placa_video.withColumn('ID_CATEGORY_FK',f.lit(3))\
                     .withColumn('ID_SITE_FK', f.when(df_placa_video['uri'].contains('kabum'), 1).otherwise(2))\
                     .withColumnRenamed('data_collect','datacolect')


        table_mark = spark.read.jdbc(url=url,table='tb_dim_mark',properties=db_properties)

        table_joins = table_mark.join(df_placa_video,'name_mark')

        table_joins=table_joins.withColumnRenamed('id_mark','id_mark_fk')
        table_joins=table_joins.drop('name_mark','id_placa_video','category')

        table_joins.show()

        table_joins.write.jdbc(url=url,table='tb_fact_video_card',mode='append',properties=db_properties)
    
    if namefile == 'placa_mae':

        df_placa_mae = spark.read.parquet(local_path) 

        df_placa_mae=df_placa_mae.withColumn('ID_CATEGORY_FK',f.lit(4))\
                                .withColumn('ID_SITE_FK', f.when(df_placa_mae['uri'].contains('kabum'), 1).otherwise(2))\
                                .withColumnRenamed('data_collect','datacolect')


        table_mark_pl = spark.read.jdbc(url=url,table='tb_dim_mark',properties=db_properties)

        table_pl_mark = table_mark_pl.join(df_placa_mae,'name_mark')

        table_pl_mark=table_pl_mark.withColumnRenamed('id_mark','id_mark_fk')
        table_pl_mark=table_pl_mark.drop('name_mark','id_placa_mae','name','category')

        table_pl_mark.show()

        table_pl_mark.write.jdbc(url=url,table='tb_fact_motherboard',mode='append',properties=db_properties)
