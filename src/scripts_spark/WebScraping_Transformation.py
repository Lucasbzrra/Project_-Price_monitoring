from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.types import DoubleType, LongType, DecimalType
from pyspark.sql import Column, Row
import sys
import re



def set_none_if_empty_is_string(col:Column)->Column:
    return f.when(f.col(col)=='',f.lit('NOT_INFO')).otherwise(f.col(col))

def set_none_if_empty_is_integer(col:Column)->Column:
        return f.when(f.col(col)=='',f.lit(0)).otherwise(f.col(col))

def SaveFile(df_placa_mae_fn, df_filtred_memoria_fn,df_processadores_fn ,df_placa_video_fn,df_dim_site,df_marca,df_dim_categoria,json_file_path):

    data = re.search(r"\d{4}-\d{2}-\d{2}", json_file_path)
    print(json_file_path)
    print(data)

    if 'kabum_' in json_file_path:

        df_placa_mae_fn.write.parquet(f's3a://silverzone/kabum/kabum_{data.group()}/placa_mae.parquet')
        df_filtred_memoria_fn.write.parquet(f's3a://silverzone/kabum/kabum_{data.group()}/memoria_k.parquet')
        df_processadores_fn.write.parquet(f's3a://silverzone/kabum/kabum_{data.group()}/processador_k.parquet')
        df_placa_video_fn.write.parquet(f's3a://silverzone/kabum/kabum_{data.group()}/placa_video_k.parquet')
        df_dim_site.write.parquet(f's3a://silverzone/kabum/kabum_{data.group()}/dim_site.parquet')
        df_marca.write.parquet(f's3a://silverzone/kabum/kabum_{data.group()}/dim_marca.parquet')
        df_dim_categoria.write.parquet(f's3a://silverzone/kabum/kabum_{data.group()}/dim_categoria.parquet')

    if 'pichau_' in json_file_path:

        df_placa_mae_fn.write.parquet(f's3a://silverzone/pichau/pichau_{data.group()}/placa_mae.parquet')
        df_filtred_memoria_fn.write.parquet(f's3a://silverzone/pichau/pichau_{data.group()}/memoria_k.parquet')
        df_processadores_fn.write.parquet(f's3a://silverzone/pichau/pichau_{data.group()}/processador_k.parquet')
        df_placa_video_fn.write.parquet(f's3a://silverzone/pichau/pichau_{data.group()}/placa_video_k.parquet')
        df_dim_site.write.parquet(f's3a://silverzone/pichau/pichau_{data.group()}/dim_site.parquet')
        df_marca.write.parquet(f's3a://silverzone/pichau/pichau_{data.group()}/dim_marca.parquet')
        df_dim_categoria.write.parquet(f's3a://silverzone/pichau/pichau_{data.group()}/dim_categoria.parquet')

   


def transformation_data(spark,json_file_path):
        #leitura do arquivo

        if 'kabum_' in json_file_path:

            file_path =  's3a://rawzone/kabum/'+json_file_path

            df=spark.read.json(file_path)
            
            df = df.withColumn("value", f.trim(f.regexp_replace(f.col("value"), "[^0-9.]", "")).cast(DecimalType(12, 2)))
            df=df.withColumn('data_collect',f.current_date())

        if 'pichau_' in json_file_path:

            file_path =  's3a://rawzone/pichau/'+json_file_path
            df = spark.read.json(file_path)

            df=df.withColumn('value',df['value'].cast(DecimalType(12,2)))
            df=df.withColumn('data_collect',f.current_date())


        df=df.withColumn('name',f.upper(df['name']))

        #Filtrando por categoria
        df_placa_mae_filtred=df.filter(df['category']=='placas-mae')

        df_placa_mae = df_placa_mae_filtred.withColumn('modelo', f.regexp_extract('name', r'(H\d{2,3}|B\d{2,3}|Z\d{2,3}|TRX\d{2,3}|X\d{2,3}|A\d{2,3}|G\d{2,3}|C\d{2,3}|G41|H55|B85)', 1))\
        .withColumn('marca',f.regexp_extract('name',r'(ASUS|MSI|GIGABYTE|ASROCK|AORUS|DUEX|AFOX|BIOSTAR|PCWARE|BLUECASE|BRAZIL\s?PC|ALLIGATOR\s?SHOP|GOLINE|GOLDENTEC|LENOVO|BRX|OXY|REVENGER|ART\s?TECH)', 1))\
        .withColumn('name_mark',set_none_if_empty_is_string('marca'))\
        .withColumn('model',set_none_if_empty_is_string('modelo')) \
        .withColumn('socket', f.regexp_extract('name', r'(LGA\s?\d{3,4}|AM\d\+?)', 1)) \
        .withColumn('socket',set_none_if_empty_is_string('socket'))\
        .withColumn('DDR',f.regexp_replace(f.regexp_extract('name', r'(DDR\d)', 1),'DDR','')) \
        .withColumn('DDR',set_none_if_empty_is_integer('DDR').cast(LongType()))\
        .withColumnRenamed('link','uri')
        

       
        df_placa_mae_fn=df_placa_mae.select(['name_mark','model','socket','DDR','value','uri','data_collect','category'])

        df_placa_mae_fn.limit(15).show()

        #Filtrando por categoria
        df_filtred_memoria=df.filter(df['category']=='memoria-ram')

        df_filtred_memoria = df_filtred_memoria.withColumn('GB', f.regexp_replace(f.regexp_extract('name', r'(\d+GB)', 1),'GB','').cast(LongType())) \
        .withColumn('MHZ', f.regexp_replace(f.regexp_extract('name', r'(\d+MHZ)', 1),'MHZ','')) \
       .withColumn('MHZ',set_none_if_empty_is_integer('MHZ').cast(LongType()))\
       .withColumn('DDR',f.regexp_replace(f.regexp_extract('name', r'(DDR\d)', 1),'DDR','')) \
       .withColumn('DDR',set_none_if_empty_is_integer('DDR').cast(LongType()))\
       .withColumn('latencia',f.regexp_replace(f.regexp_extract('name', r'(CL\d+)', 1),'CL','')) \
       .withColumn('latency',set_none_if_empty_is_integer('latencia').cast(LongType()))\
       .withColumn('marca',f.regexp_extract('name', r'(KINGSTON|XPG|HUSKY|RISE MODE|KABUM!|CORSAIR|CRUCIAL|ADATA|LEXAR|OXY|NEOLOGIC|NTC|PEG|BRAZILPC|IOWAY|SEGOTEP|WIN MEMORY|KEEPDATA|HYPERX|PATRIOT|BEST MEMORY|KTROK|PROSMART|DALE7|PRIMETEK|REDRAGON|HIKVISION|EASY MEMORY|SAMSUNG|DELL)\s?(FURY|GAMMIX|AVALANCHE|VENGEANCE|GAMING|BASICS|RGB|VIPER|VIPER ELITE II)?', 1))\
       .withColumn('name_mark',set_none_if_empty_is_string('marca'))\
       .withColumnRenamed('link','uri')\
       .withColumnRenamed('name','memory_name')
       

  

         #selecionando as colunas
        df_filtred_memoria_fn=df_filtred_memoria.select(['memory_name','name_mark','MHZ','GB','DDR','latency','value','uri','data_collect','category'])

        df_filtred_memoria_fn.limit(15).show()

        #Filtrando por categoria
        df_processadores=df.filter(df['category']=='processadores')

        df_processadores = df_processadores.withColumn('marca', f.regexp_extract('name', r'^(PROCESSADOR\s+[\w\s\(\)\/\.\-]+)', 1)) \
                                   .withColumn('name_mark', set_none_if_empty_is_string('marca')) \
                                   .withColumn('modelo', f.regexp_extract('name', r'(CORE I\d|RYZEN \d)', 1)) \
                                   .withColumn('model', set_none_if_empty_is_string('modelo')) \
                                   .withColumn('frequencia_base', f.regexp_replace(f.regexp_extract('name', r'(\d+\.\d+GHZ)', 1), 'GHZ', '')) \
                                   .withColumn('frequency_base', set_none_if_empty_is_integer('frequencia_base').cast(DoubleType())) \
                                   .withColumn('frequencia_turbo', f.regexp_extract('name', r'\((\d+\.\d+GHZ) MAX TURBO\)', 1)) \
                                   .withColumn('cache', f.regexp_replace(f.regexp_extract('name', r'(\d+MB)', 1), 'MB', '')) \
                                   .withColumn('cache', set_none_if_empty_is_integer('cache').cast(LongType())) \
                                   .withColumn('geracao', f.regexp_extract('name', r'(\d+ª GERAÇÃO)', 1)) \
                                   .withColumn('socket', f.regexp_extract('name', r'(LGA\d+|AM\d+)', 1))\
                                   .withColumnRenamed('link','uri')
        #selecionando as colunas
        df_processadores_fn=df_processadores.select(['name_mark','model','frequency_base','cache','value','uri','data_collect','category'])

        df_processadores_fn.limit(15).show()

        #Filtrando por categoria
        df_placa_video=df.filter(df['category']=='placa-de-video-vga')

        df_placa_video = df_placa_video.withColumn('marca', f.regexp_extract('name', r'(AITEK|AMD|AFOX|ASROCK|PNY|NVIDIA|BRX|SAPPHIRE|REVENGER|PCYES)\s?(GT|RX|GTX|RTX|Quadro|Intel ARC|TUF|ROG)?\s?(\d{3,4}(?:XT)?|SUPER|OC|EVO|GAMING)?\s?(\d{1,2}GB|GDDR\d|DDR\d|X|XTX)?', 1)) \
                               .withColumn('name_mark',set_none_if_empty_is_string('marca'))\
                               .withColumn('modelo', f.regexp_extract('name', r'((?:GT|RX|GTX|RTX|Quadro|Intel ARC|TUF|ROG)\s?\d{3,4}(?:XT|SUPER|OC|EVO|GAMING)?)', 1)) \
                               .withColumn('model',set_none_if_empty_is_string('modelo'))\
                               .withColumn('GB', f.regexp_replace(f.regexp_extract('name', r'(\d+GB)', 1),'GB','')) \
                               .withColumn('GB',set_none_if_empty_is_integer('GB').cast(LongType()))\
                               .withColumn('tipo_memoria', f.regexp_extract('name', r'(GDDR6X|GDDR6|GDD5|GDDR 6| DDR5| DDR3)', 1)) \
                               .withColumn('memory',set_none_if_empty_is_string('tipo_memoria'))\
                               .withColumn('barramento', f.regexp_extract('name', r'(\d+BITS)', 1))\
                               .withColumnRenamed('link','uri')
        
        

        #selecionando as colunas
        df_placa_video_fn=df_placa_video.select(['name_mark','model','GB','memory','value','uri','data_collect','category'])

        df_placa_video_fn.limit(15).show()

        # dim site
        data = [Row(site="kabum",id_site=1), Row(site="pichau",id_site=2)]
        df_dim_site = spark.createDataFrame(data)

        # dim marca
        placa_marca=df_placa_video.select('name_mark').dropDuplicates()
        processador_marca=df_processadores.select('name_mark').dropDuplicates()
        placamae_marca=df_placa_mae_fn.select('name_mark').dropDuplicates()
        memoria_marca=df_filtred_memoria_fn.select('name_mark').dropDuplicates()

        df_marca=placa_marca.unionAll(processador_marca)
        df_marca=df_marca.unionAll(placamae_marca)
        df_marca=df_marca.unionAll(memoria_marca)
        df_marca=df_marca.dropDuplicates()

        

        # dim categoria
        df_dim_categoria=df.select('category').dropDuplicates()

        

        SaveFile(df_placa_mae_fn, df_filtred_memoria_fn,df_processadores_fn ,df_placa_video_fn,df_dim_site,df_marca,df_dim_categoria,json_file_path)


import argparse
from pyspark.sql import SparkSession

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='Process some JSON Data')
    parser.add_argument('--json_file_path', type=str, required=True)
    args = parser.parse_args()

    json_file_path = args.json_file_path

    spark = (
        SparkSession.builder
        .master("local[*]") 
        .getOrCreate()
    )
    
    sc = spark.sparkContext

    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AVcyU1dIcrfI88EqIa7p")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "Wf2SYhz7K7LmsjSgFVW1Bbdtclakn98ujl66AcO4")
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    transformation_data(spark,json_file_path)

    spark.stop()
