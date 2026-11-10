from pipeline import executar_formatos_fatorial
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("GatilhoSinistralidade").getOrCreate()


df_formatos = executar_formatos_fatorial(spark)


