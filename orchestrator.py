import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pipeline import executar_formatos_fatorial

spark = (SparkSession.builder.master("local[2]").appName("GatilhoSinistralidade").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df_formatos = executar_formatos_fatorial(spark)


