import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
from pyspark.sql import SparkSession
from pipeline import executar_formatos_fatorial, executar_rls, executar_exposto_percentuais

spark = (SparkSession.builder.master("local[2]").appName("GatilhoSinistralidade").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ------ execucao
df_formatos = executar_formatos_fatorial(spark)
df_rls = executar_rls(df_formatos)
df_exposto_unificado = executar_exposto_percentuais(spark)