import pandas as pd
from config import arquivos
from pyspark.sql import functions as F
from utils import converter_colunas_decimal, colunas_em_snake_case, converter_para_spark, valores_em_snake_case
from transformation import unir_formatos_fatorial


def executar_formatos(spark):
    print("Iniciando pipeline de formatos...")
    df_formatos = pd.read_excel(
        arquivos,
        sheet_name="formatos_recarga",
        dtype={"CHAVE": str, "START": str, "RAMO": str},
    )

    df_formatos = converter_para_spark(df_formatos, spark)
    df_formatos = converter_colunas_decimal(
        df_formatos,
        colunas=["PERCENT_PREMIO_IBNR", "PERCENT_SINISTRO_IBNR", "PERCENT_ULAE", "SINISTRALIDADE_PLANO"],
        precisao=18,
        escala=10
    )

    df_formatos = colunas_em_snake_case(df_formatos)
    df_formatos = valores_em_snake_case(df_formatos, ['tipo_apuracao_nep', 'tipo_regra_ibnr','tipo_regra_ulae'])
    df_formatos = df_formatos.filter(F.col("mostrar_gatilho") == "SIM") 
    df_formatos.show(10, truncate=False)
    return df_formatos

def executar_fatorial(spark):
    df_fatorial = pd.read_excel(
        arquivos,
        sheet_name="formatos_recarga",
        dtype={"CHAVE": str},
    )

    df_fatorial = converter_para_spark(df_fatorial, spark)
    df_fatorial = colunas_em_snake_case(df_fatorial)
    df_fatorial.show(10, truncate=False)

    return df_fatorial

def executar_formatos_fatorial(spark):
    df_formatos = executar_formatos(spark)
    df_fatorial = executar_fatorial(spark)
    df_resultado = unir_formatos_fatorial(df_formatos, df_fatorial)
    df_resultado.show(10, truncate=False)
    return df_resultado