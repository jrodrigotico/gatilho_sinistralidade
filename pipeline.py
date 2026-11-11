import pandas as pd
from config import arquivos
from pyspark.sql import functions as F
from utils import converter_colunas_decimal, colunas_em_snake_case, converter_para_spark, valores_em_snake_case
from transformation import unir_formatos_fatorial, preparar_exposto_percentuais
from quality import checar_duplicidade


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

    return df_formatos

def executar_fatorial(spark):
    df_fatorial = pd.read_excel(
        arquivos,
        sheet_name="fatores_ibnr",
        dtype={"CHAVE": str},
    )

    df_fatorial = converter_para_spark(df_fatorial, spark)
    df_fatorial = colunas_em_snake_case(df_fatorial)

    return df_fatorial

def executar_formatos_fatorial(spark):
    df_formatos = executar_formatos(spark)
    df_fatorial = executar_fatorial(spark)
    df_resultado = unir_formatos_fatorial(df_formatos, df_fatorial)

    checar_duplicidade(df_resultado, "duplicidade da coluna 'chave'em formatos", ["chave"])
    return df_resultado

def executar_rls(df):
    df_resultado = df.select("chave", "acesso").filter(
            F.col("chave").isNull()
            | (F.col("chave") != "000000000000")
        )

    checar_duplicidade(df_resultado, "duplicidade da coluna 'chave' no RLS", ["chave"])
    return df_resultado


def executar_exposto_percentuais(spark):
    print("Iniciando pipeline de percentuais do exposto...")

    df_ge = pd.read_excel(
        arquivos,
        sheet_name="src_exposto_ge",
        dtype={"APOLICE": str, "CHAVE": str},
    )

    df_rfd = pd.read_excel(
        arquivos,
        sheet_name="src_exposto_rfd",
        dtype={"APOLICE": str, "CHAVE": str},
    )

    df_others = pd.read_excel(
        arquivos,
        sheet_name="src_exposto_others",
        dtype={"APOLICE": str, "CHAVE": str},
    )

    df_ge = colunas_em_snake_case(converter_para_spark(df_ge, spark))
    df_rfd = colunas_em_snake_case(converter_para_spark(df_rfd, spark))
    df_others = colunas_em_snake_case(converter_para_spark(df_others, spark))

    df_unificado = (
        df_ge
        .unionByName(df_rfd)
        .unionByName(df_others)
    )
    checar_duplicidade(df_unificado, "duplicidade da coluna 'chave' e 'dt_comp' no exposto final", ["chave", "dt_comp"])

    df_unificado = preparar_exposto_percentuais(df_unificado)

    return df_unificado
