import pandas as pd
import re
import unicodedata
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.window import Window


def colunas_minusculas(df):
    return df.toDF(*[c.strip().lower() for c in df.columns])

def colunas_em_snake_case(df):
    def normalizar(nome):
        nome = unicodedata.normalize("NFKD", str(nome))
        nome = "".join(c for c in nome if not unicodedata.combining(c))
        nome = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", nome)
        nome = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", nome)
        nome = re.sub(r"[^a-zA-Z0-9]+", "_", nome)
        return nome.strip("_").lower()
    mapeamento = {coluna: normalizar(coluna) for coluna in df.columns}
    nomes = list(mapeamento.values())
    duplicados = {nome for nome in nomes if nomes.count(nome) > 1}
    if duplicados:
        detalhes = {
            nome: [original for original, normalizado in mapeamento.items() if normalizado == nome]
            for nome in sorted(duplicados)
        }
        mensagem = "\n".join(
            f"- {nome}: {originais}"
            for nome, originais in detalhes.items()
        )
        raise ValueError(
            f"A conversÃ£o para snake_case gerou nomes duplicados:\n{mensagem}"
        )
    return df.toDF(*nomes)

def normalizar_texto(texto):
    if pd.isna(texto):
        return ""
    texto = str(texto).strip().lower()
    texto = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in texto if not unicodedata.combining(c))

def renomear_cols_zews(df):
    cols = [
        F.col(c).alias(
            c.lower()
            if c.upper() in ["APOLICE", "DT_COMP"]
            else f"{c.lower()}_zews"
        )
        for c in df.columns
    ]
    return df.select(*cols)

def normalizar_dt_comp(df, coluna_data):
    col_dt = next(
        (c for c in df.columns if c.strip().lower() == coluna_data.strip().lower()),
        None
    )
    if col_dt is None:
        raise ValueError(
            f"Coluna '{coluna_data}' nÃ£o encontrada. "
            f"Colunas disponÃ­veis: {df.columns}"
        )
    valor = F.trim(F.col(col_dt).cast("string"))
    data = (
        F.when(
            valor.rlike(r"^[0-9]{6}$"),
            F.to_date(F.concat(valor, F.lit("01")), "yyyyMMdd")
        )
        .when(
            valor.rlike(r"^[0-9]{8}$"),
            F.to_date(valor, "yyyyMMdd")
        )
        .otherwise(F.to_date(F.col(col_dt)))
    )
    return df.withColumn(col_dt, F.trunc(data, "month"))

def converter_colunas_decimal(df, colunas, precisao=18, escala=10):
    tipo_decimal = f"decimal({precisao},{escala})"

    for coluna in colunas:
        valor = F.trim(F.col(coluna).cast("string"))

        df = df.withColumn(
            coluna,
            F.when(
                valor.isNull() | (valor == ""),
                F.lit(None).cast(tipo_decimal)
            ).otherwise(
                F.regexp_replace(valor, ",", ".").cast(tipo_decimal)
            )
        )

    return df

def converter_para_spark(df, spark):
    return spark.createDataFrame(df)

def valores_em_snake_case(df, colunas=None):    
    if colunas is None:
        colunas = [
            campo.name
            for campo in df.schema.fields
            if campo.dataType.simpleString() == "string"
        ]
    for coluna in colunas:
        df = df.withColumn(
            coluna,
            F.lower(
                F.regexp_replace(
                    F.trim(F.col(coluna)),
                    r"[^a-zA-Z0-9]+",
                    "_"
                )
            )
        )
    return df