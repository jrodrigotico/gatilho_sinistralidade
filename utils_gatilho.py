import pandas as pd
import unicodedata
import re
from pyspark.sql import functions as F


def publicar_delta(df, tabela):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabela)
    )


def colunas_minusculas(df):
    return df.toDF(*[c.strip().lower() for c in df.columns])

def conferir_carteira(nome_etapa, df):
    totais = df.agg(
        F.count("*").alias("linhas"),
        F.sum("gwp").alias("gwp"),
        F.sum("nep").alias("nep"),
        F.sum("sinistro").alias("sinistro")
    ).first()

    print(
        f"{nome_etapa} | "
        f"linhas={totais['linhas']} | "
        f"gwp={totais['gwp']} | "
        f"nep={totais['nep']} | "
        f"sinistro={totais['sinistro']}"
    )

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
            f"Coluna '{coluna_data}' não encontrada. "
            f"Colunas disponíveis: {df.columns}"
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

def checar_duplicidade(df, nome, chaves):

    df_dup = (
        df.groupBy(*chaves)
        .count()
        .filter(F.col("count") > 1)
    )

    qtd = df_dup.count()

    if qtd > 0:
        print(f"⚠️ {nome}: {qtd} chaves duplicadas")

        (
            df.join(
                df_dup.select(*chaves),
                on=chaves,
                how="inner"
            )
            .orderBy(*chaves)
            .show(200, truncate=False)
        )

    else:
        print(f"{nome}: sem duplicidade de chaves")

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
            f"A conversão para snake_case gerou nomes duplicados:\n{mensagem}"
        )

    return df.toDF(*nomes)

