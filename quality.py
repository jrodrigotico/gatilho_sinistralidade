import pandas as pd
from pyspark.sql import functions as F


def checar_duplicidade(df, nome, chaves):
    df_dup = (
        df.groupBy(*chaves)
        .count()
        .filter(F.col("count") > 1)
    )
    qtd = df_dup.count()
    if qtd > 0:
        print(f"âš ï¸ [CARACTERE ILEGÍVEL] {nome}: {qtd} chaves duplicadas")
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
        print(f"âœ… {nome}: sem duplicidades")

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