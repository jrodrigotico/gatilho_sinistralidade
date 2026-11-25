from pyspark.sql import functions as F
from .regra_parceiro import RegraParceiro


class Sky(RegraParceiro):
    def __init__(self):
        super().__init__(nome_parceiro="SKY")

    def classificar_periodo(self, df, coluna_data, df_formatos):
        auxiliares = {"_eh_sky", "_data_sky"}

        if auxiliares.intersection(df.columns):
            raise ValueError(
                "A base já contém colunas auxiliares do tratamento SKY."
            )

        # Uma linha por chave para não multiplicar os registros da base.
        chaves_sky = (
            df_formatos
            .filter(
                F.upper(F.trim(F.col("grupo_geral"))) == "SKY"
            )
            .select(
                F.trim(F.col("chave").cast("string")).alias("chave")
            )
            .filter(
                F.col("chave").isNotNull()
                & (F.col("chave") != "")
            )
            .distinct()
            .withColumn("_eh_sky", F.lit(True))
        )

        base = (
            df
            .withColumn(
                "chave",
                F.trim(F.col("chave").cast("string"))
            )
            .join(chaves_sky, on="chave", how="left")
            .withColumn(
                "_data_sky",
                F.to_date(F.col(coluna_data))
            )
        )

        # Não permite classificar SKY sem uma data válida.
        if (
            base
            .filter(
                F.col("_eh_sky").isNotNull()
                & F.col("_data_sky").isNull()
            )
            .limit(1)
            .count() > 0
        ):
            raise ValueError(
                f"Há registros SKY com data nula ou inválida em {coluna_data}."
            )

        return (
            base
            .withColumn(
                "periodo_sky",
                F.when(
                    F.col("_eh_sky").isNotNull(),
                    F.when(
                        F.col("_data_sky") < F.lit("2023-06-01").cast("date"),
                        F.lit("SKY")
                    ).otherwise(
                        F.lit("SKY - Renovação 2023")
                    )
                ).otherwise(
                    F.lit(None).cast("string")
                )
            )
            .drop("_eh_sky", "_data_sky")
        )