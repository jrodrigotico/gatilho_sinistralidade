from pyspark.sql import functions as F
from .regra_parceiro import RegraParceiro


class Havan(RegraParceiro):
    def __init__(self):
        super().__init__(nome_parceiro="HAVAN")

    def ajustar_chave_renovacao(
        self,
        df,
        coluna_data,
        coluna_chave="chave",
        data_corte="2025-01-01",
    ):
        CHAVES_HAVAN_ORIGINAIS = [
            "017791863361",
            "017791864031",
            "017791865301",
            "019091861241",
            "019091861261",
            "019091861271",
        ]

        chave = F.trim(
            F.col(coluna_chave).cast("string")
        )

        data_referencia = F.to_date(
            F.col(coluna_data)
        )

        ramo = F.substring(
            chave,
            3,
            2,
        )

        eh_havan_original = (
            chave.isin(CHAVES_HAVAN_ORIGINAIS)
            & ramo.isin("77", "90")
            & chave.endswith("1")
        )

        depois_do_corte = (
            data_referencia.isNotNull()
            & (
                data_referencia
                >= F.lit(data_corte).cast("date")
            )
        )

        return df.withColumn(
            coluna_chave,
            F.when(
                eh_havan_original & depois_do_corte,
                F.concat(
                    F.substring(chave, 1, 11),
                    F.lit("2"),
                ),
            ).otherwise(chave),
        )