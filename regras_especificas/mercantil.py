from pyspark.sql import functions as F
from .regra_parceiro import RegraParceiro


class Mercantil(RegraParceiro):
    def __init__(self):
        super().__init__(nome_parceiro="MERCANTIL")

    def aplicar_safras(self, df_exposto, df_emissao_final, df_premio_ganho):
        # ============================================================
        # MERCANTIL: CRIA SAFRA 01 NA CHAVE V2
        # ============================================================
        chave_mercantil_input = "667791861252"     # Versao 2 = Safra 01
        chave_mercantil_carteira = "667791861251"  # Versao 1 = Safra 02
        apolice_mercantil = "66779186125"


        # Cria as linhas da Safra 01 diretamente a partir dos inputs.
        # Usa o schema do exposto para manter unionByName consistente.
        df_mercantil_safra_01 = (
            df_premio_ganho
            .select(
                F.col("chave_join"),
                F.col("ano_mes_join"),
                F.col("premio_ganho_mercantil")
            )
            .withColumn(
                "chave",
                F.lit(chave_mercantil_input)
            )
            .withColumn(
                "dt_nep",
                F.to_date(
                    F.concat(
                        F.col("ano_mes_join").cast("string"),
                        F.lit("01")
                    ),
                    "yyyyMMdd"
                )
            )
            .withColumn(
                "apolice",
                F.lit(apolice_mercantil)
            )
        )

        # Adiciona ao DataFrame todas as colunas que existem no exposto.
        # NEP recebe exclusivamente o input manual.
        # Demais métricas financeiras ficam nulas para não copiar
        # IBNR, comissão, assistência, QBE etc. da Safra 02.
        expressoes_safra_01 = []

        for campo in df_exposto.schema.fields:
            nome = campo.name

            if nome == "chave":
                expr = F.lit(chave_mercantil_input)

            elif nome == "chave_join":
                expr = F.lit(chave_mercantil_input)

            elif nome == "ano_mes_join":
                expr = F.col("ano_mes_join")

            elif nome == "dt_nep":
                expr = F.col("dt_nep")

            elif nome == "apolice":
                expr = F.lit(apolice_mercantil)

            elif nome == "nep":
                expr = F.col("premio_ganho_mercantil").cast(
                    campo.dataType
                )

            else:
                expr = F.lit(None).cast(campo.dataType)

            expressoes_safra_01.append(
                expr.alias(nome)
            )

        df_mercantil_safra_01 = (
            df_mercantil_safra_01
            .select(*expressoes_safra_01)
        )

        # Segurança: não pode já existir chave V2 nesses meses.
        mercantil_v2_existente = (
            df_exposto
            .filter(
                (F.col("chave") == chave_mercantil_input)
                & F.col("ano_mes_join").between(
                    202406,
                    202508
                )
            )
        )

        if mercantil_v2_existente.limit(1).count() > 0:
            raise ValueError(
                "Ja existem registros da chave Mercantil Safra 01 "
                "no exposto. O input manual nao sera duplicado."
            )

        # Acrescenta a Safra 01 ao exposto.
        df_exposto = (
            df_exposto
            .unionByName(
                df_mercantil_safra_01
            )
        )

        # Validação final: precisam existir exatamente os 15 meses.
        qtd_meses_safra_01 = (
            df_exposto
            .filter(
                F.col("chave") == chave_mercantil_input
            )
            .select("ano_mes_join")
            .distinct()
            .count()
        )

        if qtd_meses_safra_01 != 15:
            raise ValueError(
                "Mercantil Safra 01 deveria possuir 15 meses, "
                f"mas foram encontrados {qtd_meses_safra_01}."
            )

        # ============================================================
        # MERCANTIL: AJUSTA NEP DA SAFRA 02
        # ============================================================
        # Traz o valor manual da Safra 01 por mes, mas associa
        # temporariamente esse valor a chave da Safra 02.
        df_mercantil_valor_safra_01 = (
            df_premio_ganho
            .select(
                F.col("ano_mes_join"),
                F.col("premio_ganho_mercantil").alias(
                    "_nep_safra_01"
                )
            )
        )

        df_exposto = (
            df_exposto
            .join(
                F.broadcast(df_mercantil_valor_safra_01),
                on="ano_mes_join",
                how="left"
            )
        )

        eh_safra_02_mercantil = (
            F.col("chave") == chave_mercantil_carteira
        )

        periodo_divisao_mercantil = (
            eh_safra_02_mercantil
            & F.col("ano_mes_join").between(
                202406,
                202508
            )
        )

        # Safra 02 = Carteira - Safra 01
        df_exposto = (
            df_exposto
            .withColumn(
                "nep",
                F.when(
                    periodo_divisao_mercantil,
                    F.col("nep") - F.col("_nep_safra_01")
                ).otherwise(
                    F.col("nep")
                )
            )
            .drop("_nep_safra_01")
        )

        # ============================================================
        # MERCANTIL: GWP IGUAL AO NEP DAS SAFRAS
        # ============================================================
        # Obtém o NEP final do Mercantil por chave e mês.
        # Safra 01: chave 667791861252, valor manual.
        # Safra 02: chave 667791861251, Carteira menos Safra 01
        # até 202508; Carteira integral a partir de 202509.
        df_gwp_mercantil = (
            df_exposto
            .filter(
                F.col("chave").isin(
                    chave_mercantil_input,
                    chave_mercantil_carteira
                )
                & (
                    F.col("ano_mes_join") >= 202406
                )
            )
            .select(
                F.trim(
                    F.col("chave").cast("string")
                ).alias("chave"),
                F.trunc(
                    F.to_date(F.col("dt_nep")),
                    "month"
                ).alias("dt_emissao"),
                F.col("nep").alias("_gwp_mercantil")
            )
        )

        # Deve existir apenas um valor por chave e mês no exposto.
        duplicados_gwp_mercantil = (
            df_gwp_mercantil
            .groupBy(
                "chave",
                "dt_emissao"
            )
            .count()
            .filter(
                F.col("count") > 1
            )
        )

        if duplicados_gwp_mercantil.limit(1).count() > 0:
            duplicados_gwp_mercantil.show(
                100,
                truncate=False
            )

            raise ValueError(
                "Mercantil possui mais de um NEP por chave/mes. "
                "Nao e possivel definir o GWP."
            )

        # Remove da emissão os registros originais do Mercantil
        # a partir de 202406. Eles serão reconstruídos com GWP = NEP.
        df_emissao_sem_mercantil = (
            df_emissao_final
            .withColumn(
                "chave",
                F.trim(
                    F.col("chave").cast("string")
                )
            )
            .withColumn(
                "dt_emissao",
                F.trunc(
                    F.to_date(F.col("dt_emissao")),
                    "month"
                )
            )
            .filter(
                ~(
                    F.col("chave").isin(
                        chave_mercantil_input,
                        chave_mercantil_carteira
                    )
                    & (
                        F.col("dt_emissao")
                        >= F.lit("2024-06-01").cast("date")
                    )
                )
            )
        )

        # Cria as linhas de emissão com o mesmo schema
        # de df_emissao_final.
        expressoes_emissao_mercantil = []

        for campo in df_emissao_final.schema.fields:
            nome = campo.name

            if nome == "chave":
                expressao = F.col("chave")

            elif nome == "dt_emissao":
                expressao = F.col("dt_emissao")

            elif nome == "gwp":
                expressao = F.col(
                    "_gwp_mercantil"
                ).cast(campo.dataType)

            elif nome == "apolice":
                expressao = F.lit(
                    apolice_mercantil
                ).cast(campo.dataType)

            else:
                # Não replica RVNE nem colunas ZEWS.
                expressao = F.lit(None).cast(
                    campo.dataType
                )

            expressoes_emissao_mercantil.append(
                expressao.alias(nome)
            )

        df_emissao_mercantil = (
            df_gwp_mercantil
            .select(
                *expressoes_emissao_mercantil
            )
        )

        df_emissao_final = (
            df_emissao_sem_mercantil
            .unionByName(
                df_emissao_mercantil
            )
        )

        return df_exposto, df_emissao_final
