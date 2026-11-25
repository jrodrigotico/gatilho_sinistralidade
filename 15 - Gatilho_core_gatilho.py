import pandas as pd
import unicodedata
import re
from pyspark.sql import functions as F


















# # Cálculos
# class CalcNepGatilho:
#     @staticmethod
#     def aplicar(df):
#         df = (
#             df
#             .withColumn("tipo_gatilho", F.trim(F.coalesce(F.col("tipo_gatilho").cast("string"), F.lit(""))))
#             .withColumn("origem_gatilho", F.lower(F.trim(F.coalesce(F.col("origem_gatilho").cast("string"), F.lit("")))))
#             .withColumn("nep", F.coalesce(F.col("nep"), F.lit(0.0)))
#             .withColumn("pro_labore", F.coalesce(F.col("pro_labore"), F.lit(0.0)))
#             .withColumn("agenciamento", F.coalesce(F.col("agenciamento"), F.lit(0.0)))
#             .withColumn("corretagem", F.coalesce(F.col("corretagem"), F.lit(0.0)))
#             .withColumn("nep_comp_zews", F.coalesce(F.col("nep_comp_zews"), F.lit(0.0)))
#             .withColumn("prolabore_diferido_zews", F.coalesce(F.col("prolabore_diferido_zews"), F.lit(0.0)))
#             .withColumn("agenciamento_diferido_zews", F.coalesce(F.col("agenciamento_diferido_zews"), F.lit(0.0)))
#             .withColumn("corretagem_diferido_zews", F.coalesce(F.col("corretagem_diferido_zews"), F.lit(0.0)))
            
#             .withColumn(
#                 "v_nep",
#                 F.when(F.col("origem_gatilho") == "carteira", F.col("nep"))
#                  .otherwise(F.col("nep_comp_zews"))
#             )
#             .withColumn(
#                 "v_pro_labore",
#                 F.when(F.col("origem_gatilho") == "carteira", F.col("pro_labore"))
#                  .otherwise(F.col("prolabore_diferido_zews"))
#             )
#             .withColumn(
#                 "v_agenciamento",
#                 F.when(F.col("origem_gatilho") == "carteira", F.col("agenciamento"))
#                  .otherwise(F.col("agenciamento_diferido_zews"))
#             )
#             .withColumn(
#                 "v_corretagem",
#                 F.when(F.col("origem_gatilho") == "carteira", F.col("corretagem"))
#                  .otherwise(F.col("corretagem_diferido_zews"))
#             )
#             .withColumn(
#                 "valor_pro_labore",
#                 F.when(
#                     F.col("origem_gatilho") == "carteira",
#                     F.col("v_nep") * F.col("v_pro_labore")
#                 ).otherwise(F.col("v_pro_labore"))
#             )
#             .withColumn(
#                 "valor_agenciamento",
#                 F.when(
#                     F.col("origem_gatilho") == "carteira",
#                     F.col("v_nep") * F.col("v_agenciamento")
#                 ).otherwise(F.col("v_agenciamento"))
#             )
#             .withColumn(
#                 "valor_corretagem",
#                 F.when(
#                     F.col("origem_gatilho") == "carteira",
#                     F.col("v_nep") * F.col("v_corretagem")
#                 ).otherwise(F.col("v_corretagem"))
#             )
#         )

#         calc_liq_iof = F.col("v_nep")
#         calc_liq_imp_pl = calc_liq_iof - (F.col("v_nep") * F.lit(0.0465))  - (F.col("v_pro_labore"))
#         calc_liq_imp_cor = calc_liq_iof - (F.col("v_nep") * F.lit(0.0465))  - (F.col("v_corretagem"))
#         calc_liq_imp_pl_cor = calc_liq_iof - (F.col("v_nep") * F.lit(0.0465))  - (F.col("v_pro_labore") + (F.col("v_corretagem")))
#         calc_liq_imp_pl_cor_ag = calc_liq_iof - (F.col("v_nep") * F.lit(0.0465))  - (F.col("v_pro_labore") + (F.col("v_corretagem")) + (F.col("v_agenciamento")))

#         return (
#         df
#             .withColumn("prolabore_spark", F.col("valor_pro_labore"))
#             .withColumn("agenciamento_spark", F.col("valor_agenciamento"))
#             .withColumn("corretagem_spark", F.col("valor_corretagem"))
#             .withColumn(
#             "nep_gatilho",
#                 F.when(F.col("tipo_gatilho") == "LIQ_IOF", calc_liq_iof)
#                 .when(F.col("tipo_gatilho") == "LIQ_IMP_PL", calc_liq_imp_pl)
#                 .when(F.col("tipo_gatilho") == "LIQ_IMP_COR", calc_liq_imp_cor)
#                 .when(F.col("tipo_gatilho") == "LIQ_IMP_PL_COR", calc_liq_imp_pl_cor)
#                 .when(F.col("tipo_gatilho") == "LIQ_IMP_PL_COR_AG", calc_liq_imp_pl_cor_ag)
#                 .otherwise(F.lit(0.0))
#         )
#         .drop("v_nep", "v_pro_labore", "v_agenciamento", "v_corretagem")
#         )


# class CalcULAE:
#     @staticmethod
#     def aplicar(df):
#         df = (
#             df
#             .withColumn("tipo_regra_ulae", F.lower(F.trim(F.coalesce(F.col("tipo_regra_ulae").cast("string"), F.lit("")))))
#             .withColumn("percent_ulae", F.coalesce(F.col("percent_ulae").cast("double"), F.lit(0.0)))
#             .withColumn("valor_ulae_mensal_calc", F.coalesce(F.col("valor_ulae_mensal_calc").cast("double"), F.lit(0.0)))
#             .withColumn("nep_gatilho", F.coalesce(F.col("nep_gatilho").cast("double"), F.lit(0.0)))
#             .withColumn("sinistro", F.coalesce(F.col("sinistro").cast("double"), F.lit(0.0)))
#             .withColumn("sinistro_zews", F.coalesce(F.col("sinistro_zews").cast("double"), F.lit(0.0)))
            
#             .withColumn(
#                 "tipo_regra_ulae",
#                 F.when(F.col("tipo_regra_ulae").isin("", "nan", "none", "null"), F.lit(""))
#                  .otherwise(F.col("tipo_regra_ulae"))
#             )
#             .withColumn(
#                 "v_sinistro",
#                 F.when(F.col("origem_gatilho") == "carteira", F.col("sinistro"))
#                  .otherwise(F.col("sinistro_zews"))
#             )
#         )

#         return df.withColumn(
#             "ulae_mensal",
#             F.when(
#                 F.col("tipo_regra_ulae") == "percentual_premio",
#                 F.col("nep_gatilho") * F.col("percent_ulae")
#             ).when(
#                 F.col("tipo_regra_ulae") == "percentual_sinistro",
#                 F.col("v_sinistro") * F.col("percent_ulae")
#             ).when(
#                 F.col("tipo_regra_ulae") == "calculo_zurich",
#                 F.col("valor_ulae_rateado")
#             ).when(
#                 F.col("tipo_regra_ulae").isin("", "sem_ulae", "sem_regra"),
#                 F.lit(0.0)
#             ).otherwise(F.lit(0.0))
#         )


# class CalcIBNR:
#     @staticmethod
#     def aplicar(df):
#         df = (
#             df
#             .withColumn("origem_gatilho", F.lower(F.trim(F.coalesce(F.col("origem_gatilho").cast("string"), F.lit("")))))
#             .withColumn("tipo_regra_ibnr", F.lower(F.trim(F.coalesce(F.col("tipo_regra_ibnr").cast("string"), F.lit("")))))
#             .withColumn("percent_premio_ibnr", F.coalesce(F.col("percent_premio_ibnr").cast("double"), F.lit(0.0)))
#             .withColumn("percent_sinistro_ibnr", F.coalesce(F.col("percent_sinistro_ibnr").cast("double"), F.lit(0.0)))
#             .withColumn("premio_ganho_12m", F.coalesce(F.col("premio_ganho_12m").cast("double"), F.lit(0.0)))
#             .withColumn("sinistro_12m", F.coalesce(F.col("sinistro_12m").cast("double"), F.lit(0.0)))
#             .withColumn("ibnr_exp_carteira_acum", F.coalesce(F.col("ibnr_exp_carteira_acum").cast("double"), F.lit(0.0)))
            
#             .withColumn(
#                 "tipo_regra_ibnr",
#                 F.when(F.col("tipo_regra_ibnr").isin("", "nan", "none", "null"), F.lit(""))
#                  .otherwise(F.col("tipo_regra_ibnr")))
#         )

#         valor_premio = F.col("nep_gatilho") * F.col("percent_premio_ibnr")
#         valor_sinistro = F.col("sinistro") * F.col("percent_sinistro_ibnr")

#         return df.withColumn(
#             "ibnr_mensal",
#             F.when(
#                 F.col("tipo_regra_ibnr") == "experiencia_carteira",
#                 F.col("ibnr_exposto")
#             ).when(
#                 F.col("tipo_regra_ibnr") == "max_premio_sinistro",
#                 F.greatest(valor_premio, valor_sinistro)
#             ).when(
#                 F.col("tipo_regra_ibnr") == "percentual_premio",
#                 valor_premio
#             ).when(
#                 F.col("tipo_regra_ibnr") == "percentual_sinistro",
#                 valor_sinistro
#             ).when(
#                 F.col("tipo_regra_ibnr").isin("", "sem_ibnr", "sem_regra"),
#                 F.lit(0.0)
#             ).otherwise(F.lit(0.0))
#         )


# class CalcCustoTotalMensal:
#     @staticmethod
#     def aplicar(df):
#         df = (
#             df
#             .withColumn("custo_mensal_regra", F.lower(F.trim(F.coalesce(F.col("custo_mensal_regra").cast("string"), F.lit("")))))
#             .withColumn("custo_mensal_regra", F.regexp_replace(F.col("custo_mensal_regra"), "[()]", ""))
#             .withColumn("custo_mensal_regra", F.regexp_replace(F.col("custo_mensal_regra"), "ç", "c"))
#             .withColumn("custo_mensal_regra", F.regexp_replace(F.col("custo_mensal_regra"), "ã", "a"))
#             .withColumn("custo_mensal_regra", F.regexp_replace(F.col("custo_mensal_regra"), r"[+ ]", "_"))
#             .withColumn("custo_mensal_regra", F.regexp_replace(F.col("custo_mensal_regra"), "_+", "_"))
#             .withColumn("custo_mensal_regra", F.regexp_replace(F.col("custo_mensal_regra"), r"^_|_$", ""))
#         )

#         for coluna in ["sinistro", "ulae_mensal", "ibnr_mensal", "cap_mensal"]:
#             df = df.withColumn(
#                 coluna,
#                 F.coalesce(
#                     F.regexp_replace(F.col(coluna).cast("string"), ",", ".").cast("double"),
#                     F.lit(0.0)
#                 )
#             )

#         sinistro = F.col("sinistro")
#         ibnr = F.col("ibnr_mensal")
#         ulae = F.col("ulae_mensal")
#         cap = F.col("cap_mensal")

#         return df.withColumn(
#             "valor_custo_mensal",
#             F.when(F.col("custo_mensal_regra") == "sinistro_ulae", sinistro + ulae)
#              .when(F.col("custo_mensal_regra") == "sinistro_ibnr_ulae", sinistro + ibnr + ulae)
#              .when(F.col("custo_mensal_regra") == "sinistro_ibnr", sinistro + ibnr)
#              .when(F.col("custo_mensal_regra") == "sinistro_pagos_pendentes", sinistro)
#              .when(F.col("custo_mensal_regra") == "sinistro_ulae_capitalizacao", sinistro + ulae + cap)
#              .when(F.col("custo_mensal_regra") == "sinistro_ibnr_ulae_capitalizacao", sinistro + ibnr + ulae + cap)
#              .otherwise(F.lit(0.0))
#         )

#         return df