# Databricks notebook source
# MAGIC %sql
# MAGIC select *
# MAGIC from san_affinities.tb_comiss_farol_aux_gatilho;
# MAGIC -- describe san_affinities.tb_comiss_farol_aux_gatilho;
# MAGIC
# MAGIC
# MAGIC -- select *
# MAGIC -- from hive_metastore.san_affinities.tb_farol_corretagem;
# MAGIC
# MAGIC
# MAGIC -- select *
# MAGIC -- from  hive_metastore.san_affinities.base_emissao_consolidada;
# MAGIC
# MAGIC
# MAGIC select *
# MAGIC from san_affinities.tb_calc_ulae_zurich;
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

import datetime
import pandas as pd
import importlib
import importlib.util
import os
import re
import sys
import unicodedata
from datetime import date
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum as spark_sum

import utils_gatilho
importlib.reload(utils_gatilho)
import transformation
importlib.reload(transformation)
import cargas_gatilho
importlib.reload(cargas_gatilho)


# COMMAND ----------

# --------------------------- data_snapshot
# mes_anterior = date.today() - relativedelta(months=1)
# data_snapshot_input = mes_anterior.replace(day=20).strftime("%Y-%m-%d") # roda semmpre com a referencia dia 20 de cada mes considerando M-1
data_snapshot_input = '2026-11-23'

# --------------------------- formatos
origens_que_usam_zews = ["RFD", "GE", "OTHERS"]
df_formatos_gatilho_ajustado = cargas_gatilho.carregar_formatos_gatilho(spark)
print("✅ Formatos gerado")

df_rls_gatilho = cargas_gatilho.gerar_rls_gatilho(spark)
print("✅ RLS gerado")

# chaves distintas que devem usar ZEWS.
df_chaves_zews_excel = (
    df_formatos_gatilho_ajustado
    .select(
        F.trim(
            F.col("chave").cast("string")
        ).alias("chave"),
        F.upper(
            F.trim(
                F.coalesce(
                    F.col("origem_gatilho").cast("string"),
                    F.lit("")
                )
            )
        ).alias("origem_gatilho")
    )
    .filter(
        F.col("chave").isNotNull()
        & (F.col("chave") != "")
        & F.col("origem_gatilho").isin(
            origens_que_usam_zews
        )
    )
    .select("chave")
    .distinct()
)

chave_zews_excel = [
    linha["chave"]
    for linha in df_chaves_zews_excel.collect()
]

if chave_zews_excel:
    apolices_zews_sql = ", ".join(
        f"'{chave[:-1]}'"
        for chave in chave_zews_excel
    )
else:
    apolices_zews_sql = "NULL"

# ------------------------- percentuais exposto e farol
df_exposto_percent = cargas_gatilho.carregar_percentuais_exposto(spark=spark, apolices_zews_sql=apolices_zews_sql)
df_comiss_farol = cargas_gatilho.carregar_comissao_farol(spark=spark).drop("apolice")

# ----------------------------------- bases carteira
print("🚀 Rodando bases da Carteira...")
df_emissao_base, df_exposto_base, df_sinistro_base = cargas_gatilho.carregar_bases_carteira(spark=spark, data_snapshot_input=data_snapshot_input, apolices_zews_sql=apolices_zews_sql)
cargas_gatilho.validar_bases_carteira(df_emissao_base, df_exposto_base, df_sinistro_base)
print("✅ Bases Carteira carregadas e validadas")

# ============================================================
# EXCEÇÕES FFC: COPIAR DADOS DA CARTEIRA
# ============================================================
df_mapa_ffc = spark.createDataFrame(
    [
        ("017191882611", "017191882612"),
        ("017791865011", "017791865012"),
    ],
    "chave_origem string, chave_ffc string"
)

df_emissao_base = transformation.adicionar_copias_ffc(
    df=df_emissao_base,
    coluna_data="dt_emissao",
    df_mapa_ffc=df_mapa_ffc
)

df_exposto_base = transformation.adicionar_copias_ffc(
    df=df_exposto_base,
    coluna_data="dt_nep",
    df_mapa_ffc=df_mapa_ffc
)

df_sinistro_base = transformation.adicionar_copias_ffc(
    df=df_sinistro_base,
    coluna_data="dt_ocorrencia",
    df_mapa_ffc=df_mapa_ffc
)

# ----------------------------------- bases zews
print("🔥 Rodando bases ZEWS...")
df_emissao_zews, df_exposto_zews, df_sinistro_zews = cargas_gatilho.carregar_bases_zews(spark)
# df_exposto_zews = transformation.ajustar_chave_renovacao_havan(
#     df=df_exposto_zews,
#     coluna_data="dt_nep_zews",
#     coluna_chave="chave_zews"
# )
# cargas_gatilho.validar_bases_zews(df_emissao_zews, df_exposto_zews, df_sinistro_zews)
print("✅ Bases ZEWS carregadas e validadas")

# ----------------------------------- criação das bases finais 'intermediarias'
df_emissao_final = utils_gatilho.colunas_em_snake_case(
    transformation.juntar_por_chave_mes(
        df_carteira=df_emissao_base,
        df_zews=df_emissao_zews,
        df_formatos_gatilho=df_formatos_gatilho_ajustado,
        data_carteira="dt_emissao",
        data_zews="dt_emissao_zews",
        substituicoes_zews={"gwp": "premio_liq_iof_zews"}
    )
)

df_exposto_final = utils_gatilho.colunas_em_snake_case(
    transformation.juntar_por_chave_mes(
        df_carteira=df_exposto_base,
        df_zews=df_exposto_zews,
        df_formatos_gatilho=df_formatos_gatilho_ajustado,
        data_carteira="dt_nep",
        data_zews="dt_nep_zews",
        substituicoes_zews={
            "nep": "nep_comp_zews",
            "ibnr_exposto": "ibnr_zews"
        }
    )
)

df_sinistro_final = utils_gatilho.colunas_em_snake_case(
    transformation.juntar_por_chave_mes(
        df_carteira=df_sinistro_base,
        df_zews=df_sinistro_zews,
        df_formatos_gatilho=df_formatos_gatilho_ajustado,
        data_carteira="dt_ocorrencia",
        data_zews="dt_ocorrencia_zews",
        substituicoes_zews={"sinistro": "sinistro_zews"}
    )
)

# ============================================================
# BASE DE EXPOSTO
# ============================================================
df_base = (
    df_exposto_final.withColumn("chave_join", F.trim(F.col("chave").cast("string")))
    .withColumn(
        "ano_mes_join",
        F.date_format(
            F.col("dt_nep"),
            "yyyyMM"
        ).cast("int")
    )
)

# ----------------------------------- bases auxiliares
print("🔧 Preparando bases auxiliares...")
df_assist_join, df_outros_custos_join, df_qbe_base, df_ulae_zurich = cargas_gatilho.carregar_bases_auxiliares(spark=spark)

# ----------------------------------- juntando QBE apenas em exposto e sinistro
df_qbe_join_exposto = (
    df_qbe_base
    .groupBy("chave_join", "ano_mes_join")
    .agg(F.sum("premio_ganho_qbe").alias("premio_ganho_qbe")
    )
)

df_qbe_join_sinistro = (
    df_qbe_base
    .groupBy(
        "chave_join",
        "ano_mes_join"
    )
    .agg(
        F.sum("sinistro_qbe").alias("sinistro_qbe")
    )
)

# ============================================================
# EXPOSTO: ASSISTÊNCIA, nep QBE, OUTROS CUSTOS, FAROL E CAPITALIZAÇÃO
# ============================================================
dt_comp_yyyymm = F.date_format(F.col("dt_nep"), "yyyyMM").cast("int")
apolice_da_chave = F.substring(F.col("chave_join").cast("string"), 1, 11)

df_exposto = (
    df_base
    .join(
        df_qbe_join_exposto,
        on=["chave_join", "ano_mes_join"],
        how="full"
    )

    # Recupera a chave nas linhas existentes somente no QBE.
    .withColumn(
        "chave",
        F.coalesce(
            F.col("chave"),
            F.col("chave_join")
        )
    )

    # Recupera a data nas linhas existentes somente no QBE.
    .withColumn(
        "dt_nep",
        F.coalesce(
            F.col("dt_nep"),
            F.to_date(
                F.concat(
                    F.col("ano_mes_join").cast("string"),
                    F.lit("01")
                ),
                "yyyyMMdd"
            )
        )
    )

    .join(
        df_assist_join,
        on=["chave_join", "ano_mes_join"],
        how="left"
    )
    .join(
        df_outros_custos_join,
        on=["chave_join", "ano_mes_join"],
        how="left"
    )
    .join(
        df_comiss_farol,
        on=["chave"],
        how="left"
    )
)   

df_exposto = df_exposto.withColumn(
    "cap_mensal",
    F.when(
        F.substring(F.col("chave"), 1, 11) == "01909186124",
        F.when(F.date_format("dt_nep", "yyyyMM").cast("int") <= 202305, 184000)
        .when(F.date_format("dt_nep", "yyyyMM").cast("int") <= 202503, 160800)
        .otherwise(0)
    )
    .when(F.substring(F.col("chave"), 1, 11) == "01909186128", 4533)
    .when(
        (F.substring(F.col("chave"), 1, 11) == "01909186136")
        & (F.date_format("dt_nep", "yyyyMM").cast("int") >= 202505),
        160800
    )
    .otherwise(0)
)

apolice_da_chave = F.substring(F.col("chave").cast("string"), 1, 11)

apolice_existente = (
    F.col("apolice").cast("string")
    if "apolice" in df_exposto.columns
    else F.lit(None).cast("string")
)

df_exposto = df_exposto.withColumn(
    "apolice",
    F.coalesce(
        apolice_existente,
        apolice_da_chave
    )
)

# ============================================================
# MERCANTIL: CRIA SAFRA 01 NA CHAVE V2
# ============================================================
chave_mercantil_input = "667791861252"     # Versao 2 = Safra 01
chave_mercantil_carteira = "667791861251"  # Versao 1 = Safra 02
apolice_mercantil = "66779186125"

df_premio_ganho_mercantil = (transformation.carregar_premio_ganho_mercantil(spark=spark))
utils_gatilho.checar_duplicidade(df_premio_ganho_mercantil,"df_premio_ganho_mercantil", ["chave_join", "ano_mes_join"])

# Cria as linhas da Safra 01 diretamente a partir dos inputs.
# Usa o schema do exposto para manter unionByName consistente.
df_mercantil_safra_01 = (
    df_premio_ganho_mercantil
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
    df_premio_ganho_mercantil
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

# ============================================================
# SINISTRO: SINISTRO QBE
# QBE pode possuir valores em meses sem sinistro carteira/ZEWS
# ============================================================
df_sinistro_final = (
    df_sinistro_final

    # Chaves auxiliares para o join
    .withColumn(
        "chave_join",
        F.trim(F.col("chave").cast("string"))
    )
    .withColumn(
        "ano_mes_join",
        F.date_format(
            F.col("dt_ocorrencia"),
            "yyyyMM"
        ).cast("int")
    )

    # FULL para preservar meses existentes somente no QBE
    .join(
        df_qbe_join_sinistro,
        on=["chave_join", "ano_mes_join"],
        how="full"
    )

    # Quando a linha veio somente do QBE, recupera a chave
    .withColumn(
        "chave",
        F.coalesce(
            F.col("chave"),
            F.col("chave_join")
        )
    )

    # Quando a linha veio somente do QBE,
    # transforma AAAAMM no primeiro dia do mês
    .withColumn(
        "dt_ocorrencia",
        F.coalesce(
            F.col("dt_ocorrencia"),
            F.to_date(
                F.concat(
                    F.col("ano_mes_join").cast("string"),
                    F.lit("01")
                ),
                "yyyyMMdd"
            )
        )
    )

    # Sem QBE = zero
    .withColumn(
        "sinistro_qbe",
        F.coalesce(
            F.col("sinistro_qbe"),
            F.lit(0.0)
        )
    )
)



# COMMAND ----------

import utils_gatilho
importlib.reload(utils_gatilho)
import transformation
importlib.reload(transformation)
import cargas_gatilho
importlib.reload(cargas_gatilho)


df_exposto_teste = transformation.calcular_ulae_zurich_exposto(
    df_exposto=df_exposto,
    df_formatos=df_formatos_gatilho_ajustado,
    df_ulae_zurich=df_ulae_zurich,
    coluna_regra_ulae="tipo_regra_ulae",
    coluna_data_ulae="data",
)

# df_emissao_final = transformation.ajustar_chave_renovacao_havan(
#     df=df_emissao_final,
#     coluna_data="dt_emissao",
#     data_corte="2025-01-01"
# )

# df_exposto = transformation.ajustar_chave_renovacao_havan(
#     df=df_exposto,
#     coluna_data="dt_nep",
#     data_corte="2025-01-01"
# )

# df_sinistro_final = transformation.ajustar_chave_renovacao_havan(
#     df=df_sinistro_final,
#     coluna_data="dt_ocorrencia",
#     data_corte="2025-01-01"
# )


# # ============================================================
# # final
# # ============================================================
# chaves_formatos = (
#     df_formatos_gatilho_ajustado
#     .select(
#         F.trim(F.col("chave").cast("string")).alias("chave")
#     )
#     .filter(
#         F.col("chave").isNotNull()
#         & (F.col("chave") != "")
#     )
#     .distinct()
# )


# df_emissao_final = (
#     df_emissao_final
#     .withColumn("chave", F.trim(F.col("chave").cast("string")))
#     .join(chaves_formatos, on="chave", how="left_semi")
# )

# df_exposto_final = (
#     df_exposto
#     .withColumn("chave", F.trim(F.col("chave").cast("string")))
#     .join(chaves_formatos, on="chave", how="left_semi")
# )

# df_sinistro_final = (
#     df_sinistro_final
#     .withColumn("chave", F.trim(F.col("chave").cast("string")))
#     .join(chaves_formatos, on="chave", how="left_semi")
# )

# # ----------------------------- CORTES DE PERÍODO POR AGRUPADO
# regras_por_agrupado = [
#     ("Vivo - Runoff", "2019-11-01"),
#     ('Riachuelo - Renovação 2023', '2023-04-01')
# ]

# df_parametros_corte = (
#     spark.createDataFrame(
#         regras_por_agrupado,
#         "agrupado string, inicio_texto string"
#     )
#     .select(
#         F.upper(F.trim("agrupado")).alias("agrupado"),
#         F.to_date("inicio_texto", "yyyy-MM-dd").alias(
#             "dt_inicio_gatilho"
#         )
#     )
# )


# # Relação entre agrupado e chave, obtida dos formatos.
# df_mapa_agrupados = (
#     df_formatos_gatilho_ajustado
#     .select(
#         F.trim(
#             F.col("chave").cast("string")
#         ).alias("chave"),

#         F.upper(
#             F.trim(F.col("agrupado").cast("string"))
#         ).alias("agrupado"),
#     )
#     .filter(
#         F.col("chave").isNotNull()
#         & (F.col("chave") != "")
#     )
#     .distinct()
# )


# df_regras_corte = (
#     df_mapa_agrupados
#     .join(
#         df_parametros_corte,
#         on="agrupado",
#         how="inner"
#     )
#     .select(
#         "chave",
#         "agrupado",
#         "dt_inicio_gatilho"
#     )
#     .distinct()
# )

# # aplicar corte
# df_emissao_final, df_emissao_excluidos_corte = transformation.aplicar_corte_por_chave(
#     df_emissao_final,
#     "dt_emissao",
#     df_regras_corte
# )

# df_exposto_final, df_exposto_excluidos_corte = transformation.aplicar_corte_por_chave(
#     df_exposto_final,
#     "dt_nep",
#     df_regras_corte
# )

# df_sinistro_final, df_sinistro_excluidos_corte = transformation.aplicar_corte_por_chave(
#     df_sinistro_final,
#     "dt_ocorrencia",
#     df_regras_corte
# )

# # Sky: classificar o historico sem excluir registro
# df_emissao_final = transformation.classificar_periodo_sky(
#     df=df_emissao_final,
#     coluna_data="dt_emissao",
#     df_formatos=df_formatos_gatilho_ajustado
# )

# df_exposto_final = transformation.classificar_periodo_sky(
#     df=df_exposto_final,
#     coluna_data="dt_nep",
#     df_formatos=df_formatos_gatilho_ajustado
# )

# df_sinistro_final = transformation.classificar_periodo_sky(
#     df=df_sinistro_final,
#     coluna_data="dt_ocorrencia",
#     df_formatos=df_formatos_gatilho_ajustado
# )

# # ------------------------- criação das bases finais
# bases = {
#     "emissao": df_emissao_final,
#     "exposto": df_exposto_final,
#     "sinistro": df_sinistro_final,
# }

# for assunto, df in bases.items():
#     (
#         df.write
#         .format("delta")
#         .mode("overwrite")
#         .option("overwriteSchema", "true")
#         .saveAsTable(f"san_affinities.gatilho_{assunto}")
#     )

# # ----------------------------- inputs
# # sera usado para o caso de IBNR atuarial do GCB e Havan, esses valores vem fechados da area atuarial, portanto nao tem como eu fazer um calculo separado
# df_inputs = spark.sql(
#     """
#     CREATE or replace TABLE san_affinities.tb_ibnr_input (
#         agrupado STRING NOT NULL,
#         ramo STRING NOT NULL,
#         dt_referencia DATE NOT NULL,
#         valor_ibnr_input DOUBLE NOT NULL,
#         chave_input_ibnr STRING GENERATED ALWAYS AS (
#             CONCAT(
#                 UPPER(TRIM(agrupado)),
#                 '|',
#                 LPAD(TRIM(ramo), 2, '0')
#             )
#         )
#     )
#     USING DELTA
#     """
# )

# # nome dos parceiros precisa estar sempre tudo maiusculo para conseguir dar match com formatos_gatilho_recarga
# spark.sql(
#     """
#     INSERT INTO san_affinities.tb_ibnr_input (
#         agrupado,
#         ramo,
#         dt_referencia,
#         valor_ibnr_input
#     )
#     VALUES
#         ('GRUPO CASAS BAHIA - RENOVAÇÃO 2022', '71', DATE '2026-09-01', 1861877),
#         ('GRUPO CASAS BAHIA - RENOVAÇÃO 2022', '95', DATE '2026-09-01', 9538694)
#     """
# )

# print("✅ Finalizado !")

# COMMAND ----------


chave_teste = "019591862381"
apolice_teste = chave_teste[:-1]  # 01959186238

display(
    df_ulae_teste_apolice
    .withColumn("chave_testada", F.lit(chave_teste))
    .filter(F.col("mes") >= F.lit("2024-01-01").cast("date"))
    .orderBy("mes")
)

# COMMAND ----------

display(
    spark.table("san_affinities.exposto_gatilho")
    .filter(
        (F.col("APOLICE").cast("string") == "01959186238")
        & (F.to_date("DT_COMP") >= F.lit("2025-01-01").cast("date"))
    )
    .select("APOLICE", "CHAVE", "DT_COMP", "NEP")
    .orderBy("DT_COMP", "CHAVE")
)

# COMMAND ----------

(
    df_exposto
    .withColumn("chave_teste", F.trim(F.col("chave").cast("string")))
    .withColumn("apolice_teste", F.substring("chave_teste", 1, 11))
    .withColumn("mes_nep", F.trunc(F.to_date("dt_nep"), "month"))
    .filter(
        F.col("apolice_teste").isin("01909186124", "01909186136")
        & F.col("mes_nep").isin(
            F.lit("2025-04-01").cast("date"),
            F.lit("2025-05-01").cast("date"),
        )
    )
    .groupBy("mes_nep", "apolice_teste", "chave_teste")
    .agg(
        F.count("*").alias("qtd_linhas"),
        F.count("nep").alias("qtd_nep_preenchido"),
        F.sum("nep").alias("nep"),
    )
    .orderBy("mes_nep", "apolice_teste", "chave_teste")
    .show(truncate=False)
)