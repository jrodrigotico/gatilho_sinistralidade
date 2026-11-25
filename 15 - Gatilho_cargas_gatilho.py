from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F
import utils_gatilho
import transformation
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType)


def carregar_formatos_gatilho (spark):
    origens_que_usam_zews = ["RFD", "GE", "OTHERS"]
    df_formatos_gatilho_ajustado = (carregar_formatos_gatilho_origem(spark).withColumn("VERSAO", F.col("VERSAO").cast("int")))
    colunas_percentuais = ["PERCENT_PREMIO_IBNR", "PERCENT_SINISTRO_IBNR", "PERCENT_ULAE", "SINISTRALIDADE_PLANO"]

    for coluna in colunas_percentuais:
        df_formatos_gatilho_ajustado = df_formatos_gatilho_ajustado.withColumn(
            coluna,
            F.when(
                F.col(coluna).isNull() | (F.trim(F.col(coluna).cast("string")) == ""),
                F.lit(None).cast("decimal(18,10)")
            ).otherwise(
                F.regexp_replace(
                    F.trim(F.col(coluna).cast("string")),
                    ",",
                    "."
                ).cast("decimal(18,10)")
            )
        )

    df_formatos_gatilho_ajustado = utils_gatilho.colunas_em_snake_case(df_formatos_gatilho_ajustado)

    nomes_omni = ["Omni - Renovação 2019", "Omni - Renovação 2020", 'Omni - Renovação 2021']

    df_formatos_gatilho_ajustado = (
        df_formatos_gatilho_ajustado
        .filter(
            F.col("chave").isNull()
            | (F.col("chave") != "000000000000")
        )
        .filter(
            F.upper(
                F.trim(
                    F.coalesce(
                        F.col("mostrar_gatilho").cast("string"),
                        F.lit("")
                    )
                )
            ) == "SIM"
        )
        .withColumn(
            "nome_produto_comercial",
            F.upper(F.col("nome_produto_comercial"))
        )
        .withColumn(
            "apolice",
            F.when(
                F.length(F.col("chave").cast("string")) > 1,
                F.expr(
                    "substring(cast(chave as string), 1, "
                    "length(cast(chave as string)) - 1)"
                )
            ).otherwise(F.lit(None).cast("string"))
        )
        .withColumn(
            "agrupado",
            F.when(F.col("agrupado").isin(nomes_omni), "Omni")
            .otherwise(F.col("agrupado"))    
        )
    )

    # unindo os fatores de ibnr do atuarial
    df_fatores_ibnr = carregar_fatores_ibnr_gatilho(spark)
    colunas_fatores = [f"fator_{numero}" for numero in range(6)]

    df_formatos_gatilho_ajustado = (
        df_formatos_gatilho_ajustado
        .drop(*colunas_fatores)
        .alias("g")
        .join(
            df_fatores_ibnr.alias("f"),
            F.col("g.chave") == F.col("f.chave"),
            how="left",
        )
        .select(
            "g.*",
            *[F.col(f"f.{coluna}").alias(coluna) for coluna in colunas_fatores],
        )
    )

    # chave para conseguir puxar o valor dos inputs de ibnr atuarial
    df_formatos_gatilho_ajustado = (
        df_formatos_gatilho_ajustado
        .withColumn(
            "agrupado",
            F.upper(
                F.trim(
                    F.coalesce(
                        F.col("agrupado").cast("string"),
                        F.lit(""),
                    )
                )
            ),
        )
        .withColumn(
            "agrupado",
            F.when(
                F.upper(F.trim(F.col("agrupado"))).isin(
                    "VIVO",
                    "VIVO - RUNOFF"
                ),
                F.lit("VIVO - RUNOFF")
            )
            .when(
                F.upper(F.trim(F.col("agrupado"))).isin(
                    "RIACHUELO",
                    "RIACHUELO - RENOVAÇÃO 2023"
                ),
                F.lit("RIACHUELO - RENOVAÇÃO 2023")
            )
            .otherwise(F.col("agrupado"))
        )
        .withColumn(
            "RAMO",
            F.lpad(
                F.regexp_replace(
                    F.trim(
                        F.coalesce(
                            F.col("RAMO").cast("string"),
                            F.lit(""),
                        )
                    ),
                    r"\.0$",
                    "",
                ),
                2,
                "0",
            ),
        )
        .withColumn(
            "chave_ibnr_input",
            F.concat(
                F.col("agrupado"),
                F.lit("|"),
                F.col("RAMO"),
            ),
        )
    )

    (
        df_formatos_gatilho_ajustado.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("san_affinities.formatos_gatilho_ajustado")
    )

    return df_formatos_gatilho_ajustado


def gerar_rls_gatilho(spark):
    spark.sql(
        """
        CREATE OR REPLACE TABLE san_affinities.rls_gatilho AS
        SELECT
            chave,
            start,
            agrupado,
            acesso
        FROM san_affinities.formatos_gatilho_recarga
        WHERE chave IS NULL
           OR chave <> '000000000000'
        """
    )
    return spark.table("san_affinities.rls_gatilho")


def carregar_percentuais_exposto(
    spark,
    apolices_zews_sql,
    tabela_destino="san_affinities.exposto_percentuais_gatilho"):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {tabela_destino} AS

        WITH ValoresGE AS (
            SELECT
                CAST(APOLICE AS STRING) AS APOLICE,

                CONCAT(
                    CAST(APOLICE AS STRING),
                    CASE
                        WHEN CAST(APOLICE AS STRING) IN ({apolices_zews_sql})
                             AND TO_DATE(DT_COMP) >= DATE '2025-01-01'
                            THEN '2'
                        ELSE '1'
                    END
                ) AS CHAVE,

                DT_COMP,

                SUM(PROLABORE_DIFERIDO) AS PRO_LABORE,
                SUM(AGENCIAMENTO_DIFERIDO) AS AGENCIAMENTO,
                SUM(CORRETAGEM_DIFERIDO) AS CORRETAGEM,
                SUM(NEP_COMP) AS NEP

            FROM san_affinities.base_calc_exposto_ge_final_sem_ibnr

            GROUP BY
                CAST(APOLICE AS STRING),

                CONCAT(
                    CAST(APOLICE AS STRING),
                    CASE
                        WHEN CAST(APOLICE AS STRING) IN ({apolices_zews_sql})
                             AND TO_DATE(DT_COMP) >= DATE '2025-01-01'
                            THEN '2'
                        ELSE '1'
                    END
                ),

                DT_COMP
        ),

        ValoresRFD AS (
            SELECT
                CAST(APOLICE AS STRING) AS APOLICE,

                CONCAT(
                    CAST(APOLICE AS STRING),
                    CASE
                        WHEN CAST(APOLICE AS STRING) IN ({apolices_zews_sql})
                             AND TO_DATE(DT_COMP) >= DATE '2025-01-01'
                            THEN '2'
                        ELSE '1'
                    END
                ) AS CHAVE,

                DT_COMP,

                SUM(PROLABORE_DIFERIDO) AS PRO_LABORE,
                SUM(AGENCIAMENTO_DIFERIDO) AS AGENCIAMENTO,
                SUM(CORRETAGEM_DIFERIDO) AS CORRETAGEM,
                SUM(NEP_COMP) AS NEP

            FROM san_affinities.base_calc_exposto_rfd_final_sem_ibnr

            GROUP BY
                CAST(APOLICE AS STRING),

                CONCAT(
                    CAST(APOLICE AS STRING),
                    CASE
                        WHEN CAST(APOLICE AS STRING) IN ({apolices_zews_sql})
                             AND TO_DATE(DT_COMP) >= DATE '2025-01-01'
                            THEN '2'
                        ELSE '1'
                    END
                ),

                DT_COMP
        ),

        ValoresOTHERS AS (
            SELECT
                CAST(APOLICE AS STRING) AS APOLICE,

                CONCAT(
                    CAST(APOLICE AS STRING),
                    CASE
                        WHEN CAST(APOLICE AS STRING) IN ({apolices_zews_sql})
                             AND TO_DATE(DT_COMP) >= DATE '2025-01-01'
                            THEN '2'
                        ELSE '1'
                    END
                ) AS CHAVE,

                DT_COMP,

                SUM(PROLABORE_DIFERIDO) AS PRO_LABORE,
                SUM(AGENCIAMENTO_DIFERIDO) AS AGENCIAMENTO,
                SUM(CORRETAGEM_DIFERIDO) AS CORRETAGEM,
                SUM(NEP_COMP) AS NEP

            FROM san_affinities.base_calc_exposto_others_final_sem_ibnr

            GROUP BY
                CAST(APOLICE AS STRING),

                CONCAT(
                    CAST(APOLICE AS STRING),
                    CASE
                        WHEN CAST(APOLICE AS STRING) IN ({apolices_zews_sql})
                             AND TO_DATE(DT_COMP) >= DATE '2025-01-01'
                            THEN '2'
                        ELSE '1'
                    END
                ),

                DT_COMP
        ),

        UnionTables AS (
            SELECT * FROM ValoresGE

            UNION ALL

            SELECT * FROM ValoresRFD

            UNION ALL

            SELECT * FROM ValoresOTHERS
        )

        SELECT
            APOLICE,
            CHAVE,
            DT_COMP,

            PRO_LABORE / NEP AS PRO_LABORE,
            AGENCIAMENTO / NEP AS AGENCIAMENTO,
            CORRETAGEM / NEP AS CORRETAGEM,

            (
                PRO_LABORE
                + AGENCIAMENTO
                + CORRETAGEM
            ) / NEP AS COMISSAO

        FROM UnionTables
    """)

    return spark.table(tabela_destino)


def carregar_comissao_farol(spark):
    tabela = "san_affinities.tb_comiss_farol_aux_gatilho"

    colunas = [
        "apolice",
        "ramo",
        "nome",
        "comiss_total",
        "pro_labore_farol",
        "agenc_farol",
        "corr_farol",
        "chave",
    ]

    df = spark.table(tabela).select(*colunas)

    regras_havan = {
        "71": {
            "comiss_total": 47.35,
            "pro_labore_farol": 43.15,
            "agenc_farol": 0.00,
            "corr_farol": 4.20,
        },
        "77": {
            # Mesma composição do ramo 95
            "comiss_total": 48.65,
            "pro_labore_farol": 46.5,
            "agenc_farol": 0.00,
            "corr_farol": 2.15,
        },
        "90": {
            "comiss_total": 53.20,
            "pro_labore_farol": 51.70,
            "agenc_farol": 0.00,
            "corr_farol": 1.50,
        },
        "95": {
            "comiss_total": 55.60,
            "pro_labore_farol": 51.40,
            "agenc_farol": 0.00,
            "corr_farol": 4.20,
        },
    }

    chaves_havan = [
        "019091861241",
        "019091861261",
        "019091861271",
        "019591862381",
        "017791863361",
        "017791864031",
        "017191869331",
        "017191869341",
        "017191869321",
    ]

    dados_havan = []

    for chave in chaves_havan:
        ramo = chave[2:4]
        regra = regras_havan[ramo]

        dados_havan.append(
            (
                chave[:11],
                ramo,
                "HAVAN",
                regra["comiss_total"],
                regra["pro_labore_farol"],
                regra["agenc_farol"],
                regra["corr_farol"],
                chave,
            )
        )

    df_havan = spark.createDataFrame(
        dados_havan,
        """
        apolice string,
        ramo string,
        nome string,
        comiss_total double,
        pro_labore_farol double,
        agenc_farol double,
        corr_farol double,
        chave string
        """,
    )

    return (
        df.join(
            F.broadcast(df_havan.select("chave")),
            on="chave",
            how="left_anti",
        )
        .unionByName(df_havan)
        .select(*colunas)
    )


def carregar_bases_carteira(
    spark,
    data_snapshot_input,
    apolices_zews_sql):

    # ============================================================
    # EMISSÃO CARTEIRA
    # ============================================================

    tabela_emissao = "san_affinities.emissao_gatilho"

    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {tabela_emissao} AS

        WITH formatos_gatilho_ultima_versao AS (
            SELECT
                START,
                CAST(VERSAO AS INT) AS VERSAO
            FROM (
                SELECT
                    START,
                    VERSAO,
                    ROW_NUMBER() OVER (
                        PARTITION BY START
                        ORDER BY CAST(VERSAO AS INT) DESC
                    ) AS rn
                FROM san_affinities.formatos_gatilho_recarga
                WHERE MOSTRAR_GATILHO = 'SIM'
            )
            WHERE rn = 1
        )

        SELECT
            CAST(bs.APOLICE AS STRING) AS APOLICE,

            CONCAT(
                CAST(bs.APOLICE AS STRING),
                CASE
                    WHEN CAST(bs.APOLICE AS STRING)
                             IN ({apolices_zews_sql})
                         AND TO_DATE(bs.DTCOMP) >= DATE '2025-01-01'
                        THEN '2'
                    ELSE '1'
                END
            ) AS CHAVE,

            bs.DTCOMP AS DT_COMP,

            SUM(bs.PREMIO_LIQ_IOF) AS GWP,
            SUM(bs.RVNE) AS RVNE

        FROM san_affinities.tbl_prem_Emit_Agrupada_snapshot bs

        INNER JOIN formatos_gatilho_ultima_versao fmt
            ON CAST(bs.APOLICE AS STRING)
             = CAST(fmt.START AS STRING)

        WHERE bs.data_snapshot = DATE '{data_snapshot_input}'

        GROUP BY
            CAST(bs.APOLICE AS STRING),

            CONCAT(
                CAST(bs.APOLICE AS STRING),
                CASE
                    WHEN CAST(bs.APOLICE AS STRING)
                             IN ({apolices_zews_sql})
                         AND TO_DATE(bs.DTCOMP) >= DATE '2025-01-01'
                        THEN '2'
                    ELSE '1'
                END
            ),

            bs.DTCOMP
        """
    )

    df_emissao = spark.table(tabela_emissao)

    # ============================================================
    # EXPOSTO CARTEIRA
    # ============================================================

    tabela_exposto = "san_affinities.exposto_gatilho"

    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {tabela_exposto} AS

        WITH formatos_gatilho_ultima_versao AS (
            SELECT
                START,
                CAST(VERSAO AS INT) AS VERSAO
            FROM (
                SELECT
                    START,
                    VERSAO,
                    MOSTRAR_GATILHO,

                    ROW_NUMBER() OVER (
                        PARTITION BY START
                        ORDER BY CAST(VERSAO AS INT) DESC
                    ) AS rn

                FROM san_affinities.formatos_gatilho_recarga

                WHERE MOSTRAR_GATILHO = 'SIM'
            )
            WHERE rn = 1
        ),

        exposto AS (
            SELECT
                CAST(bs.APOLICE AS STRING) AS APOLICE,

                CONCAT(
                    CAST(bs.APOLICE AS STRING),
                    CASE
                        WHEN CAST(bs.APOLICE AS STRING)
                                 IN ({apolices_zews_sql})
                             AND TO_DATE(bs.DT_COMP_NEP)
                                 >= DATE '2025-01-01'
                            THEN '2'
                        ELSE '1'
                    END
                ) AS CHAVE,

                bs.DT_COMP_NEP AS DT_COMP,

                SUM(bs.IBNR) AS IBNR_EXPOSTO,
                SUM(bs.NEP_COMP) AS NEP

            FROM
                san_affinities
                .gold_fato_tbl_premio_ganho_afft_snapshot bs

            INNER JOIN formatos_gatilho_ultima_versao fmt
                ON CAST(bs.APOLICE AS STRING)
                 = CAST(fmt.START AS STRING)

            WHERE bs.data_snapshot = DATE '{data_snapshot_input}'

            GROUP BY
                CAST(bs.APOLICE AS STRING),

                CONCAT(
                    CAST(bs.APOLICE AS STRING),
                    CASE
                        WHEN CAST(bs.APOLICE AS STRING)
                                 IN ({apolices_zews_sql})
                             AND TO_DATE(bs.DT_COMP_NEP)
                                 >= DATE '2025-01-01'
                            THEN '2'
                        ELSE '1'
                    END
                ),

                bs.DT_COMP_NEP
        )

        SELECT DISTINCT
            exp.APOLICE,
            exp.CHAVE,
            exp.DT_COMP,
            exp.NEP,
            exp.IBNR_EXPOSTO,
            pct.PRO_LABORE,
            pct.AGENCIAMENTO,
            pct.CORRETAGEM,
            pct.COMISSAO

        FROM exposto exp

        LEFT JOIN san_affinities.exposto_percentuais_gatilho pct
            ON exp.CHAVE = pct.CHAVE
           AND exp.DT_COMP = pct.DT_COMP
        """
    )

    df_exposto = spark.table(tabela_exposto)

    # ============================================================
    # SINISTRO CARTEIRA
    # ============================================================

    tabela_sinistro = "san_affinities.sinistro_gatilho"

    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {tabela_sinistro} AS

        WITH formatos_gatilho_ultima_versao AS (
            SELECT
                START,
                CAST(VERSAO AS INT) AS VERSAO
            FROM (
                SELECT
                    START,
                    VERSAO,

                    ROW_NUMBER() OVER (
                        PARTITION BY START
                        ORDER BY CAST(VERSAO AS INT) DESC
                    ) AS rn

                FROM san_affinities.formatos_gatilho_recarga

                WHERE MOSTRAR_GATILHO = 'SIM'
            )
            WHERE rn = 1
        )

        SELECT
            CAST(bs.APOLICE_AJUSTADA AS STRING) AS APOLICE,

            CONCAT(
                CAST(bs.APOLICE_AJUSTADA AS STRING),
                CASE
                    WHEN CAST(bs.APOLICE_AJUSTADA AS STRING)
                             IN ({apolices_zews_sql})
                         AND TO_DATE(bs.DT_OCORRENCIA)
                             >= DATE '2025-01-01'
                        THEN '2'
                    ELSE '1'
                END
            ) AS CHAVE,

            bs.DT_OCORRENCIA,

            SUM(bs.CUSTO_TOTAL) AS CUSTO_TOTAL,
            SUM(bs.QTD_SIN) AS QTD_SIN_OCOR,
            SUM(bs.QTD_SIN_AVI) AS QTD_SIN_AVIS

        FROM
            san_affinities
            .gold_fato_tbl_addin_sinistro_carteira_afft_snapshot bs

        INNER JOIN formatos_gatilho_ultima_versao fmt
            ON CAST(bs.APOLICE_AJUSTADA AS STRING)
             = CAST(fmt.START AS STRING)

        WHERE bs.TIPO_RISCO = 'DIRETO'
          AND bs.REAL_ESTIMADO = 'ESTIMADO'
          AND bs.data_snapshot = DATE '{data_snapshot_input}'

        GROUP BY
            CAST(bs.APOLICE_AJUSTADA AS STRING),

            CONCAT(
                CAST(bs.APOLICE_AJUSTADA AS STRING),
                CASE
                    WHEN CAST(bs.APOLICE_AJUSTADA AS STRING)
                             IN ({apolices_zews_sql})
                         AND TO_DATE(bs.DT_OCORRENCIA)
                             >= DATE '2025-01-01'
                        THEN '2'
                    ELSE '1'
                END
            ),

            bs.DT_OCORRENCIA
        """
    )

    df_sinistro = spark.table(tabela_sinistro)

    # ============================================================
    # TRATAMENTO DA EMISSÃO
    # ============================================================

    df_emissao_base = (
        df_emissao
        .withColumnRenamed("Apolice", "APOLICE")
        .withColumnRenamed("DT_COMP", "dt_emissao")
        .withColumn(
            "APOLICE",
            F.col("APOLICE").cast("string")
        )
    )

    # ============================================================
    # TRATAMENTO DO EXPOSTO
    # ============================================================

    df_exposto_base = (
        df_exposto
        .withColumnRenamed("Apolice", "APOLICE")
        .withColumnRenamed("DT_COMP", "dt_nep")
        .withColumn(
            "APOLICE",
            F.col("APOLICE").cast("string")
        )
        .withColumn("nep_carteira",F.col("NEP"))
        .select(
            "CHAVE",
            "dt_nep",
            "ibnr_exposto",
            "PRO_LABORE",
            "AGENCIAMENTO",
            "CORRETAGEM",
            "COMISSAO",
            "NEP",
            'nep_carteira'
        )
    )

    # ============================================================
    # TRATAMENTO DO SINISTRO
    # ============================================================

    df_sinistro_base = (
        df_sinistro
        .withColumn(
            "APOLICE",
            F.col("APOLICE").cast("string")
        )
        .withColumn(
            "dt_ocorrencia",
            F.date_format(
                F.col("DT_OCORRENCIA"),
                "yyyyMM"
            ).cast("int")
        )
        .groupBy(
            "CHAVE",
            "dt_ocorrencia"
        )
        .agg(
            F.sum("CUSTO_TOTAL").alias("sinistro"),
            F.sum("QTD_SIN_OCOR").alias("qtd_sin_ocor"),
            F.sum("QTD_SIN_AVIS").alias("qtd_sin_avis"),
        )
    )

    # ============================================================
    # NORMALIZAÇÃO DAS DATAS
    # ============================================================

    df_emissao_base = utils_gatilho.normalizar_dt_comp(
        df_emissao_base,
        "dt_emissao",
    )

    df_exposto_base = utils_gatilho.normalizar_dt_comp(
        df_exposto_base,
        "dt_nep",
    )

    df_sinistro_base = utils_gatilho.normalizar_dt_comp(
        df_sinistro_base,
        "dt_ocorrencia",
    )

    return (
        df_emissao_base,
        df_exposto_base,
        df_sinistro_base,
    )


def validar_bases_carteira(
    df_emissao_base,
    df_exposto_base,
    df_sinistro_base):

    utils_gatilho.checar_duplicidade(
        df_emissao_base,
        "df_emissao_base",
        ["CHAVE", "dt_emissao"],
    )

    utils_gatilho.checar_duplicidade(
        df_exposto_base,
        "df_exposto_base",
        ["CHAVE", "dt_nep"],
    )

    utils_gatilho.checar_duplicidade(
        df_sinistro_base,
        "df_sinistro_base",
        ["CHAVE", "dt_ocorrencia"],
    )


def carregar_bases_auxiliares(spark):
    # ============================================================
    # ASSISTÊNCIA
    # ============================================================
    df_assist_join = transformation.preparar_custos_mensais(
        spark=spark,
        tabela="hive_metastore.san_affinities.df_assistencias_consolidado",
        prefixo="assist_",
        coluna_valor="valor_assist",
    )

    # ============================================================
    # OUTROS CUSTOS
    # ============================================================
    df_outros_custos_join = transformation.preparar_custos_mensais(
        spark=spark,
        tabela="hive_metastore.san_affinities.df_outros_custos_consolidado",
        prefixo="outros_custos_",
        coluna_valor="valor_outros_custos",
    )

    # ============================================================
    # QBE
    # ============================================================
    df_qbe_base = (
        spark.table("san_affinities.tb_valor_qbe_recarga")
        .select(
            F.trim(
                F.col("chave_join").cast("string")
            ).alias("chave_join"),
            F.col("mes").cast("int").alias("ano_mes_join"),
            F.col("premio_ganho").cast("double").alias(
                "premio_ganho_qbe"
            ),
            F.col("sinistro").cast("double").alias(
                "sinistro_qbe"
            ),
        )
    )

    df_qbe_base = utils_gatilho.colunas_em_snake_case(
        df_qbe_base
    )

    # ============================================================
    # ULAE zurich
    # ============================================================
    df_ulae_zurich = (
        spark.table("san_affinities.tb_calc_ulae_zurich").select(
            "parceiro",
            "data",
            F.col("valor_ulae").alias("valor_ulae_mensal_input")
        )
    )

    return (
        df_assist_join,
        df_outros_custos_join,
        df_qbe_base,
        df_ulae_zurich
    )


def carregar_bases_zews(spark):

    # ============================================================
    # EMISSÃO ZEWS
    # ============================================================
    df_emissao_zews = spark.sql("""
        WITH base AS (
            SELECT
                CHAVE,
                DT_CONTABIL AS dt_emissao,
                PREMIO_LIQ_IOF,
                'GE' AS ORIGEM_GATILHO
            FROM san_affinities.base_emissao_ge_agrupada
            WHERE status_transacao = 'EMISSÃO'

            UNION ALL

            SELECT
                CHAVE,
                DT_CONTABIL AS dt_emissao,
                PREMIO_LIQ_IOF,
                'RFD' AS ORIGEM_GATILHO
            FROM san_affinities.base_emissao_rfd_agrupada
            WHERE status_transacao = 'EMISSÃO'

            UNION ALL

            SELECT
                CHAVE,
                DT_CONTABIL AS dt_emissao,
                PREMIO_LIQ_IOF,
                'OTHERS' AS ORIGEM_GATILHO
            FROM san_affinities.base_emissao_others_agrupada
            WHERE status_transacao = 'EMISSÃO'
        )

        SELECT
            CHAVE,
            DT_EMISSAO,
            SUM(PREMIO_LIQ_IOF) AS PREMIO_LIQ_IOF
        FROM base
        GROUP BY
            CHAVE,
            DT_EMISSAO
    """)

    # ============================================================
    # EXPOSTO ZEWS
    # ============================================================
    df_exposto_zews = spark.sql("""
        SELECT
            CHAVE,
            DT_COMP AS dt_nep,
            SUM(AGENCIAMENTO_DIFERIDO) AS AGENCIAMENTO_DIFERIDO,
            SUM(CORRETAGEM_DIFERIDO) AS CORRETAGEM_DIFERIDO,
            SUM(PROLABORE_DIFERIDO) AS PROLABORE_DIFERIDO,
            SUM(NEP_COMP) AS NEP_COMP,
            SUM(IBNR) AS IBNR,
            'GE' AS ORIGEM_GATILHO
        FROM san_affinities.base_calc_exposto_ge_final
        GROUP BY 1, 2

        UNION ALL

        SELECT
            CHAVE,
            DT_COMP AS dt_nep,
            SUM(AGENCIAMENTO_DIFERIDO),
            SUM(CORRETAGEM_DIFERIDO),
            SUM(PROLABORE_DIFERIDO),
            SUM(NEP_COMP),
            SUM(IBNR),
            'RFD'
        FROM san_affinities.base_calc_exposto_rfd_final
        GROUP BY 1, 2

        UNION ALL

        SELECT
            CHAVE,
            DT_COMP AS dt_nep,
            SUM(AGENCIAMENTO_DIFERIDO),
            SUM(CORRETAGEM_DIFERIDO),
            SUM(PROLABORE_DIFERIDO),
            SUM(NEP_COMP),
            SUM(IBNR),
            'OTHERS'
        FROM san_affinities.base_calc_exposto_others_final
        GROUP BY 1, 2
    """)

    # ============================================================
    # SINISTRO ZEWS
    # ============================================================
    df_sinistro_zews = spark.sql("""
        SELECT
            APOLICE,
            CHAVE,
            CUSTO_TOTAL,
            QTD_SIN,
            QTD_SIN_AVI,
            DT_AVISO,
            DT_OCORRENCIA AS dt_ocorrencia,
            'GE' AS ORIGEM_GATILHO
        FROM san_affinities.sinistros_ge

        UNION ALL

        SELECT
            APOLICE,
            CHAVE,
            CUSTO_TOTAL,
            QTD_SIN,
            QTD_SIN_AVI,
            DT_AVISO,
            DT_OCORRENCIA AS dt_ocorrencia,
            'RFD' AS ORIGEM_GATILHO
        FROM san_affinities.sinistros_rfd

        UNION ALL

        SELECT
            APOLICE,
            CHAVE,
            CUSTO_TOTAL,
            QTD_SIN,
            QTD_SIN_AVI,
            DT_AVISO,
            DT_OCORRENCIA AS dt_ocorrencia,
            'OTHERS' AS ORIGEM_GATILHO
        FROM san_affinities.sinistros_others
    """)

    # ============================================================
    # PADRONIZAÇÃO DAS COLUNAS
    # ============================================================
    df_emissao_zews = utils_gatilho.renomear_cols_zews(df_emissao_zews)
    df_exposto_zews = utils_gatilho.renomear_cols_zews(df_exposto_zews)
    df_sinistro_zews = utils_gatilho.renomear_cols_zews(df_sinistro_zews)

    # ============================================================
    # TRATAMENTO EMISSÃO
    # ============================================================
    df_emissao_zews = (
        df_emissao_zews
        .withColumn(
            "chave_zews",
            F.col("chave_zews").cast("string")
        )
    )

    # ============================================================
    # TRATAMENTO EXPOSTO
    # ============================================================
    df_exposto_zews = (
        df_exposto_zews
        .withColumn(
            "chave_zews",
            F.col("chave_zews").cast("string")
        )
    )

    # ============================================================
    # TRATAMENTO SINISTRO
    # ============================================================
    df_sinistro_zews = (
        df_sinistro_zews
        .withColumn(
            "chave_zews",
            F.col("chave_zews").cast("string")
        )
        .withColumn(
            "dt_ocorrencia_zews",
            F.trunc(
                F.to_date(F.col("dt_ocorrencia_zews")),
                "month"
            )
        )
        .groupBy(
            "chave_zews",
            "dt_ocorrencia_zews"
        )
        .agg(
            F.sum("custo_total_zews").alias(
                "sinistro_zews"
            ),
            F.sum("qtd_sin_zews").alias(
                "qtd_sin"
            ),
            F.sum("qtd_sin_avi_zews").alias(
                "qtd_sin_avi"
            )
        )
    )

    # ============================================================
    # SNAKE CASE
    # ============================================================
    df_emissao_zews = utils_gatilho.colunas_em_snake_case(df_emissao_zews)
    df_exposto_zews = utils_gatilho.colunas_em_snake_case(df_exposto_zews)
    df_sinistro_zews = utils_gatilho.colunas_em_snake_case(df_sinistro_zews)


    return (
        df_emissao_zews,
        df_exposto_zews,
        df_sinistro_zews,
    )


def carregar_formatos_gatilho_origem(spark):
    return spark.table("san_affinities.formatos_gatilho_recarga")


def carregar_fatores_ibnr_gatilho(spark):
    return spark.table("san_affinities.fatores_ibnr_atuarial_gatilho")