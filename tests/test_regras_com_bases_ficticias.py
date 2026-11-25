import unittest
from datetime import date

from pyspark.sql import SparkSession

import cargas_gatilho
import transformation
from tests.bases_ficticias import (
    APOLICE_CARTEIRA,
    APOLICES_ZEWS,
    CHAVE_QBE,
    DATA_SNAPSHOT,
    CatalogoFicticio,
    criar_bases_ficticias,
)
from regras_especificas.havan import Havan
from regras_especificas.mercantil import Mercantil
from regras_especificas.sky import Sky


def bases_mercantil(spark):
    """Cria 15 meses de sobreposição e um mês posterior com valores fictícios."""
    safra_01 = "667791861252"
    safra_02 = "667791861251"
    outra_chave = "000000000001"
    meses = [date(2024, mes, 1) for mes in range(6, 13)]
    meses += [date(2025, mes, 1) for mes in range(1, 10)]

    premio = spark.createDataFrame(
        [(safra_01, mes.year * 100 + mes.month, 100.0) for mes in meses[:-1]],
        "chave_join string, ano_mes_join int, premio_ganho_mercantil double",
    )
    exposto = spark.createDataFrame(
        [
            (safra_02, safra_02, mes.year * 100 + mes.month, mes, safra_02[:-1], 350.0)
            for mes in meses
        ],
        "chave string, chave_join string, ano_mes_join int, dt_nep date, apolice string, nep double",
    )
    emissao = spark.createDataFrame(
        [
            (safra_02, date(2024, 5, 1), safra_02[:-1], 80.0),
            (safra_02, meses[0], safra_02[:-1], 999.0),
            (outra_chave, meses[0], outra_chave[:-1], 42.0),
        ],
        "chave string, dt_emissao date, apolice string, gwp double",
    )
    return exposto, emissao, premio


class TestRegrasComBasesFicticias(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("gatilho-testes-locais")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_cargas_de_origem(self):
        bases = criar_bases_ficticias(self.spark)
        self.assertEqual(len(bases), 22)
        catalogo = CatalogoFicticio(self.spark, bases)
        apolices_sql = ", ".join(f"'{apolice}'" for apolice in APOLICES_ZEWS.values())

        self.assertEqual(cargas_gatilho.gerar_rls_gatilho(catalogo).count(), 7)
        fatores = cargas_gatilho.carregar_fatores_ibnr_gatilho(catalogo)
        self.assertEqual(fatores.first().fator_0, 1.0)

        percentuais = cargas_gatilho.carregar_percentuais_exposto(catalogo, apolices_sql)
        self.assertEqual(
            {r.CHAVE: r.COMISSAO for r in percentuais.collect()},
            {apolice + "2": 0.2 for apolice in APOLICES_ZEWS.values()},
        )
        carteira = cargas_gatilho.carregar_bases_carteira(catalogo, DATA_SNAPSHOT, apolices_sql)
        for df, campo, esperado in zip(carteira, ("GWP", "NEP", "sinistro"), (500, 400, 120)):
            self.assertEqual(df.filter(df.CHAVE == APOLICE_CARTEIRA + "1").first()[campo], esperado)

        zews = cargas_gatilho.carregar_bases_zews(catalogo)
        chaves_zews = {apolice + "2" for apolice in APOLICES_ZEWS.values()}
        for df in zews:
            self.assertEqual({r.chave_zews for r in df.select("chave_zews").collect()}, chaves_zews)

        assist, outros, qbe, ulae = cargas_gatilho.carregar_bases_auxiliares(catalogo)
        self.assertEqual({r.ano_mes_join: r.valor_assist for r in assist.collect()}, {202501: 12, 202502: 13})
        self.assertEqual({r.ano_mes_join: r.valor_outros_custos for r in outros.collect()}, {202501: 8, 202502: 9})
        self.assertEqual(qbe.first().chave_join, CHAVE_QBE)
        self.assertEqual(ulae.first().valor_ulae_mensal_input, 25.0)
        farol = cargas_gatilho.carregar_comissao_farol(catalogo)
        self.assertEqual(farol.filter(farol.chave == APOLICE_CARTEIRA + "1").first().comiss_total, 0.20)

    def test_mercantil_exposto_e_emissao(self):
        exposto, emissao = Mercantil().aplicar_safras(*bases_mercantil(self.spark))
        nep = {(r.chave, r.dt_nep): r.nep for r in exposto.collect()}
        gwp = {(r.chave, r.dt_emissao): r.gwp for r in emissao.collect()}
        safra_01 = "667791861252"
        safra_02 = "667791861251"
        junho = date(2024, 6, 1)
        setembro = date(2025, 9, 1)

        self.assertEqual(len(nep), 31)
        self.assertEqual(len(gwp), 33)
        self.assertEqual(nep[safra_01, junho], 100.0)
        self.assertEqual(nep[safra_02, junho], 250.0)
        self.assertEqual(nep[safra_02, setembro], 350.0)
        self.assertTrue(all(gwp[chave_mes] == valor for chave_mes, valor in nep.items()))
        self.assertEqual(gwp[safra_02, date(2024, 5, 1)], 80.0)
        self.assertEqual(gwp["000000000001", junho], 42.0)

    def test_havan_renovacao(self):
        base = self.spark.createDataFrame(
            [
                ("017791863361", date(2024, 12, 31)),
                ("017791863361", date(2025, 1, 1)),
                ("000000000001", date(2025, 1, 1)),
            ],
            "chave string, dt_emissao date",
        )
        esperado = [
            ("000000000001", date(2025, 1, 1)),
            ("017791863361", date(2024, 12, 31)),
            ("017791863362", date(2025, 1, 1)),
        ]
        for aplicar in (Havan().ajustar_chave_renovacao, transformation.ajustar_chave_renovacao_havan):
            resultado = aplicar(base, "dt_emissao", data_corte="2025-01-01")
            self.assertEqual(sorted(tuple(r) for r in resultado.collect()), esperado)

    def test_sky_periodos(self):
        base = self.spark.createDataFrame(
            [
                ("000000000010", date(2023, 5, 31)),
                ("000000000010", date(2023, 6, 1)),
                ("000000000020", date(2023, 6, 1)),
            ],
            "chave string, dt_nep date",
        )
        formatos = self.spark.createDataFrame(
            [("000000000010", "SKY"), ("000000000010", "SKY")],
            "chave string, grupo_geral string",
        )
        esperado = ["SKY", "SKY - Renovação 2023", None]
        for aplicar in (Sky().classificar_periodo, transformation.classificar_periodo_sky):
            resultado = aplicar(base, "dt_nep", formatos).orderBy("chave", "dt_nep")
            self.assertEqual([r.periodo_sky for r in resultado.collect()], esperado)
