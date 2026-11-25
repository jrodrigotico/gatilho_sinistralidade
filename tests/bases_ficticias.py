import csv
import re
from datetime import date
from pathlib import Path


DATA_SNAPSHOT = date(2026, 11, 23)
APOLICE_CARTEIRA = "00000000001"
APOLICES_ZEWS = {"ge": "00000000002", "rfd": "00000000003", "others": "00000000004"}
CHAVE_QBE = "000000000051"


def criar_bases_ficticias(spark):
    """Carrega os CSVs locais com tipos explícitos para datas, valores e chaves."""
    pasta = Path(__file__).resolve().parents[1] / "files"
    with (pasta / "schemas.csv").open(encoding="utf-8", newline="") as arquivo:
        schemas = {linha["tabela"]: linha["schema"] for linha in csv.DictReader(arquivo)}

    return {
        f"san_affinities.{tabela}": spark.read.csv(
            str(pasta / f"{tabela}.csv"),
            schema=schema,
            header=True,
            enforceSchema=False,
            mode="FAILFAST",
        )
        for tabela, schema in schemas.items()
    }


class CatalogoFicticio:
    """Encaminha as leituras e os CTAS das cargas para views da sessão local."""

    _nome_sql = re.compile(r"(?:hive_metastore\s*\.\s*)?san_affinities\s*\.\s*(\w+)", re.I)
    _ctas = re.compile(r"\s*CREATE OR REPLACE TABLE (\w+) AS\b", re.I)

    def __init__(self, spark, bases):
        self.spark = spark
        for nome, df in bases.items():
            df.createOrReplaceTempView(self._nome_sql.sub(self._nome_view, nome))

    @staticmethod
    def _nome_view(match):
        return "ficticio_" + match[1].lower()

    def table(self, nome):
        return self.spark.table(self._nome_sql.sub(self._nome_view, nome))

    def createDataFrame(self, dados, schema):
        return self.spark.createDataFrame(dados, schema)

    def publicar_tabela(self, df, nome):
        df.createOrReplaceTempView(self._nome_sql.sub(self._nome_view, nome))

    def sql(self, consulta):
        consulta = self._nome_sql.sub(self._nome_view, consulta)
        if (ctas := self._ctas.match(consulta)):
            df = self.spark.sql(consulta[ctas.end():])
            df.createOrReplaceTempView(ctas[1])
            return df
        return self.spark.sql(consulta)
