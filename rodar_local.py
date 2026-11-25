import csv
import runpy
from pathlib import Path

from pyspark.sql import SparkSession

from tests.bases_ficticias import DATA_SNAPSHOT, CatalogoFicticio, criar_bases_ficticias


def main():
    raiz = Path(__file__).resolve().parent
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("gatilho-local")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    try:
        catalogo = CatalogoFicticio(spark, criar_bases_ficticias(spark))
        resultado = runpy.run_path(
            str(raiz / "run_gatilho.py"),
            init_globals={
                "spark": catalogo,
                "data_snapshot_input": DATA_SNAPSHOT.isoformat(),
                "publicar_tabela": catalogo.publicar_tabela,
                "publicar_ibnr": False,
            },
        )
        saida = raiz / "files" / "saida_local"
        saida.mkdir(exist_ok=True)
        for assunto, df in resultado["bases"].items():
            linhas = df.collect()
            caminho = saida / f"gatilho_{assunto}.csv"
            with caminho.open("w", encoding="utf-8", newline="") as arquivo:
                gravador = csv.writer(arquivo)
                gravador.writerow(df.columns)
                gravador.writerows(linhas)
            print(f"{caminho.name}: {len(linhas)} linhas em {saida}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
