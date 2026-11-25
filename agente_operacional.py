"""Executa o pipeline local e resume as verificações de duplicidade."""

import argparse
from pathlib import Path
import re
import subprocess
import sys


PROJETO = Path(__file__).resolve().parent
VERIFICACAO = re.compile(
    r"^(?P<nome>.+): (?:(?P<quantidade>\d+) chaves duplicadas|sem duplicidades)$",
    re.MULTILINE,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--detalhes",
        action="store_true",
        help="Mostra a saída completa do pipeline, que pode incluir registros.",
    )
    args = parser.parse_args()

    resultado = subprocess.run(
        [sys.executable, "-X", "utf8", str(PROJETO / "orchestrator.py")],
        cwd=PROJETO,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )

    if args.detalhes:
        print(resultado.stdout, end="")
        print(resultado.stderr, end="", file=sys.stderr)

    verificacoes = list(VERIFICACAO.finditer(resultado.stdout))
    print("Verificações de duplicidade")
    for verificacao in verificacoes:
        quantidade = int(verificacao["quantidade"] or 0)
        print(f"- {verificacao['nome']}: {quantidade} chaves duplicadas")
    if not verificacoes:
        print("- Nenhum resultado de verificação foi registrado.")

    if resultado.returncode:
        print(
            f"Pipeline interrompido (código {resultado.returncode}). "
            "Use --detalhes para diagnosticar a falha.",
            file=sys.stderr,
        )
        return 1

    if not verificacoes:
        return 1
    if any(verificacao["quantidade"] for verificacao in verificacoes):
        print("Pipeline concluído com alertas de duplicidade.")
        return 2

    print("Pipeline concluído sem alertas de duplicidade nas verificações registradas.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
    