# Gatilho de Sinistralidade

Pipeline em PySpark que consolida bases de emissão, exposto e sinistro para o acompanhamento da sinistralidade. O fluxo combina dados de carteira e ZEWS, aplica regras por parceiro e publica as bases finais em tabelas Delta.

## Estrutura do repositório

| Arquivo ou pasta | Responsabilidade |
| --- | --- |
| [`run_gatilho.py`](run_gatilho.py) | Orquestra a carga, os ajustes por parceiro e a publicação das bases finais. |
| [`cargas_gatilho.py`](cargas_gatilho.py) | Carrega e valida dados de carteira, ZEWS, formatos e fontes auxiliares. |
| [`transformation.py`](transformation.py) | Contém junções por chave e mês, ajustes de prêmio e custos e regras de período e renovação. |
| [`utils_gatilho.py`](utils_gatilho.py) | Reúne funções de normalização de texto, datas e colunas, além de verificações de duplicidade. |
| [`core_gatilho.py`](core_gatilho.py) | Reúne implementações comentadas de cálculos de prêmio, ULAE, IBNR e custo mensal. |
| [`regras_especificas/`](regras_especificas/) | Contém a classe base `RegraParceiro` e as implementações `Havan`, `Sky` e [`Mercantil`](regras_especificas/mercantil.py). |
| [`files/`](files/) | Guarda os CSVs fictícios das 22 tabelas de origem e os tipos das colunas. |
| [`tests/`](tests/) | Contém os testes locais das cargas e das regras dos parceiros. |

## Fluxo do pipeline

1. Carrega os formatos do gatilho, os dados de carteira e ZEWS e as fontes auxiliares para a data de referência definida em `data_snapshot_input`.
2. Consolida emissão, exposto e sinistro e aplica as regras de cada parceiro, incluindo as safras do Mercantil, a renovação da Havan e a classificação de períodos da Sky, além dos cortes por agrupado.
3. Filtra as bases pelas chaves dos formatos e publica `gatilho_emissao`, `gatilho_exposto` e `gatilho_sinistro` no schema `san_affinities`. O script também recria a tabela Delta `tb_ibnr_input` e insere valores de IBNR atuarial definidos no código.

## Safras do Banco Mercantil

Depois de carregar e validar o prêmio ganho, `run_gatilho.py` chama `Mercantil.aplicar_safras`. O método cria a Safra 01 no exposto, ajusta o NEP da Safra 02 no período de sobreposição e reconstrói a emissão das duas safras a partir de junho de 2024, usando o NEP final como GWP.

## Ambiente e execução

O script principal depende de uma sessão `spark` já disponível, suporte a Delta, PySpark, pandas e python-dateutil. A execução precisa de acesso às tabelas de origem e permissão de escrita nos destinos. As bases finais são gravadas em modo `overwrite`.

## Testes locais com bases fictícias

Os 22 CSVs já estão disponíveis em [`files/`](files/). Cada arquivo de dados usa o nome da tabela de origem. Por exemplo, [`tb_valor_qbe_recarga.csv`](files/tb_valor_qbe_recarga.csv) representa `san_affinities.tb_valor_qbe_recarga`. O conjunto inclui os snapshots de carteira, as fontes ZEWS de GE, RFD e OTHERS, os formatos, os fatores de IBNR e as bases auxiliares de Farol, assistência, outros custos, QBE e ULAE Zurich.

1. Abra o CSV que deseja testar no VS Code e edite as linhas. Preserve o cabeçalho, os zeros à esquerda nas chaves e apólices e o formato `AAAA-MM-DD` nas datas. O arquivo [`schemas.csv`](files/schemas.csv) define os tipos usados pelo Spark.
2. Mantenha as chaves e competências consistentes entre as bases que devem se cruzar. O teste das cargas usa o snapshot `2026-11-23`. Se alterar essa data nos CSVs, ajuste também `DATA_SNAPSHOT` em [`tests/bases_ficticias.py`](tests/bases_ficticias.py).
3. Na raiz do repositório, execute o comando abaixo em um ambiente Python com PySpark, pandas e Java disponível.

   ```powershell
   python -m unittest discover -s tests -v
   ```

A bateria verifica as cargas de origem e as regras de Mercantil, Havan e Sky. O teste das cargas usa views temporárias do Spark local, sem acesso ao Databricks. A execução completa de `run_gatilho.py` e as gravações Delta ficam fora dessa cobertura.

