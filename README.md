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
| [`regras_especificas/`](regras_especificas/) | Contém a classe base `RegraParceiro` e as implementações `Havan` e `Sky`. |

## Fluxo do pipeline

1. Carrega os formatos do gatilho, os dados de carteira e ZEWS e as fontes auxiliares para a data de referência definida em `data_snapshot_input`.
2. Consolida emissão, exposto e sinistro e aplica regras como a renovação da Havan, a classificação de períodos da Sky e os cortes por agrupado.
3. Filtra as bases pelas chaves dos formatos e publica `gatilho_emissao`, `gatilho_exposto` e `gatilho_sinistro` no schema `san_affinities`. O script também recria a tabela Delta `tb_ibnr_input` e insere valores de IBNR atuarial definidos no código.

## Ambiente e execução

O script principal depende de uma sessão `spark` já disponível, suporte a Delta, PySpark, pandas e python-dateutil. A execução precisa de acesso às tabelas de origem e permissão de escrita nos destinos. As bases finais são gravadas em modo `overwrite`.

