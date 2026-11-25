# Gatilho de sinistralidade

## Fluxo do projeto

- `config.py` define a planilha de entrada e as origens.
- `orchestrator.py` executa formatos com fatores IBNR, RLS e percentuais do exposto em Spark local.
- `pipeline.py` organiza as etapas. `transformation.py` concentra as transformações e `quality.py` contém as verificações de qualidade.
- O README documenta as tabelas previstas para o Power BI. Confirme quais etapas o código realmente executa antes de relatar uma entrega.

## Como desenvolver e validar

- Mantenha os cálculos em PySpark. Preserve tipos decimais, zeros à esquerda de identificadores e as regras de RLS ao editar as etapas.
- Ao alterar junções ou agregações, confira as chaves, a granularidade, as duplicidades e o efeito sobre os totais relevantes.
- Valide a execução local com `python agente_operacional.py` usando as dependências de `requirements.txt` e a planilha configurada.
- Informe quais etapas e verificações foram executadas, os alertas observados e qualquer validação pendente. Em resumos, use contagens agregadas para evitar expor valores de apólice ou acesso.