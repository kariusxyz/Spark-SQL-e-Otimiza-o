# Semana 10 — Spark SQL e Otimização

## Objetivo
Diagnosticar um plano de execução com `explain()`, reconhecer UDF/joins/shuffle, otimizar com funções nativas + column pruning + broadcast e medir o efeito de `cache()` quando um DataFrame caro é reutilizado.

## Requisitos
- Python 3.10+
- Java 17+
- PySpark 4.2.0 (`python -m pip install -r requirements.txt`)
- VS Code

## Ordem da prática
1. `python check_env.py`
2. `python gerar_dados.py`
3. `python pipeline_lento.py`
4. Preencher TODO 1, 2 e 3 de `pipeline_desafio.py`, executando após cada etapa.
5. `python cache_demo.py`
6. Preencher o caderno, anexar prints e gerar PDF.

## Plano B para PC mais fraco
`python gerar_dados.py --linhas 300000`

## Evidências que o PDF deve mostrar
- explain baseline
- Spark UI baseline
- explain otimizado
- Spark UI otimizada
- cache/tempos
- comparação e conclusão

## Erros comuns
- `python` não encontrado: tente `py` no Windows.
- `java` não encontrado: instale Java 17+ e configure PATH/JAVA_HOME.
- `ModuleNotFoundError: pyspark`: rode `python -m pip install -r requirements.txt`.
- `data/pedidos` inexistente: rode `gerar_dados.py` primeiro.
- Spark UI não abre em 4040: use a URL que o script imprime; ela pode usar outra porta.
- Script “parou”: ele pode estar aguardando ENTER para você tirar o print da Spark UI.
