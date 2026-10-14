from __future__ import annotations
import json, time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

Path("output").mkdir(exist_ok=True)

spark = (SparkSession.builder
    .appName("Semana10Otimizado")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

pedidos = spark.read.parquet("data/pedidos")
produtos = spark.read.parquet("data/produtos")

def preparar_pedidos(df):
    """TODO 1: filtro nativo + column pruning antes do join."""
    return None

def juntar_produtos(df, dim_produtos):
    """TODO 2: broadcast explícito apenas da dimensão pequena."""
    return None

def agregar(df):
    """TODO 3: agregação final."""
    return None

p = preparar_pedidos(pedidos)
if p is None:
    raise SystemExit("TODO 1 pendente: complete preparar_pedidos(df). Veja o caderno.")

j = juntar_produtos(p, produtos)
if j is None:
    raise SystemExit("TODO 2 pendente: complete juntar_produtos(...). Veja o caderno.")

resultado = agregar(j)
if resultado is None:
    raise SystemExit("TODO 3 pendente: complete agregar(df). Veja o caderno.")

print("\n=== EXPLAIN OTIMIZADO (FORMATTED) ===")
resultado.explain("formatted")
print("\nProcure: PushedFilters, ReadSchema reduzido e BroadcastHashJoin/BroadcastExchange.")
print("Spark UI:", spark.sparkContext.uiWebUrl)
input("Abra a Spark UI e prepare o print. Pressione ENTER para executar a versão otimizada...")

t0 = time.perf_counter()
(resultado.write.mode("overwrite").parquet("output/otimizado"))
elapsed = time.perf_counter() - t0

metricas = {
    "modo": "otimizado",
    "segundos": round(elapsed, 3),
    "spark_version": spark.version,
    "ui": spark.sparkContext.uiWebUrl,
    "aqe": spark.conf.get("spark.sql.adaptive.enabled"),
}
Path("output/otimizado_metricas.json").write_text(json.dumps(metricas, indent=2), encoding="utf-8")
print("\nOTIMIZADO concluído em", metricas["segundos"], "s")
print("Métricas: output/otimizado_metricas.json")
input("Faça o print da Spark UI e pressione ENTER para encerrar...")
spark.stop()
