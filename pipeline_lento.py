from __future__ import annotations
import json, time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

Path("output").mkdir(exist_ok=True)

spark = (SparkSession.builder
    .appName("Semana10Baseline")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

@F.udf(returnType=StringType())
def categoria_valor(total):
    if total is None:
        return "desconhecido"
    if total > 1000:
        return "alto"
    if total > 100:
        return "medio"
    return "baixo"

pedidos = spark.read.parquet("data/pedidos")
produtos = spark.read.parquet("data/produtos")

# BASELINE DIDÁTICO: escolhas ruins de propósito.
resultado = (pedidos
    .join(produtos, "product_id", "inner")
    .withColumn("faixa", categoria_valor(F.col("total")))
    .filter(F.col("faixa") == "alto")
    .groupBy("product_name", "category")
    .agg(F.round(F.sum("total"), 2).alias("receita"), F.count("order_id").alias("pedidos")))

print("\n=== EXPLAIN BASELINE (FORMATTED) ===")
resultado.explain("formatted")
print("\nProcure: BatchEvalPython, SortMergeJoin/Exchange e filtros não empurrados até o scan.")
print("Spark UI:", spark.sparkContext.uiWebUrl)
input("Abra a Spark UI e prepare o print. Pressione ENTER para executar o baseline...")

t0 = time.perf_counter()
(resultado.write.mode("overwrite").parquet("output/baseline"))
elapsed = time.perf_counter() - t0

metricas = {
    "modo": "baseline",
    "segundos": round(elapsed, 3),
    "spark_version": spark.version,
    "ui": spark.sparkContext.uiWebUrl,
    "auto_broadcast": spark.conf.get("spark.sql.autoBroadcastJoinThreshold"),
    "aqe": spark.conf.get("spark.sql.adaptive.enabled"),
}
Path("output/baseline_metricas.json").write_text(json.dumps(metricas, indent=2), encoding="utf-8")
print("\nBASELINE concluído em", metricas["segundos"], "s")
print("Métricas: output/baseline_metricas.json")
input("Faça o print da Spark UI (Stages/SQL) e pressione ENTER para encerrar...")
spark.stop()
