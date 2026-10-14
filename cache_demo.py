from __future__ import annotations
import json, time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

Path("output").mkdir(exist_ok=True)

spark = (SparkSession.builder
    .appName("Semana10CacheDemo")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

pedidos = spark.read.parquet("data/pedidos")
produtos = spark.read.parquet("data/produtos")

base = (pedidos
    .filter((F.col("status") == "paid") & (F.col("total") > 1000))
    .select("product_id", "total", "mes", "regiao")
    .join(F.broadcast(produtos.select("product_id", "product_name", "category")), "product_id", "inner"))

def medir(label, fn):
    t0 = time.perf_counter(); value = fn(); dt = time.perf_counter()-t0
    print(f"{label}: {dt:.3f}s")
    return round(dt, 3), value

def relatorios(df):
    tempos = {}
    tempos["top_produtos"], _ = medir("Top produtos", lambda: df.groupBy("product_name").agg(F.sum("total")).count())
    tempos["por_mes"], _ = medir("Por mês", lambda: df.groupBy("mes").agg(F.sum("total")).count())
    tempos["por_regiao"], _ = medir("Por região", lambda: df.groupBy("regiao").agg(F.sum("total")).count())
    return tempos

print("\n=== SEM CACHE ===")
sem_cache = relatorios(base)

print("\n=== COM CACHE ===")
base_cache = base.cache()
print("StorageLevel:", base_cache.storageLevel)
materializacao, _ = medir("Materialização do cache", lambda: base_cache.count())
print("Spark UI:", spark.sparkContext.uiWebUrl)
input("Abra a Spark UI. Veja Storage/SQL e pressione ENTER para rodar 3 relatórios usando o cache...")
com_cache = relatorios(base_cache)

metricas = {"sem_cache": sem_cache, "materializacao": materializacao, "com_cache": com_cache,
            "storage_level": str(base_cache.storageLevel)}
Path("output/cache_metricas.json").write_text(json.dumps(metricas, indent=2), encoding="utf-8")
print("\nMétricas: output/cache_metricas.json")
print("IMPORTANTE: compare tendências, não espere o mesmo tempo em máquinas diferentes.")
input("Faça um print da Spark UI/terminal e pressione ENTER para liberar o cache...")
base_cache.unpersist()
spark.stop()
