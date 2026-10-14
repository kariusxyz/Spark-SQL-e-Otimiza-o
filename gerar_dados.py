from __future__ import annotations
import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser(description="Gera dados locais para a prática da Semana 10")
parser.add_argument("--linhas", type=int, default=1_200_000, help="Quantidade de pedidos")
args = parser.parse_args()

Path("data").mkdir(exist_ok=True)

spark = (SparkSession.builder
         .appName("Semana10GerarDados")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "16")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

n = args.linhas
print(f"Gerando {n:,} pedidos...")

pedidos = (spark.range(n)
    .withColumn("order_id", F.col("id") + 1)
    .withColumn("product_id", (F.pmod(F.col("id") * 17, F.lit(200)) + 1).cast("int"))
    .withColumn("customer_id", (F.pmod(F.col("id") * 29, F.lit(75_000)) + 1).cast("int"))
    .withColumn("total", (F.pmod(F.col("id") * 37, F.lit(5000)) + F.lit(10)).cast("double"))
    .withColumn("status", F.when(F.pmod(F.col("id"), F.lit(10)) < 8, F.lit("paid")).otherwise(F.lit("cancelled")))
    .withColumn("mes", (F.pmod(F.col("id"), F.lit(12)) + 1).cast("int"))
    .withColumn("regiao", F.element_at(F.array(*[F.lit(x) for x in ["N", "NE", "CO", "SE", "S"]]), (F.pmod(F.col("id"), F.lit(5)) + 1).cast("int")))
    .withColumn("quantity", (F.pmod(F.col("id"), F.lit(5)) + 1).cast("int"))
    .withColumn("discount", (F.pmod(F.col("id") * 11, F.lit(30)) / 100.0).cast("double"))
    .withColumn("channel", F.when(F.pmod(F.col("id"), F.lit(3)) == 0, "app").when(F.pmod(F.col("id"), F.lit(3)) == 1, "web").otherwise("store"))
    .drop("id"))

produtos = (spark.range(1, 201)
    .withColumnRenamed("id", "product_id")
    .withColumn("product_name", F.concat(F.lit("Produto "), F.lpad(F.col("product_id").cast("string"), 3, "0")))
    .withColumn("category", F.concat(F.lit("Categoria "), (F.pmod(F.col("product_id"), F.lit(12)) + 1)))
    .withColumn("supplier", F.concat(F.lit("Fornecedor "), (F.pmod(F.col("product_id"), F.lit(20)) + 1)))
    .withColumn("active", F.lit(True)))

(pedidos.repartition(16).write.mode("overwrite").parquet("data/pedidos"))
(produtos.coalesce(1).write.mode("overwrite").parquet("data/produtos"))

print("Dados criados:")
print(" - data/pedidos   :", pedidos.count(), "linhas")
print(" - data/produtos  :", produtos.count(), "linhas")
print("Pronto para executar pipeline_lento.py")
spark.stop()
