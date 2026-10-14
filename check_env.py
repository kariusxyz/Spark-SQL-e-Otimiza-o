from __future__ import annotations
import platform, subprocess, sys

print("=== CHECK DE AMBIENTE · SEMANA 10 ===")
print("Python:", sys.version.split()[0])
if sys.version_info < (3, 10):
    raise SystemExit("ERRO: PySpark 4.2 requer Python 3.10 ou superior.")

try:
    proc = subprocess.run(["java", "-version"], capture_output=True, text=True, check=False)
    texto = (proc.stderr or proc.stdout).splitlines()
    print("Java:", texto[0] if texto else "não identificado")
except FileNotFoundError:
    raise SystemExit("ERRO: Java não encontrado. Instale Java 17+ e confira JAVA_HOME/PATH.")

try:
    import pyspark
    print("PySpark:", pyspark.__version__)
except Exception as exc:
    raise SystemExit(f"ERRO: PySpark não importou: {exc}\nRode: python -m pip install -r requirements.txt")

from pyspark.sql import SparkSession
spark = (SparkSession.builder
         .appName("Semana10Check")
         .master("local[2]")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
print("Spark:", spark.version)
print("Spark UI:", spark.sparkContext.uiWebUrl or "será criada quando houver job ativo")
print("SO:", platform.platform())
print("AMBIENTE OK")
spark.stop()
