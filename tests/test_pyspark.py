from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# INISIALISASI SPARK
# ============================================================
print("=" * 50)
print("TEST PYSPARK INGESTION")
print("=" * 50)

spark = SparkSession.builder \
    .appName("QA_BigData_Sophie") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("✅ SparkSession berhasil dibuat!")
print()

# ============================================================
# LOAD DATA
# ============================================================
df = spark.read.csv(
    "bigdata-project/data/raw/harga_pangan_3provinsi.csv",
    header=True,
    inferSchema=True
)

print(f"✅ Data berhasil di-load ke PySpark!")
print(f"   Total rows : {df.count()}")
print(f"   Total kolom: {len(df.columns)}")
print()

# ============================================================
# CEK SCHEMA
# ============================================================
print("Schema data:")
df.printSchema()

# ============================================================
# CEK DATA
# ============================================================
print("Preview data:")
df.show(5)

# ============================================================
# CEK MISSING VALUES
# ============================================================
print("Cek missing values per kolom:")
for col in df.columns:
    missing = df.filter(F.col(col).isNull()).count()
    status = "✅" if missing == 0 else "❌"
    print(f"   {status} {col}: {missing} missing")

print()
print("✅ PySpark ingestion test selesai!")

spark.stop()