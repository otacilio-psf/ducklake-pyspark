import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.ducklake_spark_connector import DuckLakeDataSource
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DuckLakeSparkDemo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.dataSource.register(DuckLakeDataSource)

df = spark.read.format("ducklake").option("table", "nl_train_stations").load()

df.show()

spark.stop()