from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import MapType, StringType

spark = SparkSession.builder.appName("test").getOrCreate()

# Dummy data
# data = [
#     ("REF1", {"r1": "True"}, 2, "L1", 500),
#     ("REF1", {"r1": "False"}, 1, "L2", 300),
#     ("REF1", {"r1": "True"}, 3, "L3", 800),
#     ("REF2", {"r2": "False"}, 1, "L2", 100),
#     ("REF2", {"r2": "True"}, 2, "L1", 200),
# ]

# columns = [
#     "NormalizedReferenceID",
#     "businessrulecheck",
#     "TRADE_VERSION",
#     "releted_link_type_name",
#     "NormalizedTransactionAmunt"
# ]

# df = spark.createDataFrame(data, columns)

# df.show(truncate=False)
spark.read.parquet("sales.parquet")
.filter($"amount > 1500")
.repartition(3)
.select("region","amount")
.withColumn("Tax",F.col("amount") * 0.18)

            

