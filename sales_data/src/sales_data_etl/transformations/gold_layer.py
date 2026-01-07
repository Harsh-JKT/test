from config.config import TABLES
from utilities.utils import final_layer_table
import dlt
from pyspark.sql import types as types
from pyspark.sql import functions as F



@dlt.table(
    name=TABLES["final_sales_data"],
    comment="Final gold layer of sales data",
    cluster_by_auto=True #liquid clustering
)
def final_sales_data():
    return spark.read.table(TABLES["clean_sales_data_processed"])\
        .withColumn("distribution_channel",F.col("distribution_channel").cast(types.StringType()))\
        .withColumn("company_code",F.col("company_code").cast(types.StringType()))


    