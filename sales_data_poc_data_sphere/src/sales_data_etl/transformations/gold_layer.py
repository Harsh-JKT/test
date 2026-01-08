from config.config import TABLES
from utilities.utils import final_layer_table
import dlt
from pyspark.sql import types as types
from pyspark.sql import functions as F
from pyspark.sql import functions as py_func



@dlt.table(
    name=TABLES["final_sales_data"],
    comment="Final gold layer of sales data",
    cluster_by_auto=True #liquid clustering
)
def final_sales_data():
    df = spark.read.table(TABLES["clean_sales_data"])
    df = df.withColumn("invoice_date", py_func.to_date("invoice_date", "yyyyMMdd"))\
        .withColumn("period",py_func.date_format(py_func.trunc("invoice_date", "MM"), "yyyy-MM"))\
        .withColumn("new_rep_month", py_func.when((py_func.month("invoice_date") == 4) & (py_func.year("invoice_date") == 2024), py_func.date_format(py_func.date_sub("invoice_date", -1), "yyyy-MM")).otherwise(py_func.date_format(py_func.date_sub("invoice_date", -2), "yyyy-MM")))\
        .withColumn("distribution_channel",F.col("distribution_channel").cast(types.StringType()))\
        .withColumn("company_code",F.col("company_code").cast(types.StringType()))

    return df