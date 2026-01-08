import dlt
from pyspark.sql import functions as py_func
from config.config import TABLES,EXPECTIONS
from config.variables import sales_data_column_rename
from utilities.utils import auto_cdc_silver_table


auto_cdc_silver_table(
    target_table=TABLES["clean_sales_data"],
    source_table=TABLES["raw_sales_data"],
    keys=["invoice_number","sku","invoice_type","batch","volume"],
    sequence_by="__timestamp",
    # except_column_list=["_rescued_data","ppd","qtd","discount","acer","ladder_discount","add_on","tcs","round_off","10_ka_dam","fraight","claim_discount","a0bill_date"],
    except_column_list=["__sequence_number","__operation_type"],
    ignore_null_updates=False
)

# @dlt.table(name=TABLES["clean_sales_data_processed"])
# @dlt.expect_or_drop(
#     EXPECTIONS["sales_data"]["name"],
#     EXPECTIONS["sales_data"]["conditions"]
# )
# def preprocess_sales_target():
#     df = spark.read.table(TABLES["clean_sales_data"])
#     # df = df.withColumnsRenamed(sales_data_column_rename)
#     df = df.withColumn("invoice_date", py_func.to_date("invoice_date", "yyyyMMdd"))\
#         .withColumn("period",py_func.date_format(py_func.trunc("invoice_date", "MM"), "yyyy-MM"))\
#         .withColumn("new_rep_month", py_func.when((py_func.month("invoice_date") == 4) & (py_func.year("invoice_date") == 2024), py_func.date_format(py_func.date_sub("invoice_date", -1), "yyyy-MM")).otherwise(py_func.date_format(py_func.date_sub("invoice_date", -2), "yyyy-MM")))
#     return df
