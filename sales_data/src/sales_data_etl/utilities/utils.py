import pyspark.sql.functions as py_func
import dlt


def read_from_s3_autoloader_with_time(
    spark,
    s3_path: str,
    file_format: str = "csv",
    extra_options: dict = None):
    """
    Reusable function to read from S3 using Auto Loader with file modification time.
    """
    options = {
        "cloudFiles.format": file_format, #ex: "csv"
        "cloudFiles.allowOverwrites":"true",
        "cloudFiles.inferColumnTypes": "true"
    }
    if extra_options:
        options.update(extra_options) #mix of old + extra options
    return (
        spark.readStream.format("cloudFiles")
        .options(**options)
        .load(s3_path)
    )


def auto_cdc_silver_table(
        target_table: str,
        source_table: str,
        keys: list,
        sequence_by: str,
        except_column_list: list = None,
        ignore_null_updates: bool = False
        ):
    dlt.create_streaming_table(name=target_table, comment="Clean, materialized sales")
    dlt.create_auto_cdc_flow(
    target=target_table,  # The customer table being materialized
    source=source_table,  # the incoming CDC
    keys=keys,  # what we'll be using to match the rows to upsert
    sequence_by=py_func.col(sequence_by),  # de-duplicate by operation date, getting the most recent value
    ignore_null_updates=False, # null value will be updated explicitly , i.e. it will care if the value is null or not just update that value.
    except_column_list=except_column_list,  # columns to ignore in the upsert
    )

def final_layer_table(
    spark,
    target_table: str,
    source_table: str,
    comment: str):
    """
    Function to create a final layer table with the specified target and source tables.
    """
    @dlt.table(
        name=target_table,
        comment=comment
    )
    def final_table():
        return spark.read.table(source_table)

    




