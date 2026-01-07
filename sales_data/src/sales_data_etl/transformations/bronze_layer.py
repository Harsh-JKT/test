# from config.config import TABLES,S3_PATHS
import dlt

from utilities.utils import read_from_s3_autoloader_with_time
from config.config import TABLES, S3_PATHS

access_key = dbutils.secrets.get(scope="spark_creds", key="access_key")
secret_key = dbutils.secrets.get(scope="spark_creds", key="secret_key")

# Use the secrets as needed
spark.conf.set("fs.s3a.access.key", access_key)
spark.conf.set("fs.s3a.secret.key", secret_key)
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

@dlt.table(
    name=TABLES["raw_sales_data"],
    comment="Raw sales discount data loaded from S3 using Auto Loader."
)
def raw_sales_data():
    """
    This function reads raw sales discount data from S3 using Auto Loader.
    It applies the necessary transformations such as renaming columns
    and adding a file append time column.
    """
    return (
        read_from_s3_autoloader_with_time(
            spark,
            s3_path=S3_PATHS["sales_data"],
            file_format="parquet"
        )
    )


