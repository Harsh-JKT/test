
# provisioned-tableau-data.raw_layer

TABLES = {
    "raw_sales_data": "`provisioned-tableau-data`.raw_layer.sales_data_bronze",
    "clean_sales_data": "`provisioned-tableau-data`.raw_layer.sales_data_silver",
    "clean_sales_data_processed": "`provisioned-tableau-data`.raw_layer.sales_data_processed",
    "final_sales_data": "`provisioned-tableau-data`.raw_layer.sales_data_gold",

}
S3_PATHS = {
    "sales_data":"s3://sap-datasphere-poc/Sales_Data_ECC/initial/"
}
EXPECTIONS = {
    "sales_data": {
        "name": "sales_data",
        "conditions": "NOT (volume = 0 AND invoice_value = 0 AND indicator_cancel IS NULL and invoice_type not in ('YRAW', 'ZRAW', 'S1', 'S2', 'S3', 'S4','ZEXP','YEXP'))"
    }
}