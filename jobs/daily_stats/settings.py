import os.path

from pyspark.sql.types import (StructType, StructField,
        StringType, IntegerType, TimestampType, ArrayType, MapType)


# Paths
DATA_PATH = os.path.expanduser('~/Downloads/data_engineering_b2b_test/data')
OUTPUT_PATH = os.path.expanduser('~/Downloads/data_engineering_b2b_test/data')

# Schemas
CUSTOMERS_SCHEMA = (StructType()
  .add('customer_id', StringType(), True)
  .add('customer_unique_id', StringType(), True)
  .add('customer_zip_code_prefix', StringType(), True)
  .add('customer_city', StringType(), True)
  .add('customer_state', StringType(), True)
)
ORDERS_SCHEMA = (StructType()
  .add('order_id', StringType(), True)
  .add('customer_id', StringType(), True)
  .add('order_status', StringType(), True)
  .add('order_purchase_timestamp', TimestampType(), True)
  .add('order_approved_at', TimestampType(), True)
  .add('order_delivered_carrier_date', TimestampType(), True)
  .add('order_delivered_customer_date', TimestampType(), True)
  .add('order_estimated_delivery_date', TimestampType(), True)
)

TOP_CUSTOMERS_COUNT = 20
