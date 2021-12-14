#!/usr/bin/env python
import logging
import os.path

from pyspark.sql import SparkSession, functions as sf

import settings


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logging.getLogger('py4j').setLevel(logging.WARNING)


def _get_spark_session():
    return (SparkSession
        .builder
        .appName('free2move_test_repeaters')
        .getOrCreate()
    )


def _get_df(spark, path, schema=None):
    """Returns a DataFrame.

    We would use explicit schemas in production,
    but for a technical test we can be flexible (and lazy).
    """
    res = (spark.read
        .format('com.databricks.spark.csv')
        .option('header', True)
        .option('delimiter', ',')
        .option("timestampFormat", 'yyyy-MM-dd HH:mm:ss')
        .option('nullValue', 'null')
    )
    if schema:
        res = res.schema(schema)
    else:
        res = res.option('inferSchema', 'true')
    return res.load(path)


def _load_data(spark):
    """Returns a collection of dataframes as a dict.
    """
    customers_path = os.path.join(settings.DATA_PATH, 'customer.csv')
    customers_df = _get_df(spark, path=customers_path)

    products_path = os.path.join(settings.DATA_PATH, 'products.csv')
    products_df = _get_df(spark, path=products_path)

    items_path = os.path.join(settings.DATA_PATH, 'items.csv')
    items_df = _get_df(spark, path=items_path)

    orders_path = os.path.join(settings.DATA_PATH, 'orders.csv')
    orders_df = _get_df(spark, path=orders_path)

    return {
        'customers': customers_df,
        'products': products_df,
        'items': items_df,
        'orders': orders_df,
    }


def _write_df(df, filename):
    output_path = os.path.join(settings.OUTPUT_PATH, filename)
    (df.write
        .format('com.databricks.spark.csv')
        .mode('overwrite')
        .option('header', 'true')
        .save(output_path)
    )


def _get_alltime_orders_by_customer(dfs):
    """Returns the order dates by customer_id for all time.
    """
    return (dfs['orders']
        .filter(dfs['orders'].order_status == 'delivered')
        .join(dfs['customers'],
            on=dfs['customers'].customer_id == dfs['orders'].customer_id)
        .select(
            dfs['customers'].customer_id,
            dfs['orders'].order_id,
        )
    )


def compute(dfs):
    """Computes the repeaters simply by counting the number of orders
    per customer over the alltime data.

    We could identify repeaters orders:
    - over a time period
    - by counting the distinct order dates truncated to the day
    but there aren't any examples in the example dataset.
    """
    orders = _get_alltime_orders_by_customer(dfs)
    order_count_per_customer = (orders
        .groupBy(orders.customer_id)
        .agg(sf.count(orders.order_id).alias('order_count'))
    )
    repeaters = (order_count_per_customer
        .filter(order_count_per_customer.order_count > 1)
    )
# Repeaters:
# [Row(customer_id='7d1dd3c96c21c803f7a1a32aa8d9feb9', order_count=2),
#  Row(customer_id='9b8ce803689b3562defaad4613ef426f', order_count=2)]
    _write_df(repeaters, 'repeaters')


def main():
    spark = _get_spark_session()
    compute(_load_data(spark))


if __name__ == '__main__':
    main()
