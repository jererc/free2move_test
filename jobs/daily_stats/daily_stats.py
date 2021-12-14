#!/usr/bin/env python
from datetime import datetime
import logging
import os.path
import sys

from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession, functions as sf

import settings


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logging.getLogger('py4j').setLevel(logging.WARNING)


def _truncate_day(date):
    return datetime(date.year, date.month, date.day)


def _iterate_month_days(month):
    date = datetime(month.year, month.month, 1)
    end = date + relativedelta(months=1)
    while date < end:
        yield date
        date += relativedelta(days=1)


def _get_days_to_process():
    """Returns a list of days for which we want to process statistics.
    Accepts a day or month value and default to the last finished day.
    """
    parse_day = lambda x: datetime.strptime(x, '%Y%m%d')
    parse_month = lambda x: datetime.strptime(x, '%Y%m')
    try:
        date_str = sys.argv[1]
        if len(date_str) == 8:
            return [parse_day(date_str)]
        elif len(date_str) == 6:
            return list(_iterate_month_days(parse_month(date_str)))
        else:
            raise ValueError()
    except (IndexError, ValueError):
        # Let's default to the last finished day, yesterday
        return _truncate_day(datetime.utcnow()) + relativedelta(days=-1)


def _get_spark_session():
    return (SparkSession
        .builder
        .appName('free2move_test_daily_stats')
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


def _write_df(df, filename, day):
    output_path = os.path.join(settings.OUTPUT_PATH,
        _truncate_day(day).strftime('%Y%m%d'), filename)
    (df.write
        .format('com.databricks.spark.csv')
        .mode('overwrite')
        .option('header', 'true')
        .save(output_path)
    )


def _get_day_orders_by_customer(dfs, day):
    """Returns the orders by customer_id for a day.
    """
    end = _truncate_day(day)
    begin = end + relativedelta(days=-1)

    return (dfs['orders']
        .filter(dfs['orders'].order_approved_at >= begin)
        .filter(dfs['orders'].order_approved_at < end)
        .filter(dfs['orders'].order_status == 'delivered')
        .join(dfs['customers'],
            on=dfs['customers'].customer_id == dfs['orders'].customer_id)
        .select(
            dfs['customers'].customer_id,
            dfs['orders'].order_id,
        )
    )


def compute_stats(dfs, day):
    """Compute statistics and store them as csv.
    """
    orders_by_customer = _get_day_orders_by_customer(dfs, day)

    orders_count_by_customer = (orders_by_customer
        .groupBy(orders_by_customer.customer_id)
        .agg(sf.count(orders_by_customer.order_id).alias('orders_count'))
    )

    ordered_items_per_customer = (orders_by_customer
        .join(dfs['items'], on=dfs['items'].order_id == orders_by_customer.order_id)
        .select(orders_by_customer.customer_id, dfs['items'].price)
    )

    items_count_by_customer = (ordered_items_per_customer
        .groupBy(ordered_items_per_customer.customer_id)
        .agg(sf.count(ordered_items_per_customer.price).alias('items_count'))
    )

    spent_by_customer = (ordered_items_per_customer
        .groupBy(ordered_items_per_customer.customer_id)
        .agg(sf.sum(ordered_items_per_customer.price).alias('total_spent'))
    )

    top_customers = (spent_by_customer
        .sort(spent_by_customer.total_spent.desc())
        .limit(settings.TOP_CUSTOMERS_COUNT)
    )

    # Write the computed data
    _write_df(orders_count_by_customer, 'orders_count', day)
    _write_df(items_count_by_customer, 'items_count', day)
    _write_df(spent_by_customer, 'spent', day)
    _write_df(top_customers, 'top_customers', day)


def main():
    spark = _get_spark_session()
    dfs = _load_data(spark)
    for day in _get_days_to_process():
        logger.debug('processing data at day %s', day)
        compute_stats(dfs=dfs, day=day)


if __name__ == '__main__':
    main()
