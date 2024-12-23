import os
import tecton
from tecton import FileConfig, BatchSource, HiveConfig, Entity, DatetimePartitionColumn

ws = tecton.get_workspace("senthil")


#https://docs.tecton.ai/docs/sdk-reference/data-sources/DatetimePartitionColumn
#https://docs.tecton.ai/docs/sdk-reference/data-sources/BatchSource

txn_batch_config = HiveConfig(database="demo_fraud_v2", table="transactions", timestamp_field='timestamp',
    datetime_partition_columns = [
    DatetimePartitionColumn(column_name="partition_0", datepart="year", zero_padded=True),
    DatetimePartitionColumn(column_name="partition_1", datepart="month", zero_padded=True),
    DatetimePartitionColumn(column_name="partition_2", datepart="day", zero_padded=True),
    DatetimePartitionColumn(column_name="partition_3", datepart="hour", zero_padded=True),
    ]
)

txn_batch = BatchSource(
    name="transactions_batch",
    batch_config=txn_batch_config,
    tags={'release': 'development'}
   
)

#https://docs.tecton.ai/docs/sdk-reference/data-sources/batch-configuration/HiveConfig

def txn_fraud(df):
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType
    from pyspark.sql.functions import from_json, col, from_utc_timestamp

    payload_schema = StructType([
        StructField('user_id', StringType(), False),
        StructField('transaction_id', StringType(), False),
        StructField('category', StringType(), False),
        StructField('amt', StringType(), False),
        StructField('is_fraud', StringType(), False),
        StructField('merchant', StringType(), False),
        StructField('merch_lat', StringType(), False),
        StructField('merch_long', StringType(), False),
        StructField('timestamp', StringType(), False),
        ])

    return (
      df.selectExpr('cast (data as STRING) jsonData')
      .select(from_json('jsonData', payload_schema).alias('payload'))
      .select(
        col('payload.user_id').alias('user_id'),
        col('payload.transaction_id').alias('txn_id'),
        col('payload.category').alias('category'),
        col('payload.amt').cast('double').alias('amt'),
        col('payload.is_fraud').cast('long').alias('is_fraud'),
        col('payload.merchant').alias('merchant'),
        col('payload.merch_lat').cast('double').alias('merch_lat'),
        col('payload.merch_long').cast('double').alias('merch_long'),
        from_utc_timestamp('payload.timestamp', 'UTC').alias('timestamp')
        )
    )