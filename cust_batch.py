import os
import tecton
from tecton import FileConfig, BatchSource, HiveConfig, Entity, DatetimePartitionColumn

ws = tecton.get_workspace("senthil")


#https://docs.tecton.ai/docs/sdk-reference/data-sources/DatetimePartitionColumn
#https://docs.tecton.ai/docs/sdk-reference/data-sources/BatchSource

usr_batch_config = HiveConfig(database="demo_fraud_v2", table="customers", 
)

usr_batch = BatchSource(
    name="users_batch",
    batch_config=usr_batch_config,
    tags={'release': 'development'}
   
)

#https://docs.tecton.ai/docs/sdk-reference/data-sources/batch-configuration/HiveConfig

def users_fraud(df):
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType
    from pyspark.sql.functions import from_json, col, from_utc_timestamp

    payload_schema = StructType([
        StructField('user_id', StringType(), False),
        StructField('transaction_id', StringType(), False),
        StructField('city_pop', StringType(), False),
        ])

    return (
      df.selectExpr('cast (data as STRING) jsonData')
      .select(from_json('jsonData', payload_schema).alias('payload'))
      .select(
        col('payload.user_id').alias('user_id'),
        col('payload.transaction_id').alias('transaction_id'),
        col('payload.city_pop').alias('category'),
        )
    )