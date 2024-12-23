# What percentage of the transactions made at a merchant in the last 7 days were made by users who live in cities with > 10,000 residents?


# select t.merchant, count (t.transaction_id) as txn_cnt
# 	from {txn_batch} t join {cust_batch} u on t.user_id = u.user_id
# 	where u.city_pop > 10000 
# 	group by t.merchant

# ref URL: https://docs.tecton.ai/docs/sdk-reference/features/Aggregate

from tecton import batch_feature_view, Aggregate, TimeWindow
from tecton.types import Float64, Field
from tecton.aggregation_functions import approx_percentile
from datetime import datetime, timedelta

from entity import customer, transaction
from txn_batch import txn_batch
from cust_batch import usr_batch


@batch_feature_view(
    sources=[txn_batch,usr_batch.unfiltered()],
    entities=[customer,transaction],
    mode="spark_sql",
    online=True,
    offline=True,
    batch_schedule=timedelta(days=1),
    timestamp_field="timestamp",
    aggregation_interval=timedelta(days=1),
    tags={'release': 'development'},
    owner='jadsen2000@gmail.com',
    features=[
        Aggregate(input_column=Field('txn_id', Float64), function=approx_percentile(percentile=0.5), time_window=timedelta(days=7))
    ],
    feature_start_time=datetime(2022, 4, 14),
    description='The percentage of the transactions made at a merchant in the last 7 days were made by users who live in cities with > 10,000 residents, updated daily.'

)
def percentage_txn_merchant(txn_batch, cust_batch):
    return f"""
        select t.timestamp, t.merchant, count (t.txn_id) as txn_cnt
		from {txn_batch} t join {cust_batch} u on t.user_id = u.user_id
 		where u.city_pop > 10000 
		group by t.merchant
        """