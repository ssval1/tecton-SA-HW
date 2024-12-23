# The merchant fraud rate over series of time windows, updated daily.

# SELECT
#     USER_ID,
#     AMT,
#     TIMESTAMP
# FROM
#     transactions_batch


from tecton import batch_feature_view, Attribute
from tecton.types import Float64
from datetime import datetime, timedelta
from entity import customer, transaction
from txn_batch import txn_batch


@batch_feature_view(
    sources=[txn_batch],
    entities=[customer,transaction],
    mode="spark_sql",
    online=False,
    offline=False,
    batch_schedule=timedelta(days=1),
    timestamp_field="TIMESTAMP",
    features=[
        Attribute("AMT", Float64),
    ],
    feature_start_time=datetime(2022, 4, 14),
    description='The merchant fraud rate over series of time windows, updated daily.'

)
def user_last_transaction_amount(txn_batch):
    return f"""
        SELECT
            USER_ID,
            AMT,
            TIMESTAMP
        FROM
            {txn_batch}
        """