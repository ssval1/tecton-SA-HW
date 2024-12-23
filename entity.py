from tecton import Entity
from tecton.types import Field, String

customer = Entity(name="User", join_keys=[Field("user_id", String)], tags={'release': 'development'})

transaction = Entity(name="Transaction", join_keys=[Field("transaction_id", String)], tags={'release': 'development'})