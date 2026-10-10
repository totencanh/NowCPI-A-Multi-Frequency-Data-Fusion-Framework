select
    observation_date,
    country,
    value as brent_usd_per_barrel,
    available_at
from {{ ref('stg_oil') }}
