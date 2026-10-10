select
    observation_date,
    country,
    value as brent_usd_per_barrel
from {{ ref('stg_oil') }}
