select
    observation_date,
    country,
    value as brent_usd_per_barrel,
    available_at,
    source_release_ts,
    available_at_basis
from {{ ref('stg_oil') }}
