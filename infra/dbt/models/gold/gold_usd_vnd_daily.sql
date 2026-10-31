select
    observation_date,
    country,
    vnd_per_usd,
    available_at,
    available_at_basis
from {{ ref('stg_fx') }}
