select
    observation_date,
    country,
    vnd_per_usd,
    available_at
from {{ ref('stg_fx') }}
