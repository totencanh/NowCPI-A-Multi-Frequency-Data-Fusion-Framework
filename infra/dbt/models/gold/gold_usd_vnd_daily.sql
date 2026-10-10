select
    observation_date,
    country,
    vnd_per_usd
from {{ ref('stg_fx') }}
