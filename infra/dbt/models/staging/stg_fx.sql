select
    series_id,
    country,
    observation_date,
    frequency,
    value as vnd_per_usd,
    unit
from {{ source('silver', 'usd_vnd_observations') }}
where series_id = 'usd_vnd'
  and frequency = 'daily'
