select
    series_id,
    country,
    observation_date,
    frequency,
    value,
    unit
from {{ source('silver', 'iip_observations') }}
where series_id = 'iip_growth'
  and frequency = 'annual'
