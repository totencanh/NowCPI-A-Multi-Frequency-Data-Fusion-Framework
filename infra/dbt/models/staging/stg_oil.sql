select
    series_id,
    country,
    observation_date,
    frequency,
    value,
    unit
from {{ source('silver', 'brent_oil_observations') }}
where series_id = 'brent_front_month'
  and frequency = 'daily'
