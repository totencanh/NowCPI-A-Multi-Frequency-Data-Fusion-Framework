select
    series_id,
    country,
    observation_date,
    frequency,
    value as cpi_index,
    unit
from {{ source('silver', 'cpi_observations') }}
where series_id = 'cpi_headline'
  and frequency = 'monthly'
