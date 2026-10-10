select
    series_id,
    country,
    observation_date,
    frequency,
    value as price,
    unit
from {{ source('silver', 'vn_fuel_observations') }}
where series_id in ('vn_fuel_ron95', 'vn_fuel_e10_ron95')
  and frequency = 'daily'
