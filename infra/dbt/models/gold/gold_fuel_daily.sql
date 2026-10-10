select
    observation_date,
    country,
    case series_id
        when 'vn_fuel_ron95' then 'RON95'
        when 'vn_fuel_e10_ron95' then 'E10_RON95'
    end as fuel_type,
    price as vnd_per_liter,
    available_at
from {{ ref('stg_fuel') }}
