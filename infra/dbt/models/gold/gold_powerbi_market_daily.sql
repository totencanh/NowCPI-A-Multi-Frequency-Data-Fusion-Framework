select
    observation_date,
    country,
    'brent_oil' as indicator_key,
    'Brent crude oil' as indicator_name,
    brent_usd_per_barrel as indicator_value,
    'USD/barrel' as unit,
    available_at
from {{ ref('gold_brent_daily') }}

union all

select
    observation_date,
    country,
    'usd_vnd' as indicator_key,
    'USD/VND exchange rate' as indicator_name,
    vnd_per_usd as indicator_value,
    'VND/USD' as unit,
    available_at
from {{ ref('gold_usd_vnd_daily') }}

union all

select
    observation_date,
    country,
    lower(fuel_type) as indicator_key,
    case fuel_type
        when 'RON95' then 'RON95 fuel price'
        when 'E10_RON95' then 'E10 RON95 fuel price'
        else fuel_type
    end as indicator_name,
    vnd_per_liter as indicator_value,
    'VND/liter' as unit,
    available_at
from {{ ref('gold_fuel_daily') }}
