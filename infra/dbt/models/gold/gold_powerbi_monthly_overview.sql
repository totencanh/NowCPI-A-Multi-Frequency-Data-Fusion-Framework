with months as (
    select month_start, country
    from {{ ref('gold_cpi_monthly') }}
    union
    select cast(date_trunc('month', observation_date) as date), country
    from {{ ref('gold_brent_daily') }}
    union
    select cast(date_trunc('month', observation_date) as date), country
    from {{ ref('gold_usd_vnd_daily') }}
    union
    select cast(date_trunc('month', observation_date) as date), country
    from {{ ref('gold_fuel_daily') }}
), monthly_brent as (
    select
        cast(date_trunc('month', observation_date) as date) as month_start,
        country,
        avg(brent_usd_per_barrel) as brent_usd_per_barrel_avg,
        min(brent_usd_per_barrel) as brent_usd_per_barrel_min,
        max(brent_usd_per_barrel) as brent_usd_per_barrel_max,
        count(*) as brent_observation_count,
        max(available_at) as brent_latest_available_at
    from {{ ref('gold_brent_daily') }}
    group by 1, 2
), monthly_fx as (
    select
        cast(date_trunc('month', observation_date) as date) as month_start,
        country,
        avg(vnd_per_usd) as vnd_per_usd_avg,
        min(vnd_per_usd) as vnd_per_usd_min,
        max(vnd_per_usd) as vnd_per_usd_max,
        count(*) as fx_observation_count,
        max(available_at) as fx_latest_available_at
    from {{ ref('gold_usd_vnd_daily') }}
    group by 1, 2
), monthly_fuel as (
    select
        cast(date_trunc('month', observation_date) as date) as month_start,
        country,
        avg(case when fuel_type = 'RON95' then vnd_per_liter end)
            as ron95_vnd_per_liter_avg,
        min(case when fuel_type = 'RON95' then vnd_per_liter end)
            as ron95_vnd_per_liter_min,
        max(case when fuel_type = 'RON95' then vnd_per_liter end)
            as ron95_vnd_per_liter_max,
        avg(case when fuel_type = 'E10_RON95' then vnd_per_liter end)
            as e10_ron95_vnd_per_liter_avg,
        min(case when fuel_type = 'E10_RON95' then vnd_per_liter end)
            as e10_ron95_vnd_per_liter_min,
        max(case when fuel_type = 'E10_RON95' then vnd_per_liter end)
            as e10_ron95_vnd_per_liter_max,
        count(*) as fuel_observation_count,
        max(available_at) as fuel_latest_available_at
    from {{ ref('gold_fuel_daily') }}
    group by 1, 2
)
select
    months.month_start,
    months.country,
    cpi.cpi_available_at,
    cpi.cpi_headline,
    cpi.cpi_headline_mom_percent,
    cpi.cpi_headline_yoy_percent,
    oil.brent_usd_per_barrel_avg,
    oil.brent_usd_per_barrel_min,
    oil.brent_usd_per_barrel_max,
    oil.brent_observation_count,
    oil.brent_latest_available_at,
    fx.vnd_per_usd_avg,
    fx.vnd_per_usd_min,
    fx.vnd_per_usd_max,
    fx.fx_observation_count,
    fx.fx_latest_available_at,
    fuel.ron95_vnd_per_liter_avg,
    fuel.ron95_vnd_per_liter_min,
    fuel.ron95_vnd_per_liter_max,
    fuel.e10_ron95_vnd_per_liter_avg,
    fuel.e10_ron95_vnd_per_liter_min,
    fuel.e10_ron95_vnd_per_liter_max,
    fuel.fuel_observation_count,
    fuel.fuel_latest_available_at
from months
left join {{ ref('gold_cpi_monthly') }} as cpi
    on cpi.month_start = months.month_start
   and cpi.country = months.country
left join monthly_brent as oil
    on oil.month_start = months.month_start
   and oil.country = months.country
left join monthly_fx as fx
    on fx.month_start = months.month_start
   and fx.country = months.country
left join monthly_fuel as fuel
    on fuel.month_start = months.month_start
   and fuel.country = months.country
