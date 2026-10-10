with countries as (
    select distinct country
    from {{ ref('gold_cpi_monthly') }}
    where country is not null
), target_months as (
    select target_month
    from unnest(
        sequence(
            date '2025-01-01',
            cast(date_trunc('month', current_date) as date),
            interval '1' month
        )
    ) as calendar(target_month)
), targets as (
    select
        months.target_month,
        countries.country,
        least(
            cast(current_timestamp as timestamp),
            cast(date_add('day', -1, date_add('month', 1, months.target_month)) as timestamp)
        ) as feature_cutoff_at
    from target_months as months
    cross join countries
), cpi_lagged as (
    select
        target.target_month,
        target.country,
        target.feature_cutoff_at,
        lagged.cpi_headline as cpi_headline_index_lag_1m,
        lagged.cpi_headline_mom_percent as cpi_headline_mom_lag_1m,
        lagged.cpi_headline_yoy_percent as cpi_headline_yoy_lag_1m,
        lagged.cpi_food_beverages as cpi_food_beverages_lag_1m,
        lagged.cpi_alcohol_tobacco as cpi_alcohol_tobacco_lag_1m,
        lagged.cpi_clothing_footwear as cpi_clothing_footwear_lag_1m,
        lagged.cpi_housing_utilities as cpi_housing_utilities_lag_1m,
        lagged.cpi_household_furnishings as cpi_household_furnishings_lag_1m,
        lagged.cpi_health as cpi_health_lag_1m,
        lagged.cpi_transport as cpi_transport_lag_1m,
        lagged.cpi_communication as cpi_communication_lag_1m,
        lagged.cpi_recreation_culture as cpi_recreation_culture_lag_1m,
        lagged.cpi_education as cpi_education_lag_1m,
        lagged.cpi_restaurants_hotels as cpi_restaurants_hotels_lag_1m,
        lagged.cpi_misc_goods_services as cpi_misc_goods_services_lag_1m
    from targets as target
    left join {{ ref('gold_cpi_monthly') }} as lagged
        on lagged.country = target.country
       and lagged.month_start = date_add('month', -1, target.target_month)
       and lagged.cpi_available_at <= target.feature_cutoff_at
), monthly_brent as (
    select
        target.target_month,
        target.country,
        avg(brent.brent_usd_per_barrel) as brent_usd_per_barrel_avg_mtd,
        count(brent.observation_date) as brent_observation_count_mtd,
        max(brent.available_at) as brent_latest_available_at
    from targets as target
    left join {{ ref('gold_brent_daily') }} as brent
        on brent.country = target.country
       and brent.observation_date >= target.target_month
       and brent.observation_date < date_add('month', 1, target.target_month)
       and brent.available_at <= target.feature_cutoff_at
    group by 1, 2
), monthly_fx as (
    select
        target.target_month,
        target.country,
        avg(fx.vnd_per_usd) as vnd_per_usd_avg_mtd,
        count(fx.observation_date) as fx_observation_count_mtd,
        max(fx.available_at) as fx_latest_available_at
    from targets as target
    left join {{ ref('gold_usd_vnd_daily') }} as fx
        on fx.country = target.country
       and fx.observation_date >= target.target_month
       and fx.observation_date < date_add('month', 1, target.target_month)
       and fx.available_at <= target.feature_cutoff_at
    group by 1, 2
), monthly_fuel as (
    select
        target.target_month,
        target.country,
        avg(case when fuel.fuel_type = 'RON95' then fuel.vnd_per_liter end)
            as fuel_ron95_vnd_per_liter_avg_mtd,
        avg(case when fuel.fuel_type = 'E10_RON95' then fuel.vnd_per_liter end)
            as fuel_e10_ron95_vnd_per_liter_avg_mtd,
        count(fuel.observation_date) as fuel_observation_count_mtd,
        max(fuel.available_at) as fuel_latest_available_at
    from targets as target
    left join {{ ref('gold_fuel_daily') }} as fuel
        on fuel.country = target.country
       and fuel.observation_date >= target.target_month
       and fuel.observation_date < date_add('month', 1, target.target_month)
       and fuel.available_at <= target.feature_cutoff_at
    group by 1, 2
), iip_candidates as (
    select
        target.target_month,
        target.country,
        iip.year as iip_observation_year,
        iip.iip_growth,
        iip.available_at as iip_available_at,
        row_number() over (
            partition by target.target_month, target.country
            order by iip.year desc nulls last, iip.available_at desc nulls last
        ) as iip_rank
    from targets as target
    left join {{ ref('gold_iip_annual') }} as iip
        on iip.country = target.country
       and iip.year < year(target.target_month)
       and iip.available_at <= target.feature_cutoff_at
)
select
    cpi.target_month,
    cpi.country,
    cast(cpi.feature_cutoff_at as varchar) as feature_cutoff_at,
    cpi.cpi_headline_index_lag_1m,
    cpi.cpi_headline_mom_lag_1m,
    cpi.cpi_headline_yoy_lag_1m,
    cpi.cpi_food_beverages_lag_1m,
    cpi.cpi_alcohol_tobacco_lag_1m,
    cpi.cpi_clothing_footwear_lag_1m,
    cpi.cpi_housing_utilities_lag_1m,
    cpi.cpi_household_furnishings_lag_1m,
    cpi.cpi_health_lag_1m,
    cpi.cpi_transport_lag_1m,
    cpi.cpi_communication_lag_1m,
    cpi.cpi_recreation_culture_lag_1m,
    cpi.cpi_education_lag_1m,
    cpi.cpi_restaurants_hotels_lag_1m,
    cpi.cpi_misc_goods_services_lag_1m,
    brent.brent_usd_per_barrel_avg_mtd,
    brent.brent_observation_count_mtd,
    brent.brent_latest_available_at,
    fx.vnd_per_usd_avg_mtd,
    fx.fx_observation_count_mtd,
    fx.fx_latest_available_at,
    fuel.fuel_ron95_vnd_per_liter_avg_mtd,
    fuel.fuel_e10_ron95_vnd_per_liter_avg_mtd,
    fuel.fuel_observation_count_mtd,
    fuel.fuel_latest_available_at,
    iip.iip_observation_year,
    iip.iip_growth as iip_growth_latest_prior_year,
    iip.iip_available_at
from cpi_lagged as cpi
left join monthly_brent as brent
    on brent.country = cpi.country
   and brent.target_month = cpi.target_month
left join monthly_fx as fx
    on fx.country = cpi.country
   and fx.target_month = cpi.target_month
left join monthly_fuel as fuel
    on fuel.country = cpi.country
   and fuel.target_month = cpi.target_month
left join iip_candidates as iip
    on iip.country = cpi.country
   and iip.target_month = cpi.target_month
   and iip.iip_rank = 1
