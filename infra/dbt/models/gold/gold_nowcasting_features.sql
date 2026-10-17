-- Historical joins use observation-time cutoffs because source release vintages
-- are unavailable for several inputs. This is a pseudo-vintage feature set.
with countries as (
    select distinct country
    from {{ ref('gold_cpi_monthly') }}
    where country is not null
), target_bounds as (
    select coalesce(min(month_start), date '2025-01-01') as first_month
    from {{ ref('gold_cpi_monthly') }}
), target_months as (
    select calendar.target_month
    from target_bounds as bounds
    cross join unnest(
        sequence(
            bounds.first_month,
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
), cpi_candidates as (
    select
        target.target_month,
        target.country,
        target.feature_cutoff_at,
        lagged.month_start as cpi_observation_month,
        date_diff('month', lagged.month_start, target.target_month)
            as cpi_observation_age_months,
        lagged.cpi_headline as cpi_headline_index_latest,
        lagged.cpi_headline_mom_percent as cpi_headline_mom_latest,
        lagged.cpi_headline_yoy_percent as cpi_headline_yoy_latest,
        lagged.cpi_food_beverages as cpi_food_beverages_latest,
        lagged.cpi_alcohol_tobacco as cpi_alcohol_tobacco_latest,
        lagged.cpi_clothing_footwear as cpi_clothing_footwear_latest,
        lagged.cpi_housing_utilities as cpi_housing_utilities_latest,
        lagged.cpi_household_furnishings as cpi_household_furnishings_latest,
        lagged.cpi_health as cpi_health_latest,
        lagged.cpi_transport as cpi_transport_latest,
        lagged.cpi_communication as cpi_communication_latest,
        lagged.cpi_recreation_culture as cpi_recreation_culture_latest,
        lagged.cpi_education as cpi_education_latest,
        lagged.cpi_restaurants_hotels as cpi_restaurants_hotels_latest,
        lagged.cpi_misc_goods_services as cpi_misc_goods_services_latest,
        row_number() over (
            partition by target.target_month, target.country
            order by lagged.month_start desc nulls last
        ) as cpi_rank
    from targets as target
    left join {{ ref('gold_cpi_monthly') }} as lagged
        on lagged.country = target.country
       and lagged.month_start < target.target_month
       and lagged.cpi_headline is not null
), cpi_lagged as (
    select *
    from cpi_candidates
    where cpi_rank = 1
), monthly_brent as (
    select
        target.target_month,
        target.country,
        avg(brent.brent_usd_per_barrel) as brent_usd_per_barrel_avg_mtd,
        count(brent.observation_date) as brent_observation_count_mtd,
        max(brent.observation_date) as brent_latest_observation_date,
        max(brent.available_at) as brent_latest_available_at
    from targets as target
    left join {{ ref('gold_brent_daily') }} as brent
        on brent.country = target.country
       and brent.observation_date >= target.target_month
       and brent.observation_date <= cast(target.feature_cutoff_at as date)
       and brent.observation_date < date_add('month', 1, target.target_month)
    group by 1, 2
), monthly_fx as (
    select
        target.target_month,
        target.country,
        avg(fx.vnd_per_usd) as vnd_per_usd_avg_mtd,
        count(fx.observation_date) as fx_observation_count_mtd,
        max(fx.observation_date) as fx_latest_observation_date,
        max(fx.available_at) as fx_latest_available_at
    from targets as target
    left join {{ ref('gold_usd_vnd_daily') }} as fx
        on fx.country = target.country
       and fx.observation_date >= target.target_month
       and fx.observation_date <= cast(target.feature_cutoff_at as date)
       and fx.observation_date < date_add('month', 1, target.target_month)
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
        max(fuel.observation_date) as fuel_latest_observation_date,
        max(fuel.available_at) as fuel_latest_available_at
    from targets as target
    left join {{ ref('gold_fuel_daily') }} as fuel
        on fuel.country = target.country
       and fuel.observation_date >= target.target_month
       and fuel.observation_date <= cast(target.feature_cutoff_at as date)
       and fuel.observation_date < date_add('month', 1, target.target_month)
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
)
select
    cpi.target_month,
    cpi.country,
    '{{ invocation_id }}' as gold_build_id,
    current_timestamp as gold_built_at,
    cast(cpi.feature_cutoff_at as varchar) as feature_cutoff_at,
    'observation_date_cutoff_no_archived_release_vintages' as feature_vintage_mode,
    cpi.cpi_observation_month,
    cpi.cpi_observation_age_months,
    cpi.cpi_headline_index_latest,
    cpi.cpi_headline_mom_latest,
    cpi.cpi_headline_yoy_latest,
    cpi.cpi_food_beverages_latest,
    cpi.cpi_alcohol_tobacco_latest,
    cpi.cpi_clothing_footwear_latest,
    cpi.cpi_housing_utilities_latest,
    cpi.cpi_household_furnishings_latest,
    cpi.cpi_health_latest,
    cpi.cpi_transport_latest,
    cpi.cpi_communication_latest,
    cpi.cpi_recreation_culture_latest,
    cpi.cpi_education_latest,
    cpi.cpi_restaurants_hotels_latest,
    cpi.cpi_misc_goods_services_latest,
    brent.brent_usd_per_barrel_avg_mtd,
    brent.brent_observation_count_mtd,
    brent.brent_latest_observation_date,
    date_diff(
        'day', brent.brent_latest_observation_date,
        cast(cpi.feature_cutoff_at as date)
    ) as brent_observation_age_days,
    brent.brent_latest_available_at,
    fx.vnd_per_usd_avg_mtd,
    fx.fx_observation_count_mtd,
    fx.fx_latest_observation_date,
    date_diff(
        'day', fx.fx_latest_observation_date,
        cast(cpi.feature_cutoff_at as date)
    ) as fx_observation_age_days,
    fx.fx_latest_available_at,
    fuel.fuel_ron95_vnd_per_liter_avg_mtd,
    fuel.fuel_e10_ron95_vnd_per_liter_avg_mtd,
    fuel.fuel_observation_count_mtd,
    fuel.fuel_latest_observation_date,
    date_diff(
        'day', fuel.fuel_latest_observation_date,
        cast(cpi.feature_cutoff_at as date)
    ) as fuel_observation_age_days,
    fuel.fuel_latest_available_at,
    iip.iip_observation_year,
    year(cpi.target_month) - iip.iip_observation_year as iip_observation_age_years,
    iip.iip_growth as iip_growth_latest_prior_year,
    iip.iip_available_at,
    cast(100.0 * (
        case when cpi.cpi_headline_index_latest is not null then 1 else 0 end
        + case when brent.brent_observation_count_mtd > 0 then 1 else 0 end
        + case when fx.fx_observation_count_mtd > 0 then 1 else 0 end
        + case when fuel.fuel_observation_count_mtd > 0 then 1 else 0 end
        + case when iip.iip_growth is not null then 1 else 0 end
    ) / 5 as double) as feature_source_coverage_pct
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
