with cpi_observations as (
    select
        country,
        series_id,
        cast(date_trunc('month', observation_date) as date) as month_start,
        cpi_index
    from {{ ref('stg_cpi') }}
    union all
    select
        country,
        series_id,
        cast(date_trunc('month', observation_date) as date) as month_start,
        component_index as cpi_index
    from {{ ref('stg_cpi_components') }}
), cpi_with_changes as (
    select
        current.country,
        current.series_id,
        current.month_start,
        current.cpi_index,
        100.0 * (current.cpi_index - previous.cpi_index)
            / nullif(previous.cpi_index, 0) as mom_percent,
        100.0 * (current.cpi_index - prior_year.cpi_index)
            / nullif(prior_year.cpi_index, 0) as yoy_percent
    from cpi_observations as current
    left join cpi_observations as previous
        on previous.country = current.country
       and previous.series_id = current.series_id
       and previous.month_start = date_add('month', -1, current.month_start)
    left join cpi_observations as prior_year
        on prior_year.country = current.country
       and prior_year.series_id = current.series_id
       and prior_year.month_start = date_add('month', -12, current.month_start)
)
select
    month_start,
    country,
    max(case when series_id = 'cpi_headline' then cpi_index end) as cpi_headline,
    max(case when series_id = 'cpi_headline' then mom_percent end) as cpi_headline_mom_percent,
    max(case when series_id = 'cpi_headline' then yoy_percent end) as cpi_headline_yoy_percent,
    max(case when series_id = 'cpi_component_cp01' then cpi_index end) as cpi_food_beverages,
    max(case when series_id = 'cpi_component_cp02' then cpi_index end) as cpi_alcohol_tobacco,
    max(case when series_id = 'cpi_component_cp03' then cpi_index end) as cpi_clothing_footwear,
    max(case when series_id = 'cpi_component_cp04' then cpi_index end) as cpi_housing_utilities,
    max(case when series_id = 'cpi_component_cp05' then cpi_index end) as cpi_household_furnishings,
    max(case when series_id = 'cpi_component_cp06' then cpi_index end) as cpi_health,
    max(case when series_id = 'cpi_component_cp07' then cpi_index end) as cpi_transport,
    max(case when series_id = 'cpi_component_cp08' then cpi_index end) as cpi_communication,
    max(case when series_id = 'cpi_component_cp09' then cpi_index end) as cpi_recreation_culture,
    max(case when series_id = 'cpi_component_cp10' then cpi_index end) as cpi_education,
    max(case when series_id = 'cpi_component_cp11' then cpi_index end) as cpi_restaurants_hotels,
    max(case when series_id = 'cpi_component_cp12' then cpi_index end) as cpi_misc_goods_services
from cpi_with_changes
group by month_start, country
