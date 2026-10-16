with monthly as (
    select
        series_id,
        component_name,
        country,
        cast(date_trunc('month', observation_date) as date) as month_start,
        component_index,
        available_at,
        row_number() over (
            partition by series_id, country, cast(date_trunc('month', observation_date) as date)
            order by available_at desc nulls last, observation_date desc
        ) as revision_rank
    from {{ ref('stg_cpi_components') }}
), deduplicated as (
    select * from monthly where revision_rank = 1
), with_changes as (
    select
        current.series_id,
        current.component_name,
        current.country,
        current.month_start,
        current.component_index,
        current.available_at,
        100.0 * (current.component_index - previous.component_index)
            / nullif(previous.component_index, 0) as component_mom_percent,
        100.0 * (current.component_index - prior_year.component_index)
            / nullif(prior_year.component_index, 0) as component_yoy_percent
    from deduplicated as current
    left join deduplicated as previous
        on previous.series_id = current.series_id
       and previous.country = current.country
       and previous.month_start = date_add('month', -1, current.month_start)
    left join deduplicated as prior_year
        on prior_year.series_id = current.series_id
       and prior_year.country = current.country
       and prior_year.month_start = date_add('month', -12, current.month_start)
)
select
    series_id,
    component_name,
    country,
    month_start,
    component_index,
    component_mom_percent,
    component_yoy_percent,
    available_at
from with_changes
