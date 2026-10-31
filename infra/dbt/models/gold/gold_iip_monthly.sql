with indexed as (
    select
        series_id,
        country,
        observation_date as month_start,
        value as iip_index,
        unit,
        source,
        source_record_id,
        source_release_ts,
        available_at,
        available_at_basis
    from {{ ref('stg_iip') }}
), compared as (
    select
        current_iip.*,
        previous.iip_index as iip_index_same_month_previous_year
    from indexed as current_iip
    left join indexed as previous
        on previous.country = current_iip.country
       and previous.month_start = date_add('month', -12, current_iip.month_start)
)
select
    series_id,
    country,
    month_start,
    iip_index,
    iip_index_same_month_previous_year,
    case
        when iip_index_same_month_previous_year > 0
        then 100.0 * (iip_index / iip_index_same_month_previous_year - 1.0)
    end as iip_yoy_growth_percent,
    unit,
    source,
    source_record_id,
    source_release_ts,
    available_at,
    available_at_basis
from compared
