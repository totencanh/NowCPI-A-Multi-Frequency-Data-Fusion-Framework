select
    series_id,
    case replace(series_id, 'cpi_component_', '')
        when 'cp01' then 'Food and non-alcoholic beverages'
        when 'cp02' then 'Alcoholic beverages, tobacco and narcotics'
        when 'cp03' then 'Clothing and footwear'
        when 'cp04' then 'Housing, water, electricity, gas and other fuels'
        when 'cp05' then 'Furnishings, household equipment and routine household maintenance'
        when 'cp06' then 'Health'
        when 'cp07' then 'Transport'
        when 'cp08' then 'Communication'
        when 'cp09' then 'Recreation and culture'
        when 'cp10' then 'Education'
        when 'cp11' then 'Restaurants and hotels'
        when 'cp12' then 'Miscellaneous goods and services'
        else 'Unknown CPI component'
    end as component_name,
    country,
    observation_date,
    frequency,
    value as component_index,
    unit,
    source,
    source_record_id,
    source_release_ts,
    source_ingested_at,
    available_at
from {{ source('silver', 'cpi_component_observations') }}
where frequency = 'monthly'
