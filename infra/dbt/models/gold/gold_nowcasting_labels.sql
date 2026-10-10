select
    month_start as target_month,
    country,
    cpi_headline as target_cpi_index,
    cpi_headline_mom_percent as target_cpi_mom_percent,
    cpi_headline_yoy_percent as target_cpi_yoy_percent,
    cpi_available_at as label_available_at
from {{ ref('gold_cpi_monthly') }}
where cpi_headline is not null
