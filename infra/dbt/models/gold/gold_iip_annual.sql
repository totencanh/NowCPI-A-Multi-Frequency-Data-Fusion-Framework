select
    year(observation_date) as year,
    country,
    value as iip_growth,
    available_at
from {{ ref('stg_iip') }}
