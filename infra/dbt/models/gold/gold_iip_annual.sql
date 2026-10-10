select
    year(observation_date) as year,
    country,
    value as iip_growth
from {{ ref('stg_iip') }}
