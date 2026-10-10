select
    series_id,
    country,
    observation_date,
    frequency,
    value,
    unit,
    source,
    source_record_id,
    source_release_ts,
    source_ingested_at,
    available_at
from {{ source('silver', 'brent_oil_observations') }}
where series_id = 'brent_front_month'
  and frequency = 'daily'
