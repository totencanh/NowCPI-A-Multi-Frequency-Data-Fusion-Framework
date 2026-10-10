select
    series_id,
    country,
    observation_date,
    frequency,
    value as cpi_index,
    unit,
    source,
    source_record_id,
    source_release_ts,
    source_ingested_at,
    available_at
from {{ source('silver', 'cpi_observations') }}
where series_id = 'cpi_headline'
  and frequency = 'monthly'
