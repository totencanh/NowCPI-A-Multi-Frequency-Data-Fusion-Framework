select
    series_id,
    country,
    observation_date,
    frequency,
    value as vnd_per_usd,
    unit,
    source,
    source_record_id,
    source_release_ts,
    source_ingested_at,
    available_at
from {{ source('silver', 'usd_vnd_observations') }}
where series_id = 'usd_vnd'
  and frequency = 'daily'
