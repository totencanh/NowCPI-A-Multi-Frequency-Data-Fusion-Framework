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
    available_at,
    available_at_basis
from {{ source('silver', 'iip_observations') }}
where series_id = 'iip_total_index'
  and frequency = 'monthly'
