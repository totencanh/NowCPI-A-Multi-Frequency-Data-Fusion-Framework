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
    available_at,
    case
        when source_release_ts is not null then 'source_release_ts'
        when kafka_timestamp is not null and available_at = kafka_timestamp then 'kafka_timestamp'
        when bronze_ingested_at is not null and available_at = bronze_ingested_at then 'bronze_ingested_at'
        when source_ingested_at is not null and available_at = source_ingested_at then 'source_ingested_at'
        else 'unavailable'
    end as available_at_basis
from {{ source('silver', 'cpi_observations') }}
where series_id = 'cpi_headline'
  and frequency = 'monthly'
