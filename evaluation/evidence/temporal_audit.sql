WITH records AS (
    SELECT 'CPI' AS source, source_release_ts, available_at
    FROM delta.silver.cpi_observations
    UNION ALL
    SELECT 'BRENT', source_release_ts, available_at
    FROM delta.silver.brent_oil_observations
    UNION ALL
    SELECT 'FX', source_release_ts, available_at
    FROM delta.silver.usd_vnd_observations
    UNION ALL
    SELECT 'FUEL', source_release_ts, available_at
    FROM delta.silver.vn_fuel_observations
    UNION ALL
    SELECT 'IIP', source_release_ts, available_at
    FROM delta.silver.iip_observations
)
SELECT
    source,
    count(*) AS total_records,
    count_if(source_release_ts IS NULL) AS missing_release_ts,
    count_if(available_at IS NULL) AS missing_available_at,
    count_if(
        source_release_ts IS NOT NULL
        AND available_at < source_release_ts
    ) AS timestamp_order_violations
FROM records
GROUP BY source
ORDER BY source
