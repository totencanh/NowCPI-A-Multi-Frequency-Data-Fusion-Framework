{{ config(materialized='incremental', incremental_strategy='append') }}

with latest_cpi_month as (
    select max(month_start) as month_start
    from {{ ref('gold_cpi_monthly') }}
    where cpi_headline is not null
), current_target as (
    select date_add('month', 1, month_start) as target_month
    from latest_cpi_month
), current_features as (
    select features.*
    from {{ ref('gold_nowcasting_features') }} as features
    inner join current_target
        on features.target_month = current_target.target_month
), snapshot_cutoffs as (
    select
        features.*,
        features.gold_built_at as asof_cutoff_ts,
        current_timestamp as snapshot_recorded_at
    from current_features as features
), eligibility as (
    select
        *,
        cpi_latest_available_at is not null
            and cpi_latest_available_at <= asof_cutoff_ts
            and cpi_observation_month < target_month as cpi_asof_eligible,
        brent_latest_available_at is not null
            and brent_latest_available_at <= asof_cutoff_ts
            and brent_latest_observation_date <= cast(asof_cutoff_ts as date)
            as brent_asof_eligible,
        fx_latest_available_at is not null
            and fx_latest_available_at <= asof_cutoff_ts
            and fx_latest_observation_date <= cast(asof_cutoff_ts as date)
            as fx_asof_eligible,
        fuel_latest_available_at is not null
            and fuel_latest_available_at <= asof_cutoff_ts
            and fuel_latest_observation_date <= cast(asof_cutoff_ts as date)
            as fuel_asof_eligible,
        iip_available_at is not null
            and iip_available_at <= asof_cutoff_ts
            and iip_observation_month < target_month as iip_asof_eligible
    from snapshot_cutoffs
)
select
    concat(gold_build_id, ':', cast(target_month as varchar), ':', country)
        as feature_snapshot_id,
    gold_build_id as feature_build_id,
    snapshot_recorded_at,
    asof_cutoff_ts,
    target_month,
    country,
    feature_vintage_mode,
    cpi_observation_month,
    cpi_observation_age_months,
    cpi_latest_available_at as cpi_available_at,
    cpi_available_at_basis as cpi_available_at_basis,
    cpi_headline_index_latest,
    cpi_headline_mom_latest,
    cpi_headline_yoy_latest,
    cpi_asof_eligible,
    brent_usd_per_barrel_avg_mtd,
    brent_observation_count_mtd,
    brent_latest_observation_date,
    brent_latest_available_at as brent_available_at,
    brent_latest_available_at_basis as brent_available_at_basis,
    brent_asof_eligible,
    vnd_per_usd_avg_mtd,
    fx_observation_count_mtd,
    fx_latest_observation_date,
    fx_latest_available_at as fx_available_at,
    fx_latest_available_at_basis as fx_available_at_basis,
    fx_asof_eligible,
    fuel_ron95_vnd_per_liter_avg_mtd,
    fuel_e10_ron95_vnd_per_liter_avg_mtd,
    fuel_observation_count_mtd,
    fuel_latest_observation_date,
    fuel_latest_available_at as fuel_available_at,
    fuel_latest_available_at_basis as fuel_available_at_basis,
    fuel_asof_eligible,
    iip_observation_month,
    iip_observation_age_months,
    iip_yoy_growth_latest_prior_month,
    iip_available_at,
    iip_available_at_basis,
    iip_asof_eligible,
    cast(100.0 * (
        case when cpi_asof_eligible then 1 else 0 end
        + case when brent_asof_eligible then 1 else 0 end
        + case when fx_asof_eligible then 1 else 0 end
        + case when fuel_asof_eligible then 1 else 0 end
        + case when iip_asof_eligible then 1 else 0 end
    ) / 5 as double) as asof_feature_source_coverage_pct
from eligibility
