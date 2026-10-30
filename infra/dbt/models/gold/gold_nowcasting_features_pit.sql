-- Point-in-time safe projection of the descriptive Gold feature store.
-- Historical rows are expected to have null predictors until source versions
-- are archived. Never backfill values that were not available at the cutoff.
with base as (
    select
        *,
        with_timezone(try_cast(feature_cutoff_at as timestamp), 'UTC')
            as asof_cutoff_ts
    from {{ ref('gold_nowcasting_features') }}
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
            and iip_observation_year < year(target_month)
            as iip_asof_eligible
    from base
)
select
    target_month,
    country,
    gold_build_id,
    gold_built_at,
    asof_cutoff_ts,
    feature_vintage_mode,
    case when cpi_asof_eligible then cpi_observation_month end
        as cpi_observation_month_asof,
    case when cpi_asof_eligible then cpi_headline_mom_latest end
        as cpi_headline_mom_asof,
    case when cpi_asof_eligible then cpi_latest_available_at end
        as cpi_available_at_asof,
    cpi_asof_eligible,
    case when brent_asof_eligible then brent_usd_per_barrel_avg_mtd end
        as brent_usd_per_barrel_avg_mtd_asof,
    case when brent_asof_eligible then brent_observation_count_mtd else 0 end
        as brent_observation_count_mtd_asof,
    case when brent_asof_eligible then brent_latest_observation_date end
        as brent_latest_observation_date_asof,
    case when brent_asof_eligible then brent_latest_available_at end
        as brent_available_at_asof,
    brent_asof_eligible,
    case when fx_asof_eligible then vnd_per_usd_avg_mtd end
        as vnd_per_usd_avg_mtd_asof,
    case when fx_asof_eligible then fx_observation_count_mtd else 0 end
        as fx_observation_count_mtd_asof,
    case when fx_asof_eligible then fx_latest_observation_date end
        as fx_latest_observation_date_asof,
    case when fx_asof_eligible then fx_latest_available_at end
        as fx_available_at_asof,
    fx_asof_eligible,
    case when fuel_asof_eligible then fuel_ron95_vnd_per_liter_avg_mtd end
        as fuel_ron95_vnd_per_liter_avg_mtd_asof,
    case when fuel_asof_eligible then fuel_e10_ron95_vnd_per_liter_avg_mtd end
        as fuel_e10_ron95_vnd_per_liter_avg_mtd_asof,
    case when fuel_asof_eligible then fuel_observation_count_mtd else 0 end
        as fuel_observation_count_mtd_asof,
    case when fuel_asof_eligible then fuel_latest_observation_date end
        as fuel_latest_observation_date_asof,
    case when fuel_asof_eligible then fuel_latest_available_at end
        as fuel_available_at_asof,
    fuel_asof_eligible,
    case when iip_asof_eligible then iip_observation_year end
        as iip_observation_year_asof,
    case when iip_asof_eligible then iip_growth_latest_prior_year end
        as iip_growth_latest_prior_year_asof,
    case when iip_asof_eligible then iip_available_at end
        as iip_available_at_asof,
    iip_asof_eligible,
    cast(100.0 * (
        case when cpi_asof_eligible then 1 else 0 end
        + case when brent_asof_eligible then 1 else 0 end
        + case when fx_asof_eligible then 1 else 0 end
        + case when fuel_asof_eligible then 1 else 0 end
        + case when iip_asof_eligible then 1 else 0 end
    ) / 5 as double) as asof_feature_source_coverage_pct,
    not (
        (cpi_asof_eligible and cpi_latest_available_at > asof_cutoff_ts)
        or (brent_asof_eligible and brent_latest_available_at > asof_cutoff_ts)
        or (fx_asof_eligible and fx_latest_available_at > asof_cutoff_ts)
        or (fuel_asof_eligible and fuel_latest_available_at > asof_cutoff_ts)
        or (iip_asof_eligible and iip_available_at > asof_cutoff_ts)
    ) as asof_check_passed
from eligibility
