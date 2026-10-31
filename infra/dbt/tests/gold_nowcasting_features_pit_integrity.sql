select *
from {{ ref('gold_nowcasting_features_pit') }}
where not asof_check_passed
   or asof_feature_source_coverage_pct < 0
   or asof_feature_source_coverage_pct > 100
   or (not cpi_asof_eligible and cpi_headline_mom_asof is not null)
   or (not brent_asof_eligible and brent_usd_per_barrel_avg_mtd_asof is not null)
   or (not fx_asof_eligible and vnd_per_usd_avg_mtd_asof is not null)
   or (not fuel_asof_eligible and fuel_e10_ron95_vnd_per_liter_avg_mtd_asof is not null)
   or (not iip_asof_eligible and iip_yoy_growth_latest_prior_month_asof is not null)
   or cpi_available_at_asof > asof_cutoff_ts
   or brent_available_at_asof > asof_cutoff_ts
   or fx_available_at_asof > asof_cutoff_ts
   or fuel_available_at_asof > asof_cutoff_ts
   or iip_available_at_asof > asof_cutoff_ts
   or brent_latest_observation_date_asof > cast(asof_cutoff_ts as date)
   or fx_latest_observation_date_asof > cast(asof_cutoff_ts as date)
   or fuel_latest_observation_date_asof > cast(asof_cutoff_ts as date)
   or iip_observation_month_asof >= target_month
