select *
from {{ ref('gold_nowcasting_features') }}
where feature_source_coverage_pct < 0
   or feature_source_coverage_pct > 100
   or cpi_observation_age_months < 1
   or brent_observation_age_days < 0
   or fx_observation_age_days < 0
   or fuel_observation_age_days < 0
   or iip_observation_age_years < 1
