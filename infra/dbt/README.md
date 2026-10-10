# NowCPI Gold dashboard tables

Gold contains stable subject-specific tables for dashboards and a monthly,
multi-source feature table for CPI nowcasting. The subject tables keep their
own natural grain; the nowcasting table aligns predictors to each CPI target month.

| Table | Grain | Contents |
| --- | --- | --- |
| `gold_cpi_monthly` | Country and month | Headline CPI, headline MoM/YoY, and one column for each of the 12 CPI groups |
| `gold_brent_daily` | Country and day | Brent price per barrel |
| `gold_usd_vnd_daily` | Country and day | VND per USD |
| `gold_fuel_daily` | Country, day, and fuel type | RON 95 and E10 RON 95 prices per liter |
| `gold_iip_annual` | Country and year | Annual IIP growth |
| `gold_nowcasting_features` | Country and target month | Lagged CPI, target-month market-to-date averages available by `feature_cutoff_at`, counts, and prior-year IIP; contains no target labels |
| `gold_nowcasting_labels` | Country and CPI target month | Observed headline CPI index, MoM, YoY, and when the label became available; join to features only for training/evaluation |

`gold_nowcasting_features` creates monthly target rows from January 2025 through
the current month. Historical rows use month-end as the feature cutoff; the
current month uses the current timestamp. High-frequency observations are
included only when both their observation date falls in the target month and
their `available_at` is no later than that cutoff. This is an as-of dataset;
late-arriving historical events are intentionally excluded from earlier
snapshots when their actual historical availability cannot be established.

The CPI MoM and YoY rates are calculated against the matching month one and
twelve months earlier. CPI groups remain separate columns in the monthly table.
Brent, exchange rates, fuel, and annual IIP stay in separate subject tables
because they have different meanings and grains. Fuel grades share one
long-form table with an explicit `fuel_type`. The nowcasting feature table
aggregates target-month daily market observations only when their observation
date is in that month and their `available_at` is no later than the cutoff.
Lagged CPI and prior-year IIP are subject to the same availability cutoff.

## dbt layout

- `models/staging`: source-oriented typed views over Silver.
- `models/gold`: the dashboard tables and the nowcasting feature table above.

The `nowcpi_daily_pipeline` Airflow DAG runs staging and then Gold after Silver
completes. Trigger `nowcpi_dbt_pipeline` for a standalone manual rebuild, or run
`dbt run` in the Airflow environment.
