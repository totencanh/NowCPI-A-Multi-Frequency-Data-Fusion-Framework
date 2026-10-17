# NowCPI Gold dashboard tables

Gold contains stable subject-specific tables for dashboards and a monthly,
multi-source feature table for CPI analysis. The subject tables keep their own
natural grain; the feature table aligns data to each CPI target month.

| Table | Grain | Contents |
| --- | --- | --- |
| `gold_cpi_monthly` | Country and month | Headline CPI, headline MoM/YoY, and one column for each of the 12 CPI groups |
| `gold_brent_daily` | Country and day | Brent price per barrel |
| `gold_usd_vnd_daily` | Country and day | VND per USD |
| `gold_fuel_daily` | Country, day, and fuel type | RON 95 and E10 RON 95 prices per liter |
| `gold_iip_annual` | Country and year | Annual IIP growth |
| `gold_nowcasting_features` | Country and target month | Latest CPI observation before target month with its age, target-month Brent/USD/VND/fuel summaries, prior-year IIP, and five-source coverage percentage; contains no target labels |
| `gold_nowcasting_labels` | Country and CPI target month | Observed headline CPI index, MoM, YoY, and when the label became available; join to features only for training/evaluation |
| `gold_powerbi_monthly_overview` | Country and month | CPI headline and monthly Brent, USD/VND, RON95, and E10 summaries with source counts/freshness |
| `gold_powerbi_cpi_components` | Country, component, and month | CPI component index and exact calendar MoM/YoY changes |
| `gold_powerbi_market_daily` | Country, indicator, and day | Long-form daily Brent, USD/VND, and fuel observations with units |

`gold_nowcasting_features` creates monthly target rows from the earliest
available headline CPI month through the current month. It carries the latest
CPI observation before each target month and exposes its age in months, so a
delayed monthly CPI release does not erase all CPI signal. Historical market
features use observation-date windows; the current target month uses data
observed through the current date. Source release vintages are not archived for
all inputs, so historical rows are observation-time pseudo-vintages, not
publication-time-accurate snapshots. The model/report must disclose this when
interpreting backtests.

The CPI MoM and YoY rates are calculated against the matching month one and
twelve months earlier. CPI groups remain separate columns in the monthly table.
Brent, exchange rates, fuel, and annual IIP stay in separate subject tables
because they have different meanings and grains. Fuel grades share one
long-form table with an explicit `fuel_type`. The feature table exposes a
`feature_source_coverage_pct` across CPI, Brent, USD/VND, fuel, and prior-year
IIP; this is source availability, not model accuracy.
The current MIDAS script directly reads CPI labels and Brent Gold history
through Trino; it does not consume every column in the feature table.

## dbt layout

- `models/staging`: source-oriented typed views over Silver.
- `models/gold`: subject, dashboard, and nowcasting tables.

The Power BI models are reporting datasets. Their existence does not mean that
all available sources are predictors in the active Brent U-MIDAS model. See
`analytics/powerbi/README.md` for the Trino connection and suggested report
pages.

The `nowcpi_daily_pipeline` Airflow DAG runs staging and then Gold after Silver
completes. Trigger `nowcpi_dbt_pipeline` for a standalone manual rebuild, or run
`dbt run` in the Airflow environment.
