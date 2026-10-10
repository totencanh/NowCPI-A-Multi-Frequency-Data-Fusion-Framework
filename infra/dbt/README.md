# NowCPI Gold dashboard tables

Gold contains only stable, subject-specific tables for dashboard use. It has no
shared dimensions, catch-all fact table, or intermediate marts. Each table
keeps its own natural grain and only the fields needed to analyze that subject.

| Table | Grain | Contents |
| --- | --- | --- |
| `gold_cpi_monthly` | Country and month | Headline CPI, headline MoM/YoY, and one column for each of the 12 CPI groups |
| `gold_brent_daily` | Country and day | Brent price per barrel |
| `gold_usd_vnd_daily` | Country and day | VND per USD |
| `gold_fuel_daily` | Country, day, and fuel type | RON 95 and E10 RON 95 prices per liter |
| `gold_iip_annual` | Country and year | Annual IIP growth |

The CPI MoM and YoY rates are calculated against the matching month one and
twelve months earlier. CPI groups remain separate columns in the monthly table.
Brent, exchange rates, fuel, and annual IIP stay in separate subject tables
because they have different meanings and grains. Fuel grades share one
long-form table with an explicit `fuel_type`, avoiding duplicate table schemas.

## dbt layout

- `models/staging`: source-oriented typed views over Silver.
- `models/gold`: the five dashboard tables listed above.

The `nowcpi_daily_ingestion` Airflow DAG runs staging and then Gold after Silver
completes. Trigger `nowcpi_dbt_pipeline` for a standalone manual rebuild, or run
`dbt run` in the Airflow environment.
