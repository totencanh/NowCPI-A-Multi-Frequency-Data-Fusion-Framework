# Power BI for NowCPI

This folder describes the Power BI reporting layer. Power BI should query the
curated Gold tables through Trino; it should not read JSON batches, Kafka, or
Delta files directly.

## Connect

1. Start the NowCPI services, including Trino, with Docker Compose.
2. Install/configure a Trino ODBC driver and create a DSN with host
   `localhost`, port `8080`, catalog `delta`, schema `gold`, and user `nowcpi`.
   The local Trino setup does not require a password.
3. In Power BI Desktop, choose **Get data → ODBC**, select that DSN, and import
   the datasets below. Use Import mode for the seminar dashboard; refresh after
   the daily Airflow dbt tasks complete.

## Datasets

| Gold table | Grain | Suggested visuals |
|---|---|---|
| `gold_powerbi_monthly_overview` | Country-month | CPI headline MoM/YoY cards and trend lines; monthly Brent, USD/VND, RON95 and E10 averages; source observation counts |
| `gold_powerbi_cpi_components` | Country-component-month | Component MoM/YoY bar charts, component index trends, component slicer |
| `gold_powerbi_market_daily` | Country-indicator-day | Daily Brent, exchange-rate, RON95 and E10 trends with indicator and date slicers |
| `gold_iip_annual` | Country-year | Annual IIP growth trend and comparison with annual CPI summaries |
| `gold_nowcasting_features` | Country-target month | Latest prior CPI and its age, market/IIP inputs, and source coverage percentage |

The monthly overview uses a union of observed month-country keys and left joins
each source independently. Missing source values stay null, which lets visuals
show different reporting delays rather than silently discarding months. CPI
component growth rates are computed within each component series. IIP remains a
separate annual dataset to avoid repeating annual observations as if they were
monthly data.

## Suggested report pages

- **Inflation overview:** headline CPI MoM/YoY cards and monthly trend.
- **CPI components:** component MoM/YoY clustered bars and index trend.
- **Market inputs:** daily Brent, USD/VND and fuel prices; monthly averages.
- **Industrial production:** annual IIP growth, with a separate annual CPI view
  if comparing annual frequencies.
- **Nowcast context:** latest MIDAS estimate from
  `forecasting/outputs/midas_current_nowcast.json`, shown with its target month
  and data cutoff. This JSON file is a local Airflow output, not a Trino table.

## Refresh and caveats

The daily pipeline's dbt Gold selection includes these new models. Run that
pipeline after the model files are added, then refresh the Power BI dataset. The
tables preserve `available_at` fields where source timing matters; use them for
freshness indicators and do not interpret nulls as zero. Historical CPI
component changes require a prior observation, so the first available month
will have null MoM/YoY values.
