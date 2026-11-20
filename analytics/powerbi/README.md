# Power BI for NowCPI

Power BI reads curated Gold tables through Trino. It should not read raw JSON
batches, Kafka topics, or Delta files directly. Power BI Desktop is a client
application; it is not a Docker Compose service.

## Connect Power BI Desktop to local Trino

For the project's local open-source Trino deployment, use the community
Power BI Trino custom connector (`.mez`) rather than the generic ODBC route.
The Starburst ODBC driver is intended for Starburst Galaxy/Enterprise and can
send syntax that a plain Trino OSS server rejects. The custom connector talks
to Trino's client REST API. It is community-maintained and is not a Microsoft-
certified connector.

### One-time setup

1. Start Trino from the project root and confirm the service is running:

   ```powershell
   docker compose up -d trino
   docker compose ps trino
   ```

   The local connection uses `http://localhost:8080`.

2. Download the prebuilt `Trino.mez` from the **link in step 6** of the
   [PowerBITrinoConnector README](https://github.com/CreativeDataEU/PowerBITrinoConnector/blob/master/README.md).
   Downloading the repository ZIP gives you source code, not necessarily the
   prebuilt `.mez` file. The Power Query SDK is only needed if you intend to
   modify or build the connector yourself.

3. Create this folder if it does not exist, then copy `Trino.mez` into it:

   ```text
   %USERPROFILE%\Documents\Power BI Desktop\Custom Connectors
   ```

   If Windows redirects Documents to OneDrive, create the same subfolders
   inside that Documents folder.

4. In Power BI Desktop, open **File → Options and settings → Options →
   Security → Data Extensions**. Allow the custom connector to load (the exact
   option can vary by Power BI version). This permits an unsigned community
   connector, so only load the connector you downloaded from the project linked
   above.

5. Close and restart Power BI Desktop.

### Connect and load Gold tables

1. Select **Home → Get data → More… → Other**, search for **Trino**, select it,
   and choose **Connect**.
2. Enter the connector parameters:

   | Parameter | Local NowCPI value |
   |---|---|
   | Host | `localhost` |
   | Port | `8080` |
   | Catalog | `delta` |
   | User | `nowcpi` |
   | SQL Query | Leave blank to browse tables |

   Leave optional retry/timeout parameters at their defaults initially.
3. When Power BI asks for authentication, choose **Anonymous** for the current
   local Trino configuration, which does not require a password. If Trino's
   authentication configuration changes, select the matching method instead.
4. In Navigator, expand `delta` → `gold`, select the needed tables, then choose
   **Load** (or **Transform Data** to inspect them first).

If the Trino connector does not appear, check the `.mez` location, the Data
Extensions setting, and restart Power BI Desktop. If the catalog or tables do
not appear, verify Trino is healthy and that the dbt Gold models have completed.
The latest nowcast JSON is a local output and is not currently exposed as a
Trino Gold table.

## Gold datasets

| Gold table | Grain | Suggested visuals |
|---|---|---|
| `gold_powerbi_monthly_overview` | Country-month | CPI MoM/YoY cards and trends; monthly Brent, USD/VND, RON95 and E10 summaries; observation counts |
| `gold_powerbi_cpi_components` | Country-component-month | Component MoM/YoY bars, index trends, component slicer |
| `gold_powerbi_market_daily` | Country-indicator-day | Daily Brent, exchange-rate and fuel trends with indicator/date slicers |
| `gold_iip_monthly` | Country-month | Monthly Vietnam IIP index and year-over-year growth |
| `gold_nowcasting_features` | Country-target month | Prior CPI and its age, market/IIP inputs, source coverage percentage |

The monthly overview combines observed month-country keys and left joins each
source independently. Missing values remain null to represent different
release delays. CPI component changes are computed within each component
series. IIP remains at its own monthly grain.

## Suggested report pages

- **Inflation overview:** headline CPI MoM/YoY cards and monthly trend.
- **CPI components:** component MoM/YoY clustered bars and index trend.
- **Market inputs:** daily Brent, USD/VND, and fuel prices; monthly averages.
- **Industrial production:** monthly Vietnam IIP index and year-over-year growth.
- **Nowcast context:** show the primary estimate, target month, selected model,
  and data cutoff from `forecasting/outputs/midas_current_nowcast.json`. This is
  a local Airflow output, not a Trino table, so import it separately if you want
  it in the report.

## Refresh and caveats

With **Import** mode, Power BI stores a copy of the selected tables in the
`.pbix` model. After the daily Airflow/dbt Gold tasks complete, use **Refresh**
in Power BI Desktop to fetch updated rows; do not re-import the tables. For
automatic cloud refresh, publish the report and configure a gateway that can
reach the local Trino service. A community custom connector may require
additional gateway setup and is not automatically supported by Power BI
Service.

The Gold tables preserve `available_at` where source timing matters; use it for
freshness indicators and do not interpret null as zero. The first available CPI
component month has null MoM/YoY because a prior observation is required.
