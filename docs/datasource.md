# NowCPI data sources

Collectors write JSON under `data/raw/`. Existing flat files are retained as
the initial baseline. Subsequent runs write immutable batches under a folder
named after the file stem, for example
`data/raw/brent_oil_daily/batch_<UTC timestamp>.json`. A batch contains only
new or revised events; unchanged observations are not copied into every run.
Runs with no new or revised events do not create a file.
Airflow runs the collectors daily and publishes newly created batches to their
`nowcpi.*` Kafka topics. The Kafka publisher preserves each event envelope and
uses a persistent batch-hash manifest to avoid replaying unchanged files.

| File | Source | Series | Frequency / scope |
|---|---|---|---|
| `imf_cpi_vietnam.json` | IMF SDMX | Headline CPI | Monthly; Vietnam |
| `imf_cpi_components_vietnam.json` | IMF SDMX | COICOP CPI components | Monthly; Vietnam |
| `brent_oil_daily.json` | Yahoo Finance | Brent front-month futures (`BZ=F`) | Daily |
| `usd_vnd_daily.json` | Yahoo Finance | USD/VND (`VND=X`) | Daily |
| `vn_fuel_e10_ron95.json` | giaxanghomnay API | All current-day fuel product records and source price fields | Daily when reported |
| `worldbank_iip_vietnam.json` | World Bank Indicators API | Industrial production growth proxy | Annual |

The World Bank collector labels `NV.IND.TOTL.KD.ZG` as IIP growth, but the
[World Bank defines it](https://data.worldbank.org/indicator/NV.IND.TOTL.KD.ZG?view=chart)
as industry (including construction) value-added annual growth. Treat it as an
annual industrial activity proxy, not as an industrial production index. The
pipeline keeps this growth proxy under the existing IIP series name; PPI
collection has been removed.

News collection is intentionally unimplemented. Each event retains its source
row/object in `raw_payload`; the domestic fuel collector keeps current-day
product records and excludes the API's repeated previous-day groups.
