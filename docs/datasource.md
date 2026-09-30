# NowCPI data sources

The collectors currently write JSON snapshots under `data/raw/`. They have
not yet been connected to Kafka.

| File | Source | Series | Frequency / scope |
|---|---|---|---|
| `imf_cpi_vietnam.json` | IMF SDMX | Headline CPI | Monthly; Vietnam |
| `imf_cpi_components_vietnam.json` | IMF SDMX | COICOP CPI components | Monthly; Vietnam |
| `brent_oil_daily.json` | Yahoo Finance | Brent front-month futures (`BZ=F`) | Daily |
| `usd_vnd_daily.json` | Yahoo Finance | USD/VND (`VND=X`) | Daily |
| `vn_fuel_e10_ron95.json` | giaxanghomnay API | E10 RON 95-V domestic fuel price | Daily when reported |
| `worldbank_ppi_iip_vietnam.json` | World Bank Indicators API | PPI and industry growth series | Annual |

The World Bank collector labels `NV.IND.TOTL.KD.ZG` as IIP growth, but the
[World Bank defines it](https://data.worldbank.org/indicator/NV.IND.TOTL.KD.ZG?view=chart)
as industry (including construction) value-added annual growth. Treat it as an
annual industrial activity proxy, not as an industrial production index. The
source collector is left unchanged; replace or relabel this series before
interpreting it as IIP.

News collection is intentionally unimplemented. Source responses are retained
inside each event's `raw_payload`; the domestic fuel collector stores only the
selected product rather than archiving every API response.
