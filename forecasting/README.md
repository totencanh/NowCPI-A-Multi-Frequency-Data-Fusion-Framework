# NowCPI MIDAS forecasting

`midas_nowcast.py` adapts the mixed-frequency approach in `midas_capstone` to
NowCPI's Vietnam data and Gold tables. Its target is monthly headline CPI MoM.
The forecast target is one month after the latest CPI month actually available,
following the prototype's one-step-ahead expanding-window design. It does not
force the machine's calendar month as the target. The feature window is aligned
to that target month and excludes Brent observations after its end. The main
estimate is AR-MIDAS, which combines the latest actual CPI MoM with 12 weekly
Brent log returns. A CPI-only estimate is a fallback if the complete Brent lag
block is unavailable; a Brent-only estimate is not promoted as the NowCPI
forecast.

The model reads `delta.gold.gold_nowcasting_labels` and
`delta.gold.gold_brent_daily` through Trino. After the Gold dbt models finish,
both Airflow pipeline DAGs run this script and write:

- `outputs/midas_current_nowcast.json`: one-step-ahead CPI MoM estimate after
  the latest actual CPI month, target-aligned feature cutoff, source availability,
  model selection, and processing start/end/duration.
- `outputs/midas_backtest_predictions.csv`: expanding-window historical predictions.
- `outputs/midas_backtest_metrics.csv`: MAE, RMSE, and directional accuracy.

Historical rows use market observation dates and calendar month-end cutoffs.
The lakehouse does not preserve historical source-release vintages, so those
backtest results are diagnostic and must not be described as vintage-accurate
real-time performance. For a current run, the script freezes the information
cutoff before reading the tables and independently filters Brent and CPI by
their stored availability timestamps. If no usable latest CPI lag is available,
the script reports no CPI forecast instead of returning a Brent-only number.
For a past target month, source release vintages are not preserved, so the result
is a delayed one-step estimate using currently available records bounded by the
target month's observation window, not a real-time historical vintage.
`processing_duration_seconds` reports query, preparation, and forecast
computation time, separate from the data cutoff and target month.

## Latest verified pipeline run

The full `nowcpi_daily_pipeline` run `manual__2026-10-17T08:47:02+00:00`
completed successfully through collectors, Kafka publication, Bronze catch-up,
Silver, dbt staging/Gold, and MIDAS. Its output in
`outputs/midas_current_nowcast.json` is:

- Latest actual CPI month: **2026-07**.
- One-step target month: **2026-08** (the month after the latest actual CPI).
- AR-MIDAS Ridge estimate: **0.181355% CPI MoM**.
- Model processing duration: **2.377 seconds**.
- Latest CPI observation age at run time: **3 months**.

This is a delayed estimate for August, not a nowcast for the calendar month of
October. The run cutoff was 2026-10-17 UTC; the CPI label for July was available
by then, while the target is still constrained to the month immediately after
that CPI observation. Results change when a later CPI observation becomes
available. The stored JSON is the source of truth for the latest run values.

USD/VND, domestic fuel, CPI components, and annual industrial activity are
available for Gold analysis and dashboards but are not active predictors in
this nowcast. News collection and NLP are not implemented. The standalone
CPI-only and Brent-only MIDAS predictions remain diagnostics; the reported
NowCPI estimate prioritizes the CPI-plus-Brent AR-MIDAS model.

Forecast comparison uses the same expanding-window months for the CPI-only and
MIDAS variants. Metrics are diagnostic because historical source-release
vintages are not preserved. The complete seminar scope, model roles, and claim
limitations are described in `docs/seminar_scope.md`.
