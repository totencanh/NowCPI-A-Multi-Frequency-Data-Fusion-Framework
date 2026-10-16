# NowCPI MIDAS forecasting

`midas_nowcast.py` adapts the mixed-frequency approach in `midas_capstone` to
NowCPI's Vietnam data and Gold tables. Its target is monthly headline CPI MoM.
The current nowcast uses the 12 latest weekly Brent log returns derived from
daily observations as its active predictor block.
The weekly returns enter as separate regressors in a ridge-regularized
U-MIDAS model. When the previous month's CPI has been released, the script also
evaluates CPI-only and autoregressive MIDAS variants. Missing recent CPI is
reported in the current-nowcast JSON rather than treated as a pipeline crash.

The model reads `delta.gold.gold_nowcasting_labels` and
`delta.gold.gold_brent_daily` through Trino. After the Gold dbt models finish,
both Airflow pipeline DAGs run this script and write:

- `outputs/midas_current_nowcast.json`: current month's CPI MoM nowcast, frozen
  data cutoff, source availability coverage, and processing start/end/duration.
- `outputs/midas_backtest_predictions.csv`: expanding-window historical predictions.
- `outputs/midas_backtest_metrics.csv`: MAE, RMSE, and directional accuracy.

Historical rows use market observation dates and calendar month-end cutoffs.
The lakehouse does not preserve historical source-release vintages, so those
backtest results are diagnostic and must not be described as vintage-accurate
real-time performance. For a current run, the script freezes the information
cutoff before reading the tables and independently filters Brent and CPI by
their stored availability timestamps. A source record arriving after that
cutoff is deferred to the next run; missing CPI releases do not block a Brent
MIDAS estimate. `processing_duration_seconds` reports query, preparation, and
forecast computation time, separate from the data cutoff and target month.

USD/VND, domestic fuel, CPI components, and annual industrial activity are
available for Gold analysis and dashboards but are not active predictors in
this nowcast. News collection and NLP are not implemented. The CPI-only and
AR-MIDAS variants are optional benchmarks/variants; they only produce a current
forecast when the previous month's CPI label is available by the cutoff.

Forecast comparison uses the same expanding-window months for the CPI-only and
MIDAS variants. Metrics are diagnostic because historical source-release
vintages are not preserved. The complete seminar scope, model roles, and claim
limitations are described in `docs/seminar_scope.md`.
