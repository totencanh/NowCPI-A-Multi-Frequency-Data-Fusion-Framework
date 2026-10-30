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
- `outputs/midas_backtest_metrics.csv`: naïve last-CPI, unregularized AR(1),
  CPI-only Ridge, Brent-only U-MIDAS, fixed AR-MIDAS, and validation-selected
  AR-MIDAS plus a validation-selected CPI-preserving AR-MIDAS/U-MIDAS blend,
  scored on identical common folds. It includes all common
  origins, the latest 36-origin research window, its latest-24 subwindow, and a
  descriptive latest-nine window.
- `outputs/midas_paired_comparisons.csv`: paired MAE/RMSE differences against
  last-CPI naive, relative uplift, circular block-bootstrap 95% intervals, and
  directional-accuracy deltas and Diebold–Mariano p-values for all, latest-36,
  latest-24, and descriptive latest-nine windows. DM p-values are omitted below
  24 paired origins.
- `outputs/midas_failure_cases.csv`: up to three largest paired AR-MIDAS error
  cases, with wrong-direction and worse-than-naive labels.
- `outputs/release_timestamp_audit.csv`: per-source coverage of source release
  timestamps and separate counts/bases for pipeline availability timestamps.
  Ingestion time is never substituted for a source release time.

Historical rows use an explicit pseudo-vintage: prior-month CPI is assumed to be
available by day 15 of the target month, daily Brent is assumed available on the
following UTC day, and the information cutoff is target-month end plus one day.
The model records target CPI `source_release_ts` when available. Brent currently
has no source release timestamp, and the lakehouse does not preserve historical
revisions. The release audit reports timestamp coverage; until both sources have
at least 24 source-timestamped records and vintage/revision history is preserved,
the output explicitly disallows real-time performance claims and remains a
labelled pseudo-vintage diagnostic, not proof of vintage-accurate performance.
AR-MIDAS lag count and ridge penalty are
selected only inside each outer fold's expanding validation window, using at most
the most recent 12 prior validation origins to bound compute time. For a current
run, the script freezes the information
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

Forecast comparison uses the same expanding-window folds for naïve last-CPI,
CPI-only Ridge, Brent-only U-MIDAS, and the active AR-MIDAS model. The recent
nine-month window is a subset of these common folds. The separate
`outputs/simulated_metrics.csv` is a nine-month feature-ablation exercise with a
different model set; it does not score the active AR-MIDAS forecast and must not
be used as its direct comparison. Metrics are diagnostic because historical
source-release vintages are not preserved. The complete seminar scope, model
roles, and claim limitations are described in `docs/seminar_scope.md`.

## Multi-source and Elastic Net ablation

The Gold layer also exposes `gold_nowcasting_features_pit`, a conservative
projection that nulls features when either `available_at` or the observation
period is later than the feature cutoff. `gold_nowcasting_features_pit_integrity`
is run in the daily DAG before the standalone feature ablation task.

`benchmark/evaluate_feature_ablation.py` compares last-CPI naive, AR(1),
CPI-only Ridge, Brent-only U-MIDAS, AR-MIDAS Ridge, MIDAS Elastic Net, Lasso,
Huber, and full multi-source Ridge/Elastic Net with the same rolling-origin
targets. It records nested-validation hyperparameters, paired CI/DM results,
per-source as-of coverage, model runtime, peak RSS, model-fold failures, and
the three largest forecast-error cases for each model. Outputs use the
`feature_ablation_` prefix in `outputs/`.

In the 2026-10-30 run, extra-source eligibility was 0/140 historical target
months because the stored source `available_at` timestamps post-date those
historical cutoffs. The current month is available, but lacks a realized CPI
label for backtesting. Thus full multi-source scores effectively reduce to
AR-MIDAS and do not demonstrate incremental source value. Brent-only U-MIDAS
had a slightly lower latest-36 MAE (0.1998 vs 0.2028), but its paired interval
crosses zero and it reverses on the latest 24 months. Elastic Net and Lasso
had higher latest-36 MAE; Huber was close to Ridge. Keep AR-MIDAS Ridge in
production until point-in-time raw snapshots accumulate. Detailed comparisons
are in `../docs/README_CAI_TIEN_2.md`.
