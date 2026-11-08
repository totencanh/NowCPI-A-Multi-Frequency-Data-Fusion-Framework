# NowCPI forecasting

## Model roles

The **primary production model** is Full multi-source Ridge (`full_multisource_ridge`). It forecasts Vietnam headline CPI month-over-month (MoM) for the month immediately after the latest actual CPI month. Its candidate features include CPI lags, weekly Brent returns, Brent month-to-date, USD/VND, domestic fuel prices, NSO IIP growth, and source-missingness indicators. A source feature is used only when it passes the point-in-time observation and availability cutoffs.

**AR-MIDAS is a comparison model**, alongside naive last-CPI, AR(1), CPI-only Ridge, Brent-only U-MIDAS, Elastic Net, Lasso, and Huber variants. These models are retained to evaluate whether the multi-source setup adds value; they are not the primary-model label. If a full multi-source forecast cannot be estimated, the code may fall back to AR-MIDAS or CPI-only Ridge and records the actual model in `nowcast_model`.

The target month is never selected just from the machine's current date. If the latest observed CPI is September, the one-step target is October. The output records the information cutoff and model processing duration separately.

## Implementation and execution

Airflow invokes `multisource_nowcast.py` through the task `multisource_inflation_nowcast` after dbt Gold is ready. The script reads the PIT feature view and CPI labels through Trino, produces the current forecast and rolling-origin comparison outputs, and writes a versioned primary-model result to `outputs/production_models/full_multisource_ridge/latest.json`.

Some output filenames retain the historical `midas_` prefix for compatibility with existing reports and Power BI instructions. The prefix is an artifact naming convention; it does not designate AR-MIDAS as the primary model.

Key outputs:

- `outputs/production_models/full_multisource_ridge/latest.json`: latest primary-model result, selected model/version, predictors, cutoff, and runtime.
- `outputs/midas_current_nowcast.json`: compatibility copy of the current result; inspect `nowcast_model` to see whether the primary model or a fallback produced it.
- `outputs/midas_backtest_predictions.csv` and `outputs/midas_backtest_metrics.csv`: rolling-origin predictions and model comparison metrics.
- `outputs/midas_paired_comparisons.csv`: paired error differences, confidence intervals, and Diebold–Mariano results.
- `outputs/feature_ablation_*`: PIT source coverage, ablation results, model runtime/memory, and ineligible-fold records.
- `outputs/feature_ablation_sourcewise_proxy.csv` and `outputs/feature_ablation_sourcewise_proxy_predictions.csv`: separate pseudo-vintage sourcewise diagnostics using explicit availability assumptions. These do not replace strict PIT results and are not real-time performance evidence.
- `outputs/feature_ablation_proxy_asof_audit.csv`: per-origin cutoff and proxy-policy audit for the diagnostic track.
- `outputs/release_timestamp_audit.csv` and `outputs/raw_event_version_audit.csv`: release timestamp coverage and collected raw event revisions.

## Evaluation limits

Historical observations do not imply historical point-in-time knowledge. CPI release dates are available for much of the history, but CPI value vintages are not archived. Brent, USD/VND, Vietnam fuel, and NSO IIP also lack complete historical source-release/version archives at old forecast cutoffs. Current Gold snapshots can show today's source coverage, but cannot reconstruct those old cutoffs. Historical comparisons must therefore be described as pseudo-vintage diagnostics, not strict real-time performance evidence.

Both the primary nowcast and ablation benchmark read `gold_nowcasting_feature_vintages`
and select the newest persisted snapshot whose build cutoff **and** recorded time
are no later than each forecast cutoff. The daily dbt pipeline appends these
snapshots, so valid macro training origins can accumulate prospectively as the
pipeline observes new releases. This cannot backfill old forecast origins: the
current archive begins in October 2026, and its availability timestamps mostly
mean `source_ingested_at`, not the publisher's release timestamp. Reports must
keep these pipeline-observed vintages separate from verified source-release
vintages and must not relabel the old 140 origins as PIT. The first October
snapshot can become a historical training origin only after October CPI is
actually released and its release cutoff is later than the snapshot.

The latest source audit and the feature ablation report should accompany any claim about model quality. In particular, a multi-source model may be the chosen production architecture while its incremental historical value remains unproven until enough valid PIT forecast origins accumulate. Do not present the model selection itself as proof that adding all sources improved MAE or RMSE.

The benchmark evaluates a baseline-to-primary progression and records four
leave-one-source-group-out configurations: without Brent, USD/VND, Vietnam fuel
(RON95 and E10 together), and monthly IIP. Each is compared with Full
multi-source Ridge on paired folds. The sourcewise add-one-source experiment
is retained separately. Both types of source ablation are marked not estimable
when historical PIT source coverage is missing; the code does not fabricate
scores for unavailable vintages.

For source vintage details, see [`docs/POINT_IN_TIME_SOURCE_REVIEW.md`](../docs/POINT_IN_TIME_SOURCE_REVIEW.md). For model comparison protocol and current evidence, see [`docs/README_CAI_TIEN_2.md`](../docs/README_CAI_TIEN_2.md).

## Strict PIT and proxy sourcewise evidence

The feature-ablation task writes two separate sourcewise tracks. Strict PIT remains vintage-only in `feature_ablation_sourcewise.csv`; missing historical vintages remain not estimable and are not filled with current values. The pseudo-vintage proxy artifacts are separately named and labeled. Daily Brent/USD-VND/fuel observations use a source release timestamp when supplied, otherwise observation date + one UTC day. IIP uses a source release timestamp when supplied, otherwise observation month-end + 15 Vietnam-local days. These assumptions support sensitivity diagnostics only: they do not recover historical source revisions or prove real-time performance. The pipeline metrics report displays proxy results with this caveat and never merges them into strict PIT scores.

For real historical evidence, ingest verified source archives (such as ALFRED vintage dates for Brent, dated NSO/Petrolimex publications, and versioned Vietnam FX records) as immutable source vintages. If an archive is unavailable, continue append-only snapshots prospectively; never backdate today's downloaded values.
