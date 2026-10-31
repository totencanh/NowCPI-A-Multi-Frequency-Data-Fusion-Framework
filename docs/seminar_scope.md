# Seminar scope and research framing

## Recommended title

**NowCPI: A Multi-Frequency Data Fusion Framework for Real-Time Inflation
Nowcasting in Vietnam via Streaming Lakehouse Architecture under Asynchronous
Multi-Source Data**

Vietnamese working title:

**NowCPI: Khung hợp nhất dữ liệu đa tần suất cho dự báo nhanh lạm phát tại Việt
Nam qua kiến trúc Streaming Lakehouse với nhiều nguồn dữ liệu đến bất đồng bộ**

This title keeps the lakehouse and asynchronous-data contribution while naming
the actual forecasting target and predictor block. “Real-time” is avoided as a
claim of continuous predictions: ingestion to Bronze streams continuously, but
source collection and the nowcast are scheduled daily.

## Research objective

Assess whether high-frequency Brent observations, represented as weekly log
returns and the latest CPI lag available at run time can support an estimate of
Vietnam's monthly headline CPI month-over-month inflation for the month after
the latest actual CPI observation. The workflow runs daily, but the target may
be a delayed historical month when publication data arrive late.

## Current model and data roles

| Data | Frequency | Role in the current work |
|---|---|---|
| Vietnam headline CPI | Monthly | Target label and training outcome; latest released monthly CPI MoM is a predictor in the primary model and AR-MIDAS comparison |
| Brent price | Daily source, converted to weekly log returns | High-frequency predictors in the multi-source model; also used by AR-MIDAS comparison |
| USD/VND and domestic fuel prices | Daily | Multi-source model features when eligible at the point-in-time cutoff; also Gold/Power BI analysis |
| CPI components | Monthly | Gold/Power BI analysis; not currently a primary-model feature |
| Vietnam NSO industrial production index | Monthly | Multi-source model feature when eligible; YoY growth is derived from the index |
| News | Not collected | NLP/news features are outside the implemented seminar scope |

The primary model is **Full multi-source Ridge**. AR-MIDAS, CPI-only, naive,
AR(1), U-MIDAS, and alternative estimators are comparison models or fallbacks.
The primary-model policy does not mean every source has enough historical
point-in-time coverage to demonstrate incremental predictive value; report
that limitation alongside model results.

## Asynchronous availability and forecast timing

The nowcast freezes a UTC information cutoff at the beginning of a run. Brent
observations are included only when their `available_at` is no later than that
cutoff. CPI labels and the optional lagged CPI input are checked independently
against `label_available_at`. A source that arrives late is not imputed or
silently synchronized; it becomes eligible in a later run. The output records
the cutoff and the model processing start, end, and duration separately.

`available_at` is the pipeline's best recorded availability time, preferring a
source release timestamp and falling back to transport/ingestion timestamps.
It is not guaranteed to be the original public-release time for every source.

## Evaluation and claims

- Compare AR-MIDAS Ridge with the CPI-only Ridge baseline on the same forecast
  months and expanding-window splits. Report MAE and RMSE in percentage points;
  directional accuracy may be supplementary.
- Historical source-release vintages are not archived. Historical metrics are
  pseudo-out-of-sample diagnostics using available history, not proof of
  vintage-accurate real-time performance.
- The rolling-origin MIDAS comparison reports naïve last-CPI, CPI-only Ridge,
  and AR-MIDAS on identical folds, for both all common months and the latest
  nine common months. Historical availability uses explicit assumptions
  (previous CPI by day 15 of the target month; Brent available the following
  UTC day); those assumptions are not archived official release vintages.
- Treat the forecast as an estimate for the target month, not an official CPI
  release. State the target month and data cutoff beside every reported value.
- Do not call CPI component MoM/YoY values “contributions” to headline CPI
  unless CPI expenditure weights and a contribution calculation are added.

## Seminar story

1. Explain asynchronous ingestion and the Raw/Bronze/Silver/Gold lakehouse.
2. Show that observations keep frequency and availability context.
3. Describe the target and AR-MIDAS Ridge design, including the latest
   available monthly CPI lag and weekly Brent log-return lags.
4. Compare the model with the CPI-only baseline and state the vintage-data
   limitation.
5. Use the remaining Gold data in Power BI as complementary economic
   monitoring, clearly separate from predictors used by the current model.
