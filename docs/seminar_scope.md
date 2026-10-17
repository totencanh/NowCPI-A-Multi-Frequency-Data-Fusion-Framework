# Seminar scope and research framing

## Recommended title

**NowCPI: A Streaming Lakehouse for Asynchronous Multi-Frequency CPI Nowcasting
in Vietnam: An AR-MIDAS Application with Weekly Brent and Monthly CPI**

Vietnamese working title:

**NowCPI: Lakehouse streaming cho nowcasting CPI tại Việt Nam với dữ liệu đa
tần suất đến bất đồng bộ — ứng dụng AR-MIDAS với Brent tuần và CPI tháng**

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
| Vietnam headline CPI | Monthly | Target label and training outcome; latest released monthly CPI MoM is the autoregressive lag in AR-MIDAS |
| Brent front-month price | Daily source, converted to weekly log returns | The 12 high-frequency MIDAS predictors in the current nowcast |
| USD/VND and domestic fuel prices | Daily | Gold/Power BI analysis; not current MIDAS predictors |
| CPI components | Monthly | Gold/Power BI analysis; not current MIDAS predictors |
| World Bank industrial value-added growth | Annual | Annual industrial activity proxy in Gold; not monthly IIP and not current MIDAS input |
| News | Not collected | NLP/news features are outside the implemented seminar scope |

The current nowcast therefore uses **CPI MoM as the forecast target, the latest
available CPI MoM lag, and weekly Brent returns as predictors**. The CPI-only
model is the fallback; the Brent-only model is diagnostic. Do not describe the
forecast as using every collected source. The other structured sources
demonstrate multi-source lakehouse ingestion and support dashboard analysis.

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
