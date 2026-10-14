"""NowCPI controlled historical simulation (read-only Trino). Not verified PIT.

Usage: python .\forecasting\evaluate_simulation.py
Dependencies: pandas, numpy, scikit-learn, trino
"""
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from trino.dbapi import connect

OUT = Path(__file__).resolve().parent / 'outputs'
OUT.mkdir(parents=True, exist_ok=True)
MIN_TRAIN = 8
SOURCES = ['brent', 'fx', 'fuel']

# Explicit mapping candidates; report detected schema instead of guessing silently.
TABLES = {
    'brent': ('gold_brent_daily', ['brent_usd_per_barrel', 'brent_price_usd', 'price_usd_per_barrel', 'value']),
    'fx': ('gold_usd_vnd_daily', ['vnd_per_usd', 'usd_vnd_rate', 'exchange_rate', 'value']),
    'fuel': ('gold_fuel_daily', ['ron95_vnd_per_liter', 'fuel_ron95_vnd_per_liter', 'price_vnd_per_liter', 'vnd_per_liter', 'value']),
}


def fetch(conn, table):
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM delta.gold.{table}')
    cols = [c[0].lower() for c in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)


def choose_column(df, candidates, table):
    for col in candidates:
        if col in df.columns:
            return col
    raise ValueError(f'Cannot determine numeric value column in {table}. Columns: {list(df.columns)}. Update TABLES.')


def prepare_source(df, source, table, candidates):
    if 'observation_date' not in df.columns:
        raise ValueError(f'{table}: missing observation_date. Columns: {list(df.columns)}')
    value_col = choose_column(df, candidates, table)
    if source == 'fuel' and 'series_id' in df.columns:
        series = df['series_id'].fillna('').astype(str).str.lower()
        is_ron95 = series.eq('vn_fuel_ron95') | (series.str.contains('ron95') & ~series.str.contains('e10'))
        df = df[is_ron95].copy()
    if 'country' in df.columns:
        df = df[df['country'].astype(str).str.upper() == 'VNM'].copy()
    df['date'] = pd.to_datetime(df['observation_date'], errors='coerce').dt.normalize()
    df['price'] = pd.to_numeric(df[value_col], errors='coerce')
    df = df.dropna(subset=['date', 'price'])[['date', 'price']]
    # Duplicate observations on a date: mean, explicit simplifying choice.
    df = df.groupby('date', as_index=False)['price'].mean().sort_values('date')
    print(f'{source}: {len(df)} observation days, value column={value_col}')
    return df


def cutoff_for(month):
    # Simulated monthly nowcast at 00:00 UTC on the 15th.
    return pd.Timestamp(year=month.year, month=month.month, day=15, tz='UTC')


def availability_for_market(obs):
    # Controlled assumption: available 00:00 UTC on calendar day AFTER observation.
    return obs + pd.Timedelta(days=1)


def historical_market_avg(source_df, month, cutoff):
    # Only data from target month, and ONLY dates with assumed availability <= cutoff.
    start = pd.Timestamp(year=month.year, month=month.month, day=1)
    cutoff_naive = cutoff.tz_localize(None)
    filtered = source_df.loc[
        (source_df['date'] >= start)
        & (source_df['date'] + pd.Timedelta(days=1) <= cutoff_naive)
    ]
    if filtered.empty:
        return np.nan, 0, pd.NaT
    last_assumed_available = filtered['date'].max() + pd.Timedelta(days=1)
    return float(filtered['price'].mean()), len(filtered), last_assumed_available


def create_features(labels, markets):
    rows = []
    label_by_month = dict(zip(labels['month'], labels['actual']))
    for _, r in labels.iterrows():
        month = r['month']
        cutoff = cutoff_for(month)
        prev_month = month - pd.DateOffset(months=1)
        # CPI for prior month assumed released at 00:00 UTC on day 10 of current month.
        prior_cpi_available = pd.Timestamp(year=month.year, month=month.month, day=10, tz='UTC')
        lag = label_by_month.get(prev_month, np.nan) if prior_cpi_available <= cutoff else np.nan
        item = {'target_month': month.strftime('%Y-%m'), 'month': month,
                'cutoff_utc': cutoff.isoformat(), 'actual': r['actual'],
                'lag_1': lag, 'cpi_lag_assumed_available_utc': prior_cpi_available.isoformat()}
        for source in SOURCES:
            value, n, max_available = historical_market_avg(markets[source], month, cutoff)
            item[source] = value
            item[source + '_observation_count'] = n
            item[source + '_latest_assumed_available_utc'] = (
                max_available.tz_localize('UTC').isoformat() if pd.notna(max_available) else None
            )
            if pd.notna(max_available) and max_available.tz_localize('UTC') > cutoff:
                raise AssertionError(f'Temporal alignment violation: {source} {month}')
        rows.append(item)
    return pd.DataFrame(rows)


def model_predict(train, test, features):
    X_train = train[features].copy()
    X_test = test[features].copy()
    # Drop predictors entirely missing IN TRAINING; no lookahead for selection.
    selected = [c for c in features if X_train[c].notna().any()]
    if not selected:
        return np.nan
    # Fill missing values using TRAINING data only; scale using TRAINING data only.
    pipe = make_pipeline(SimpleImputer(strategy='median'), StandardScaler(), Ridge(alpha=10.0))
    pipe.fit(X_train[selected], train['actual'])
    return float(pipe.predict(X_test[selected])[0])


def measures(frame, name):
    good = frame.dropna(subset=['actual', name])
    y = good['actual'].to_numpy(dtype=float)
    p = good[name].to_numpy(dtype=float)
    e = np.abs(y - p)
    valid = np.abs(y) > 0.1  # MAPE unstable for CPI changes near zero; report exclusions.
    return {
        'model': name, 'evaluated_months': len(good),
        'mae_pp': float(e.mean()) if len(good) else np.nan,
        'rmse_pp': float(np.sqrt(np.mean((y - p)**2))) if len(good) else np.nan,
        'mape_pct_excluding_abs_actual_le_0_1pp': float(np.mean(e[valid] / np.abs(y[valid])) * 100) if valid.any() else np.nan,
        'mape_included_months': int(valid.sum()),
        'directional_accuracy_pct': float(np.mean(np.sign(y) == np.sign(p)) * 100) if len(good) else np.nan,
    }


def main():
    conn = connect(host='localhost', port=8080, user='nowcpi', catalog='delta', schema='gold')
    try:
        labels = fetch(conn, 'gold_nowcasting_labels')
        markets = {}
        for source, (table, candidates) in TABLES.items():
            raw = fetch(conn, table)
            markets[source] = prepare_source(raw, source, table, candidates)
    finally:
        conn.close()
    labels = labels.loc[labels['country'].astype(str).str.upper() == 'VNM',
                         ['target_month', 'target_cpi_mom_percent']].copy()
    labels.columns = ['month', 'actual']
    labels['month'] = pd.to_datetime(labels['month'], errors='coerce').dt.to_period('M').dt.to_timestamp()
    labels['actual'] = pd.to_numeric(labels['actual'], errors='coerce')
    labels = labels.dropna().drop_duplicates('month').sort_values('month').reset_index(drop=True)
    features = create_features(labels, markets)
    features.drop(columns=['month']).to_csv(OUT / 'simulated_features.csv', index=False)

    predictions = []
    for i in range(len(features)):
        row = features.iloc[i]
        if pd.isna(row['lag_1']):
            continue
        train = features.iloc[:i].dropna(subset=['lag_1', 'actual'])
        if len(train) < MIN_TRAIN:
            continue
        test = features.iloc[[i]]
        out = {'target_month': row['target_month'], 'actual': float(row['actual']),
               'train_months': len(train), 'naive': float(row['lag_1'])}
        out['ridge_cpi_only'] = model_predict(train, test, ['lag_1'])
        out['ridge_full'] = model_predict(train, test, ['lag_1'] + SOURCES)
        for lost in SOURCES:
            out['loss_' + lost] = model_predict(train, test, ['lag_1'] + [s for s in SOURCES if s != lost])
        predictions.append(out)
    pred = pd.DataFrame(predictions)
    if pred.empty:
        raise RuntimeError('Not enough consecutive CPI MoM labels. Cannot backtest.')
    model_names = ['naive', 'ridge_cpi_only', 'ridge_full'] + ['loss_' + s for s in SOURCES]
    metric = pd.DataFrame([measures(pred, name) for name in model_names])
    full_mae = float(metric.loc[metric['model'] == 'ridge_full', 'mae_pp'].iloc[0])
    naive_mae = float(metric.loc[metric['model'] == 'naive', 'mae_pp'].iloc[0])
    metric['mae_improvement_vs_naive_pct'] = ((naive_mae - metric['mae_pp']) / naive_mae * 100) if naive_mae else np.nan
    metric['mae_degradation_vs_full_pct'] = ((metric['mae_pp'] - full_mae) / full_mae * 100) if full_mae else np.nan
    pred.to_csv(OUT / 'simulated_predictions.csv', index=False)
    metric.round(4).to_csv(OUT / 'simulated_metrics.csv', index=False)
    print('\nCONTROLLED SIMULATION — NOT VERIFIED HISTORICAL RELEASE TIMES')
    print('Assumptions: market next-day, previous CPI released day 10, cutoff day 15 UTC.')
    print('Ridge alpha=10; train-only median imputation + standardization; expanding window.')
    print('\nObserved feature coverage in labeled months:')
    print(features[['lag_1'] + SOURCES].notna().sum().to_string())
    print('\nResults (common rolling-origin folds):')
    print(metric.round(4).to_string(index=False))
    print('\nOutputs:', OUT)
    print('simulated_features.csv, simulated_predictions.csv, simulated_metrics.csv')
    print('\nWarning: Source-loss rows are RETRAINED ablations, not a live outage replay.')
    print('Do not interpret historical assumptions as actual source-release timestamps.')

if __name__ == '__main__':
    main()
