
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression, Ridge
from trino.dbapi import connect

# Configuration
OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET = "target_cpi_mom_percent"
MIN_TRAIN_PAIRS = 8

# Read-only Trino connection
conn = connect(
    host="localhost",
    port=8080,
    user="nowcpi",
    catalog="delta",
    schema="gold"
)

try:
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT target_month, country, {TARGET}
        FROM gold_nowcasting_labels
        WHERE {TARGET} IS NOT NULL
        ORDER BY country, target_month
    """)
    rows = cursor.fetchall()
finally:
    conn.close()

df = pd.DataFrame(
    rows,
    columns=["month", "country", "actual"]
)

df["month"] = pd.to_datetime(df["month"])
df["actual"] = pd.to_numeric(df["actual"])
df = df.sort_values(["country", "month"])

predictions = []

# Each country is evaluated independently
for country, group in df.groupby("country"):
    group = group.reset_index(drop=True)

    # Confirm consecutive calendar months
    group["previous_month"] = group["month"].shift(1)
    group["lag_1"] = group["actual"].shift(1)

    consecutive = (
        group["previous_month"].dt.to_period("M")
        == group["month"].dt.to_period("M") - 1
    )
    group.loc[~consecutive, "lag_1"] = np.nan

    for i in range(len(group)):
        current = group.iloc[i]

        if pd.isna(current["lag_1"]):
            continue

        # Strictly earlier months only
        train = group.iloc[:i].dropna(
            subset=["lag_1", "actual"]
        )

        if len(train) < MIN_TRAIN_PAIRS:
            continue

        X_train = train[["lag_1"]].to_numpy()
        y_train = train["actual"].to_numpy()

        X_test = np.array([[current["lag_1"]]])

        linear = LinearRegression()
        linear.fit(X_train, y_train)

        ridge = Ridge(alpha=1.0)
        ridge.fit(X_train, y_train)

        predictions.append({
            "country": country,
            "target_month": current["month"].strftime("%Y-%m"),
            "actual": float(current["actual"]),
            "previous_actual": float(current["lag_1"]),
            "naive": float(current["lag_1"]),
            "ar1_linear": float(linear.predict(X_test)[0]),
            "ar1_ridge": float(ridge.predict(X_test)[0]),
            "training_pairs": len(train)
        })

result = pd.DataFrame(predictions)

if result.empty:
    raise RuntimeError(
        "Not enough consecutive labeled months "
        "for the chosen training window."
    )

result.to_csv(
    OUTPUT_DIR / "predictions.csv",
    index=False
)

# Evaluate all models on identical months
def evaluate(frame, model_name):
    actual = frame["actual"].to_numpy()
    predicted = frame[model_name].to_numpy()

    error = actual - predicted

    mae = np.mean(np.abs(error))
    rmse = np.sqrt(np.mean(error ** 2))

    nonzero = actual != 0
    mape = (
        np.mean(
            np.abs(error[nonzero] / actual[nonzero])
        ) * 100
        if np.any(nonzero)
        else np.nan
    )

    directional_accuracy = (
        np.mean(np.sign(actual) == np.sign(predicted)) * 100
    )

    return {
        "model": model_name,
        "evaluated_months": len(frame),
        "MAE_pp": mae,
        "RMSE_pp": rmse,
        "MAPE_pct": mape,
        "Directional_Accuracy_pct": directional_accuracy
    }

metrics = pd.DataFrame([
    evaluate(result, name)
    for name in ["naive", "ar1_linear", "ar1_ridge"]
])

naive_mae = metrics.loc[
    metrics["model"] == "naive", "MAE_pp"
].iloc[0]

metrics["MAE_Improvement_pct"] = (
    (naive_mae - metrics["MAE_pp"])
    / naive_mae * 100
    if naive_mae > 0
    else np.nan
)

metrics = metrics.round(4)

metrics.to_csv(
    OUTPUT_DIR / "metrics.csv",
    index=False
)

print("\nNOWCPI BASELINE EVALUATION")
print("=" * 65)
print(metrics.to_string(index=False))
print("\nPredictions:")
print(result.to_string(index=False))
print("\nSaved results in:", OUTPUT_DIR)

