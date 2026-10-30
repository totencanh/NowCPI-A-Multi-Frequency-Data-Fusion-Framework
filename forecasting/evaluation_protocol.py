"""Paired forecast evaluation helpers for the NowCPI rolling-origin study."""

from __future__ import annotations

import json
import math

import numpy as np
import pandas as pd


MIN_DM_ORIGINS = 24
BOOTSTRAP_REPS = 4000
BOOTSTRAP_BLOCK = 3
BOOTSTRAP_SEED = 20261030


def release_timestamp_audit(labels: pd.DataFrame, brent: pd.DataFrame) -> pd.DataFrame:
    """Summarize true release-time coverage without treating ingestion as release."""
    rows = []
    for source, frame, release_column, available_column, basis_column in (
        ("cpi", labels, "label_source_release_ts", "label_available_at", "label_available_at_basis"),
        ("brent", brent, "source_release_ts", "available_at", "available_at_basis"),
    ):
        total = len(frame)
        release = (
            pd.to_datetime(frame[release_column], errors="coerce", utc=True)
            if release_column in frame
            else pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")
        )
        available = (
            pd.to_datetime(frame[available_column], errors="coerce", utc=True)
            if available_column in frame
            else pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")
        )
        basis = (
            frame[basis_column].fillna("unknown").astype(str).value_counts().to_dict()
            if basis_column in frame
            else {}
        )
        rows.append(
            {
                "source": source,
                "records": total,
                "records_with_source_release_timestamp": int(release.notna().sum()),
                "release_timestamp_coverage_pct": (
                    round(float(release.notna().mean() * 100), 2) if total else 0.0
                ),
                "min_source_release_ts": release.min(),
                "max_source_release_ts": release.max(),
                "records_with_pipeline_available_at": int(available.notna().sum()),
                "min_pipeline_available_at": available.min(),
                "max_pipeline_available_at": available.max(),
                "pipeline_available_at_basis_counts": json.dumps(basis, sort_keys=True),
                "ingestion_is_not_used_as_release": True,
                "release_timestamp_coverage_meets_minimum": False,
                "historical_vintages_archived": False,
                "strict_real_time_backtest_ready": False,
            }
        )
    # The strict readiness flag needs release timestamps from BOTH sources.
    cpi_coverage = rows[0]["records_with_source_release_timestamp"]
    brent_coverage = rows[1]["records_with_source_release_timestamp"]
    timestamp_coverage_meets_minimum = (
        cpi_coverage >= MIN_DM_ORIGINS and brent_coverage >= MIN_DM_ORIGINS
    )
    for row in rows:
        row["release_timestamp_coverage_meets_minimum"] = timestamp_coverage_meets_minimum
        # Timestamps alone cannot establish that historical revisions/vintages
        # were preserved, so strict readiness remains false until that is proven.
        row["strict_real_time_backtest_ready"] = False
        row["readiness_note"] = (
            "Requires source release timestamps for both sources and archived point-in-time vintages/revisions; ingestion timestamps do not qualify."
        )
    return pd.DataFrame(rows)


def _hac_dm_pvalue(loss_a: np.ndarray, loss_b: np.ndarray, horizon: int = 1) -> float | None:
    """Two-sided Harvey-Leybourne-Newbold corrected DM p-value, Bartlett HAC."""
    differential = np.asarray(loss_a - loss_b, dtype=float)
    differential = differential[np.isfinite(differential)]
    n = len(differential)
    if n < MIN_DM_ORIGINS:
        return None
    centered = differential - differential.mean()
    lag = min(max(horizon - 1, 0), n - 1)
    long_run_variance = float(np.dot(centered, centered) / n)
    for k in range(1, lag + 1):
        covariance = float(np.dot(centered[k:], centered[:-k]) / n)
        long_run_variance += 2 * (1 - k / (lag + 1)) * covariance
    if long_run_variance <= 0:
        return 1.0
    dm = float(differential.mean() / math.sqrt(long_run_variance / n))
    hln_sq = (n + 1 - 2 * horizon + horizon * (horizon - 1) / n) / n
    corrected = dm * math.sqrt(max(hln_sq, 0.0))
    return float(math.erfc(abs(corrected) / math.sqrt(2)))


def _moving_block_delta_ci(
    actual: np.ndarray,
    proposed: np.ndarray,
    baseline: np.ndarray,
    metric: str,
) -> tuple[float, float] | tuple[None, None]:
    n = len(actual)
    if n < 2:
        return None, None
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    block = min(BOOTSTRAP_BLOCK, n)
    starts = np.arange(n)
    samples = []
    blocks_per_sample = math.ceil(n / block)
    chosen = rng.choice(starts, size=(BOOTSTRAP_REPS, blocks_per_sample), replace=True)
    offsets = np.arange(block)
    indices = ((chosen[:, :, None] + offsets[None, None, :]) % n)
    indices = indices.reshape(BOOTSTRAP_REPS, -1)[:, :n]
    err_p = actual[indices] - proposed[indices]
    err_b = actual[indices] - baseline[indices]
    if metric == "mae":
        samples = np.mean(np.abs(err_p), axis=1) - np.mean(np.abs(err_b), axis=1)
    else:
        samples = np.sqrt(np.mean(err_p**2, axis=1)) - np.sqrt(np.mean(err_b**2, axis=1))
    low, high = np.quantile(samples, [0.025, 0.975])
    return float(low), float(high)


def paired_model_comparison(frame: pd.DataFrame, proposed: str, baseline: str) -> dict:
    """Report paired deltas, bootstrap intervals, and DM tests on shared origins."""
    actual = frame["actual_cpi_mom_percent"].to_numpy(dtype=float)
    pred_p = frame[proposed].to_numpy(dtype=float)
    pred_b = frame[baseline].to_numpy(dtype=float)
    n = len(frame)
    mae_p = float(np.mean(np.abs(actual - pred_p)))
    mae_b = float(np.mean(np.abs(actual - pred_b)))
    rmse_p = float(np.sqrt(np.mean((actual - pred_p) ** 2)))
    rmse_b = float(np.sqrt(np.mean((actual - pred_b) ** 2)))
    direction_p = float(np.mean(np.sign(actual) == np.sign(pred_p)) * 100)
    direction_b = float(np.mean(np.sign(actual) == np.sign(pred_b)) * 100)
    mae_ci = _moving_block_delta_ci(actual, pred_p, pred_b, "mae")
    rmse_ci = _moving_block_delta_ci(actual, pred_p, pred_b, "rmse")
    return {
        "proposed_model": proposed,
        "baseline_model": baseline,
        "paired_origins": n,
        "proposed_directional_accuracy_pct": direction_p,
        "baseline_directional_accuracy_pct": direction_b,
        "directional_accuracy_delta_pp": direction_p - direction_b,
        "proposed_mae_pp": mae_p,
        "baseline_mae_pp": mae_b,
        "mae_delta_pp_proposed_minus_baseline": mae_p - mae_b,
        "mae_relative_uplift_pct_positive_is_better": (
            100 * (mae_b - mae_p) / mae_b if mae_b else None
        ),
        "mae_delta_95ci_low_pp": mae_ci[0],
        "mae_delta_95ci_high_pp": mae_ci[1],
        "proposed_rmse_pp": rmse_p,
        "baseline_rmse_pp": rmse_b,
        "rmse_delta_pp_proposed_minus_baseline": rmse_p - rmse_b,
        "rmse_relative_uplift_pct_positive_is_better": (
            100 * (rmse_b - rmse_p) / rmse_b if rmse_b else None
        ),
        "rmse_delta_95ci_low_pp": rmse_ci[0],
        "rmse_delta_95ci_high_pp": rmse_ci[1],
        "dm_pvalue_mae_loss": _hac_dm_pvalue(
            np.abs(actual - pred_p), np.abs(actual - pred_b)
        ),
        "dm_pvalue_squared_loss": _hac_dm_pvalue(
            (actual - pred_p) ** 2, (actual - pred_b) ** 2
        ),
        "dm_status": (
            "reported"
            if n >= MIN_DM_ORIGINS
            else f"not reported: need at least {MIN_DM_ORIGINS} paired origins"
        ),
        "bootstrap_method": f"paired circular moving-block bootstrap; block={BOOTSTRAP_BLOCK}; reps={BOOTSTRAP_REPS}; seed={BOOTSTRAP_SEED}",
    }


def failure_cases(frame: pd.DataFrame, proposed: str, baseline: str) -> pd.DataFrame:
    """Return up to three worst paired failures with transparent labels."""
    out = frame[["target_month", "actual_cpi_mom_percent", proposed, baseline]].copy()
    out["proposed_abs_error_pp"] = (
        out["actual_cpi_mom_percent"] - out[proposed]
    ).abs()
    out["baseline_abs_error_pp"] = (
        out["actual_cpi_mom_percent"] - out[baseline]
    ).abs()
    out["error_gap_pp_proposed_minus_baseline"] = (
        out["proposed_abs_error_pp"] - out["baseline_abs_error_pp"]
    )
    out["failure_pattern"] = np.select(
        [
            np.sign(out[proposed]) != np.sign(out["actual_cpi_mom_percent"]),
            out["proposed_abs_error_pp"] > out["baseline_abs_error_pp"],
        ],
        ["wrong_direction", "worse_than_naive"],
        default="large_absolute_error",
    )
    return out.sort_values(
        ["error_gap_pp_proposed_minus_baseline", "proposed_abs_error_pp"],
        ascending=False,
    ).head(3)
