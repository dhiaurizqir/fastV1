"""
L04 Metrics Calculation pipeline (Spark-native)
"""

from pyspark.sql import DataFrame
from fuel.pipeline.l04_feature.metrics_calc_nodes import (
    get_metrics_rr,
    get_metrics_shift,
)

def run_metrics_calc_pipeline(
    feat_fleet_cycle: DataFrame,
    feat_payload_shift: DataFrame,
    params: dict,
) -> dict:
    """
    Calculate RR and shift metrics.
    Returns multiple outputs.
    """

    feat_rr_metrics = get_metrics_rr(
        feat_fleet_cycle,
        params["get_metrics_rr"]["groupby_keys"],
    )

    feat_shift_metrics_ref = get_metrics_shift(
        feat_fleet_cycle,
        params["get_metrics_shift"]["groupby_keys"],
    )

    feat_shift_metrics = get_metrics_shift(
        feat_payload_shift,
        params["get_metrics_shift"]["groupby_keys"],
    )

    return {
        "feat_rr_metrics": feat_rr_metrics,
        "feat_shift_metrics_ref": feat_shift_metrics_ref,
        "feat_shift_metrics": feat_shift_metrics,
    }
