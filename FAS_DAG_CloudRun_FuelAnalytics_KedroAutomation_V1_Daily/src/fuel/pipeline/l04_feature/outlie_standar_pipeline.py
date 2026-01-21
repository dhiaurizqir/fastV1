"""
L04 Outlier removal & standardization pipeline (Spark-native)
"""

from pyspark.sql import DataFrame
from fuel.pipeline.l04_feature.outlier_standar_nodes import (
    standarization_columns,
)

def run_outlier_standar_pipeline(
    feat_rr_metrics: DataFrame,
    feat_shift_metrics: DataFrame,
    params: dict,
) -> dict:
    """
    Standardize output tables.
    """

    rpt_fuel_ratio_intv = standarization_columns(
        feat_rr_metrics,
        params["fuel_ratio"]["col_names"],
        params["fuel_ratio"]["uppercase_cols"],
        params["fuel_ratio"]["keep_with_cols"],
    )

    rpt_payload_intv = standarization_columns(
        feat_shift_metrics,
        params["payload"]["col_names"],
        params["payload"]["uppercase_cols"],
        params["payload"]["keep_with_cols"],
    )

    rpt_refuel_qty_intv = standarization_columns(
        feat_rr_metrics,
        params["refuel_qty"]["col_names"],
        params["refuel_qty"]["uppercase_cols"],
        params["refuel_qty"]["keep_with_cols"],
    )

    return {
        "rpt_fuel_ratio_intv": rpt_fuel_ratio_intv,
        "rpt_payload_intv": rpt_payload_intv,
        "rpt_refuel_qty_intv": rpt_refuel_qty_intv,
    }
