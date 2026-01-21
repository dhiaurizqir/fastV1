"""
L03 Population pipeline (PySpark-native).
"""

from pyspark.sql import DataFrame
from fuel.utils.spark_utils import keep_with_cols, rename_columns


def run_population_pipeline(
    int_populasi: DataFrame,
    params: dict,
) -> dict[str, DataFrame]:
    """
    Create population datasets for dumptruck and excavator.

    Returns:
        {
            "prm_populasi_dumptruck": DataFrame,
            "prm_populasi_excavator": DataFrame
        }
    """

    # Dumptruck
    dumptruck_df = rename_columns(
        int_populasi,
        params["dumptruck"]["rename_columns"],
    )
    dumptruck_df = keep_with_cols(
        dumptruck_df,
        params["dumptruck"]["keep_with_cols"]["use_cols"],
    )

    # Excavator
    excavator_df = rename_columns(
        int_populasi,
        params["excavator"]["rename_columns"],
    )
    excavator_df = keep_with_cols(
        excavator_df,
        params["excavator"]["keep_with_cols"]["use_cols"],
    )

    return {
        "prm_populasi_dumptruck": dumptruck_df,
        "prm_populasi_excavator": excavator_df,
    }
