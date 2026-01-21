"""
L03 Refuel pipeline (PySpark-native).
"""

from pyspark.sql import DataFrame
from fuel.utils.spark_utils import merge_dataframes, keep_with_cols
from fuel.pipeline.l03_primary.refuel_nodes  import (
    refuel_timestamps_transform,
    clean_refuel,
)


def run_refuel_pipeline(
    int_refuel: DataFrame,
    prm_populasi_dumptruck: DataFrame,
    params: dict,
) -> DataFrame:
    """
    Run refuel pipeline.
    """

    df_refuel_ts = refuel_timestamps_transform(
        int_refuel,
        params["refuel_timestamps_transform"]["use_cols"],
    )

    df_refuel_pop = merge_dataframes(
        [
            df_refuel_ts,
            prm_populasi_dumptruck,
        ],
        **params["refuel_timestamps_transform"]["merge_dataframes_node_prm_populasi_dumptruck"],
    )

    df_clean = clean_refuel(df_refuel_pop)

    df_final = keep_with_cols(
        df_clean,
        params["keep_with_cols"]["use_cols"],
    )

    return df_final
