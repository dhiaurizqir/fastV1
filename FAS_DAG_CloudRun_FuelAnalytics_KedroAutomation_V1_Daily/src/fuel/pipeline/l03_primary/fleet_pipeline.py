"""
L03 Fleet pipeline (PySpark-native).
"""

from pyspark.sql import DataFrame
from fuel.utils.spark_utils import merge_dataframes, keep_with_cols

from fuel.pipeline.l03_primary.fleet_nodes import (
    clean_fleet,
    get_shift_times,
)
 

def run_fleet_pipeline(
    int_fleet: DataFrame,
    raw_customer_shift: DataFrame,
    prm_populasi_dumptruck: DataFrame,
    prm_populasi_excavator: DataFrame,
    params: dict,
) -> DataFrame:

    df_clean = clean_fleet(int_fleet)

    df_shift = get_shift_times(
        df_clean,
        raw_customer_shift,
    )

    df_dumptruck = merge_dataframes(
        [df_shift, prm_populasi_dumptruck],
        **params["merge_dataframes_node_prm_populasi_dumptruck"],
    )

    df_excavator = merge_dataframes(
        [df_dumptruck, prm_populasi_excavator],
        **params["merge_dataframes_node_prm_populasi_excavator"],
    )

    return keep_with_cols(
        df_excavator,
        params["keep_with_cols"]["use_cols"],
    )
