"""
L03 VHMS Payloads pipeline (PySpark-native).
"""

from pyspark.sql import DataFrame
from fuel.utils.io import write_dataset
from config.config import get_path, load_paths
from fuel.utils.spark_utils import merge_dataframes, keep_with_cols
from fuel.pipeline.l03_primary.vhms_payloads_nodes import (
    pivot_payload,
    create_timestamps_payloads,
)

paths = load_paths()

def run_vhms_payloads_pipeline(
    int_vhms_payloads: DataFrame,
    prm_populasi_dumptruck: DataFrame,
    raw_customer_shift: DataFrame,
    params: dict,
) -> DataFrame:

    df_pivot = pivot_payload(
        int_vhms_payloads,
        pivot_params=params["pivot_payload"]["pivot_params"],
        rename_dict=params["pivot_payload"]["rename_dict"],
        fillna_dict=params["pivot_payload"]["fillna_dict"],
    )
    
    #Write Pivoted Payloads for Tracing Purpose
    write_dataset(
        df_pivot,
        get_path(paths, "l03", "fas_daily", "pivoted_payloads")
    )

    df_pop = merge_dataframes(
        [df_pivot, prm_populasi_dumptruck],
        **params["merge_dataframes_node_prm_populasi_dumptruck"],
    )

    df_shift = create_timestamps_payloads(
        df_pop,
        raw_customer_shift,
    )

    return keep_with_cols(
        df_shift,
        params["keep_with_cols"]["use_cols"],
    )