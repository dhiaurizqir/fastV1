"""
L04 Fleet Cycles pipeline (Spark-native)
"""

from pyspark.sql import DataFrame
from fuel.utils.spark_utils import merge_dataframes
from fuel.pipeline.l04_feature.fleet_cycles_nodes import create_feature_fuel

def run_fleet_cycles_pipeline(
    prm_vhms_payloads: DataFrame,
    prm_fleet: DataFrame,
    feat_merge_refuel_payload: DataFrame,
    params: dict,s
) -> DataFrame:
    """
    Create fleet cycle features.
    """

    feat_payload_shift = merge_dataframes(
        [prm_vhms_payloads, prm_fleet],
        **params["merge_dataframes_node_prm_fleet"]
    )

    refuel_cycle_shift = merge_dataframes(
        [feat_merge_refuel_payload, prm_fleet],
        **params["merge_dataframes_node_prm_fleet"]
    )

    return create_feature_fuel(
        df_fleet_cycle=refuel_cycle_shift,
        main_cols=params["create_feature_fuel"]["main_cols"],
        refuel_cols=params["create_feature_fuel"]["refuel_cols"],
    )
