"""
L04 Merge Refuel & Payloads pipeline (Spark-native)
"""

from pyspark.sql import DataFrame
from fuel.pipeline.l04_feature.merge_refuel_payloads_nodes import (
    merge_refuel_payload_spark,
)

def run_merge_refuel_payload_pipeline(
    prm_refuel: DataFrame,
    prm_vhms_payloads: DataFrame,
    params: dict,
) -> DataFrame:
    """
    Merge refuel rounds with payload cycles.
    """

    return merge_refuel_payload_spark(
        df_refuel=prm_refuel,
        df_payload=prm_vhms_payloads,
        join_keys=params["create_merged_refuel_payload_pipeline"]["join_keys"],
        how=params["create_merged_refuel_payload_pipeline"]["how"],
    )
