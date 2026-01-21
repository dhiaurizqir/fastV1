"""
Partitioned typecasting pipeline for L02 (PySpark version).
"""

from typing import Dict

from pyspark.sql import DataFrame

from fuel.pipeline.l02_intermediate.partitioned_nodes import partitioned_processing


def run_partitioned_typecasting_pipeline(
    raw_partitioned_dfs: Dict[str, DataFrame],
    params: dict,
) -> Dict[str, DataFrame]:
    """
    Run typecasting pipeline for a partitioned dataset.`

    Args:
        raw_partitioned_dfs (Dict[str, DataFrame]):
            key   -> input partition filename
            value -> Spark DataFrame
        params (dict):
            Expected keys:
              - relevant_filenames
              - dtype
              - col_names
              - primary_keys
              - order_by
              - partition_key
              - usecols
              - dt_format

    Returns:
        Dict[str, DataFrame]:
            key   -> output partition name
            value -> processed Spark DataFrame
    """
    return partitioned_processing(
        datasets=raw_partitioned_dfs,
        relevant_filenames=params.get("relevant_filenames", {}),
        dtype=params.get("dtype"),
        col_names=params.get("col_names"),
        primary_keys=params.get("primary_keys"),
        order_by=params.get("order_by"),
        partition_key=params.get("partition_key"),
        usecols=params.get("usecols"),
        dt_format=params.get("dt_format"),
    )
