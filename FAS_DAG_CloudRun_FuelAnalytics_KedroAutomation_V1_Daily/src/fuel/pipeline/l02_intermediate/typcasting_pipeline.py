"""
L02 Typecasting pipeline (Spark-native)
"""

from pyspark.sql import DataFrame
from fuel.pipeline.l02_intermediate.typecasting_nodes import standard_processing


def run_typecasting_pipeline(
    raw_df: DataFrame,
    params: dict,
) -> DataFrame:
    """
    Run L02 standard typecasting for a single dataset.
    """
    return standard_processing(
        data=raw_df,
        dtype=params.get("dtype"),
        col_names=params.get("col_names"),
        primary_keys=params.get("primary_keys"),
        order_by=params.get("order_by"),
        partition_key=params.get("partition_key"),
        usecols=params.get("usecols"),
        dt_format=params.get("dt_format"),
    )
