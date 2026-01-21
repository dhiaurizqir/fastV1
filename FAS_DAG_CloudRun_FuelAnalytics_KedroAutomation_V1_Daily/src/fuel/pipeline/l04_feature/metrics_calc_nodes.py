from typing import List
from pyspark.sql import DataFrame, functions as F


def get_metrics_rr(
    df_fleet_cycle: DataFrame,
    groupby_keys: List[str],
) -> DataFrame:

    return (
        df_fleet_cycle
        .groupBy(*groupby_keys)
        .agg(
            F.sum("fuel_consumption").alias("refuel_qty_sum"),
            F.sum("drive_distance").alias("distance_sum"),
            F.count("cycle_id").alias("num_cycles"),
            F.sum("payload").alias("payload_sum"),
        )
    )


def get_metrics_shift(
    df_fleet_cycle: DataFrame,
    groupby_keys: List[str],
) -> DataFrame:

    return (
        df_fleet_cycle
        .groupBy(*groupby_keys)
        .agg(
            F.count("cycle_id").alias("num_cycles"),
            F.sum("payload").alias("payload_sum"),
        )
    )
