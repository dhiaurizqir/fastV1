from typing import List
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


def create_feature_fuel(
    df_fleet_cycle: DataFrame,
    main_cols: List[str],
    refuel_cols: List[str],
) -> DataFrame:

    window_refuel = Window.partitionBy(*(main_cols + refuel_cols))

    df = (
        df_fleet_cycle
        .withColumn(
            "cycle_time_per_refuel_hours",
            F.sum("total_cycle_time").over(window_refuel) / F.lit(60)
        )
        .withColumn(
            "cycle_time_hours",
            F.col("total_cycle_time") / F.lit(60)
        )
        .withColumn(
            "refuel_qty_per_refuel_prop",
            F.col("refuel_qty") / F.col("cycle_time_per_refuel_hours")
        )
        .withColumn(
            "fuel_consumption",
            F.col("refuel_qty_per_refuel_prop") * F.col("cycle_time_hours")
        )
    )

    return df
