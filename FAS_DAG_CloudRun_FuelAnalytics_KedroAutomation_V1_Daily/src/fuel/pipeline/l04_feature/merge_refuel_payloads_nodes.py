from typing import List
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


def merge_refuel_payload_spark(
    df_refuel: DataFrame,
    df_payload: DataFrame,
    join_keys: List[str],
    how: str,
) -> DataFrame:

    df_merged = (
        df_refuel
        .join(df_payload, on=join_keys, how=how)
        .filter(
            (F.col("refuel_round_start_time") <= F.col("cycle_start_time")) &
            (F.col("refuel_round_end_time") >= F.col("cycle_end_time"))
        )
    )

    df_merged = (
        df_merged
        .withColumn(
            "refuel_id",
            F.concat_ws(
                "__",
                F.date_format("refuel_round_start_time", "yyyy-MM-dd HH:mm:ss"),
                F.date_format("refuel_round_end_time", "yyyy-MM-dd HH:mm:ss"),
            )
        )
        .withColumn(
            "total_cycle_time",
            F.col("empty_drive_time")
            + F.col("empty_stop_time")
            + F.col("loaded_drive_time")
            + F.col("loaded_stop_time")
            + F.col("loading_stop_time")
        )
        .withColumn(
            "idling_time",
            F.col("loaded_stop_time") + F.col("empty_stop_time")
        )
        .withColumn(
            "empty_speed",
            F.col("empty_drive_distance") / (F.col("empty_drive_time") / F.lit(60))
        )
        .withColumn(
            "loaded_speed",
            F.col("loaded_drive_distance") / (F.col("loaded_drive_time") / F.lit(60))
        )
        .withColumn(
            "drive_distance",
            (F.col("empty_drive_distance") + F.col("loaded_drive_distance")) / F.lit(2)
        )
        .withColumn("refuel_qty", F.abs(F.col("refuel_qty")))
    )

    return df_merged
