# fuel/pipelines/l03_primary/refuel_nodes.py

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from fuel.utils.spark_utils import keep_with_cols


def refuel_timestamps_transform(
    df: DataFrame,
    use_cols: list[str],
) -> DataFrame:
    df = df.withColumn(
        "dumptruck_code",
        F.regexp_replace(F.col("dumptruck_code").cast("string"), r"\.0$", "")
    )

    df = df.withColumn(
        "customer_name",
        F.regexp_replace("customer", "_", " ")
    )

    df = df.withColumn(
        "refuel_timedelta",
        F.expr(
            "make_interval(hours => int(split(refuel_time_offset, '\\\\.')[0]), "
            "minutes => int(split(refuel_time_offset, '\\\\.')[1]))"
        )
    )

    df = df.withColumn(
        "refuel_round_end_time",
        F.col("refuel_date") + F.col("refuel_timedelta")
    )

    df = df.filter(F.col("refuel_qty") <= 0)

    return keep_with_cols(df, use_cols)


def clean_refuel(df: DataFrame) -> DataFrame:
    w = Window.partitionBy(
        "customer_code",
        "customer_name",
        "dumptruck_model",
        "dumptruck_serial_number",
        "dumptruck_code",
    ).orderBy("refuel_round_end_time")

    df = (
        df.groupBy(
            "customer_code",
            "customer_name",
            "dumptruck_model",
            "dumptruck_serial_number",
            "dumptruck_code",
            "refuel_round_end_time",
        )
        .agg(F.sum("refuel_qty").alias("refuel_qty"))
    )

    df = df.withColumn(
        "refuel_round_start_time",
        F.lag("refuel_round_end_time").over(w)
    )

    df = df.filter(F.col("refuel_round_start_time").isNotNull())
    df = df.withColumn("refuel_qty", F.abs("refuel_qty"))

    return df
