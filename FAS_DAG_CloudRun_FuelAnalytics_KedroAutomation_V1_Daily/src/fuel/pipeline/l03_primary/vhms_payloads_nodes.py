"""
VHMS payload nodes (PySpark version).
"""

from typing import Dict, List

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


# ------------------------------------------------------------------
# Pivot payload
# ------------------------------------------------------------------
def pivot_payload(
    df: DataFrame,
    pivot_params: Dict[str, List[str]],
    rename_dict: Dict[str, str],
    fillna_dict: Dict[str, float],
) -> DataFrame:
    """
    Pivot payload dataset from long to wide (Spark version).
    """

    # Clean dumptruck_serial_number
    df = df.withColumn(
        "dumptruck_serial_number",
        F.regexp_replace(
            F.col("dumptruck_serial_number").cast("string"),
            r"\.0$",
            "",
        ),
    )

    # Required pivot params
    index_cols = pivot_params["index"]
    columns_col = pivot_params["columns"]
    values_col = pivot_params["values"]

    # Pivot
    df_pivot = (
        df.groupBy(*index_cols)
        .pivot(columns_col)
        .agg(F.first(values_col))
    )

    # Rename columns
    for old, new in rename_dict.items():
        if old in df_pivot.columns:
            df_pivot = df_pivot.withColumnRenamed(old, new)

    return df_pivot.fillna(fillna_dict)


# ------------------------------------------------------------------
# Apply shift rules
# ------------------------------------------------------------------
def apply_shift_rules(df: DataFrame) -> DataFrame:
    """
    Apply shift determination rules (Spark version).
    """

    shift_1_start = df.select("shift_1_hour").limit(1).collect()[0][0]
    shift_2_start = df.select("shift_2_hour").limit(1).collect()[0][0]

    # Default shift = '2'
    df = df.withColumn("shift", F.lit("2"))

    # Shift 1 condition
    df = df.withColumn(
        "shift",
        F.when(
            F.col("cycle_start_hr").between(
                shift_1_start,
                shift_2_start - 1,
            ),
            F.lit("1"),
        ).otherwise(F.col("shift")),
    )

    # Adjust date for shift 2 early hours
    df = df.withColumn(
        "date",
        F.when(
            (F.col("shift") == "2")
            & (F.col("cycle_start_hr") < shift_1_start),
            F.date_sub(F.col("date"), 1),
        ).otherwise(F.col("date")),
    )

    return df


# ------------------------------------------------------------------
# Create timestamps payloads
# ------------------------------------------------------------------
def create_timestamps_payloads(
    df: DataFrame,
    df_shift_cust: DataFrame,
) -> DataFrame:
    """
    Create cycle timestamps and shift times (Spark version).
    """

    # Window for cycle end time
    w = (
        Window.partitionBy(
            "dumptruck_serial_number",
            "dumptruck_model",
            "customer_name",
        )
        .orderBy("cycle_start_time")
    )

    df = df.withColumn(
        "cycle_end_time",
        F.lead("cycle_start_time").over(w),
    )

    df = (
        df.withColumn("date", F.to_date("cycle_start_time"))
        .withColumn("cycle_start_hr", F.hour("cycle_start_time"))
    )

    # Join with customer shift
    df_merge = df.join(
        df_shift_cust,
        df.customer_name == df_shift_cust.customer,
        how="inner",
    )

    # Apply shift rules
    df_merge = apply_shift_rules(df_merge)

    # Shift start & end time (equivalent to _get_row_shift_times)
    df_merge = df_merge.withColumn(
        "shift_start_time",
        F.when(
            F.col("shift") == "1",
            F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.col("date"),
                    F.lpad(F.col("shift_1_hour"), 2, "0"),
                    F.lit(":00:00"),
                )
            ),
        ).otherwise(
            F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.col("date"),
                    F.lpad(F.col("shift_2_hour"), 2, "0"),
                    F.lit(":00:00"),
                )
            )
        ),
    )

    df_merge = df_merge.withColumn(
        "shift_end_time",
        F.col("shift_start_time") + F.expr("INTERVAL 12 HOURS"),
    )

    # Cycle ID
    df_merge = df_merge.withColumn(
        "cycle_id",
        F.concat_ws(
            "__",
            F.date_format("cycle_start_time", "yyyy-MM-dd HH:mm:ss"),
            F.date_format("cycle_end_time", "yyyy-MM-dd HH:mm:ss"),
        ),
    )

    return df_merge


