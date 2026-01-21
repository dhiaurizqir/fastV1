"""
Fleet processing nodes for L03 (PySpark version).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

def clean_fleet(df: DataFrame) -> DataFrame:
    mapping = {
        "shift 1": 1, "1": 1, "ds": 1, "day": 1,
        "shift 2": 2, "2": 2, "ns": 2, "night": 2,
    }

    # normalize shift
    df = df.withColumn(
        "shift",
        F.lower(F.trim(F.col("shift")))
    )

    df = df.withColumn(
        "shift",
        F.create_map(
            *[F.lit(x) for x in sum(mapping.items(), ())]
        ).getItem(F.col("shift")).cast(IntegerType())
    )

    # dumptruck_model cleanup
    df = df.withColumn(
        "dumptruck_model",
        F.when(F.col("dumptruck_model") == "hd 785", "hd785-7")
         .otherwise(F.col("dumptruck_model"))
    )

    # remove .0 from codes
    df = df.withColumn(
        "dumptruck_code",
        F.regexp_replace(F.col("dumptruck_code").cast("string"), r"\.0$", "")
    )

    df = df.withColumn(
        "excavator_code",
        F.regexp_replace(F.col("excavator_code").cast("string"), r"\.0$", "")
    )

    # customer_name
    df = df.withColumn(
        "customer_name",
        F.regexp_replace(F.col("customer"), "_", " ")
    )

    return df

def get_shift_times(
    df: DataFrame,
    df_shift_cust: DataFrame,
) -> DataFrame:
    # rename for join
    df_shift_cust = df_shift_cust.withColumnRenamed(
        "customer", "customer_name"
    )

    df = df.join(df_shift_cust, on="customer_name", how="left")

    # base datetime
    base_date = F.to_timestamp(F.col("date"))

    # shift 1
    shift_1_start = base_date + F.expr("INTERVAL 1 HOUR") * F.col("shift_1_hour")
    shift_1_end = shift_1_start + F.expr("INTERVAL 12 HOURS")

    # shift 2
    shift_2_start = base_date + F.expr("INTERVAL 1 HOUR") * F.col("shift_2_hour")
    shift_2_end = shift_2_start + F.expr("INTERVAL 12 HOURS")

    df = df.withColumn(
        "shift_start_time",
        F.when(F.col("shift") == 1, shift_1_start)
         .when(F.col("shift") == 2, shift_2_start)
         .otherwise(F.lit(None))
    )

    df = df.withColumn(
        "shift_end_time",
        F.when(F.col("shift") == 1, shift_1_end)
         .when(F.col("shift") == 2, shift_2_end)
         .otherwise(F.lit(None))
    )

    return df
