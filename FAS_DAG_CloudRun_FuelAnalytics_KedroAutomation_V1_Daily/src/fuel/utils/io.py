from typing import Optional, List
from pyspark.sql import SparkSession, DataFrame


# =========================================================
# READERS
# =========================================================

def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def read_csv(
    spark: SparkSession,
    path: str,
    header: bool = True,
    infer_schema: bool = True,
) -> DataFrame:
    return (
        spark.read
        .option("header", header)
        .option("inferSchema", infer_schema)
        .csv(path)
    )


def read_dataset(
    spark: SparkSession,
    path: str,
    fmt: str = "parquet",
) -> DataFrame:
    if fmt == "parquet":
        return read_parquet(spark, path)
    elif fmt == "csv":
        return read_csv(spark, path)
    else:
        raise ValueError(f"Unsupported format: {fmt}")


# =========================================================
# WRITERS
# =========================================================

def write_parquet(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_cols: Optional[List[str]] = None,
):
    writer = df.write.mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)


def write_csv(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    header: bool = True,
):
    df.write.mode(mode).option("header", header).csv(path)


def write_dataset(
    df: DataFrame,
    path: str,
    fmt: str = "parquet",
    mode: str = "overwrite",
    partition_cols: Optional[List[str]] = None,
):
    if fmt == "parquet":
        write_parquet(df, path, mode, partition_cols)
    elif fmt == "csv":
        write_csv(df, path, mode)
    else:
        raise ValueError(f"Unsupported format: {fmt}")
