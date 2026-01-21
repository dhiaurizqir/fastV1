"""
Standard typecasting and cleaning for L02 (PySpark).
Equivalent to Kedro standard_processing node.
"""

from typing import Dict, List, Optional, Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    TimestampType,
)

from fuel.pipeline.l02_intermediate.utils import clean_col_names, deduplicate, META_COL


# ------------------------------------------------------------------
# Helper: cast string dtype from config to SparkType
# ------------------------------------------------------------------
def _map_dtype(dtype: str):
    dtype = dtype.lower()
    if dtype == "string":
        return StringType()
    if dtype in {"int64", "int"}:
        return LongType()
    if dtype in {"float64", "float"}:
        return DoubleType()
    if dtype == "datetime64[ns]":
        return TimestampType()
    raise ValueError(f"Unsupported dtype: {dtype}")


# ------------------------------------------------------------------
# Main L02 processing function
# ------------------------------------------------------------------
def standard_processing(
    data: Union[DataFrame, Dict[str, DataFrame]],
    dtype: Optional[Dict[str, str]] = None,
    col_names: Optional[Dict[str, str]] = None,
    primary_keys: Optional[List[str]] = None,
    order_by: Optional[List[str]] = None,
    partition_key: Optional[str] = None,
    usecols: Optional[List[str]] = None,
    dt_format: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """
    PySpark equivalent of Kedro L02 standard_processing.

    Steps:
    1. union data if dict
    2. clean & rename columns
    3. drop fully-null rows
    4. trim string & empty -> null
    5. cast datatype
    6. lowercase string
    7. deduplicate
    """

    # --------------------------------------------------------------
    # 1. Union if input is dict (PartitionedDataSet equivalent)
    # --------------------------------------------------------------
    if isinstance(data, dict):
        data = (
            list(data.values())[0]
            .sparkSession
            .createDataFrame([], list(data.values())[0].schema)
            .unionByName(*data.values())
        )

    partition_key = partition_key or ""

    # --------------------------------------------------------------
    # 2. Clean & rename columns
    # --------------------------------------------------------------
    df = data.drop(META_COL, errors="ignore")
    df = clean_col_names(df).dropDuplicates()

    if col_names:
        rename_expr = {
            k.lower(): v for k, v in col_names.items()
        }
        for old, new in rename_expr.items():
            if old in df.columns:
                df = df.withColumnRenamed(old, new)

    # --------------------------------------------------------------
    # 3. Drop rows where all data columns are null
    # --------------------------------------------------------------
    data_cols = [
        c for c in df.columns if c not in {partition_key, META_COL}
    ]
    df = df.dropna(how="all", subset=data_cols)

    # --------------------------------------------------------------
    # 4. Trim strings & empty string -> null
    # --------------------------------------------------------------
    for col in df.columns:
        if isinstance(df.schema[col].dataType, StringType):
            df = df.withColumn(
                col,
                F.when(
                    F.trim(F.col(col)).isin("", "nan"),
                    F.lit(None)
                ).otherwise(
                    F.lower(
                        F.trim(
                            F.regexp_replace(F.col(col), r"^['\"]|['\"]$", "")
                        )
                    )
                )
            )

    # --------------------------------------------------------------
    # 5. Keep only selected columns
    # --------------------------------------------------------------
    if usecols:
        df = df.select(*usecols)

    # --------------------------------------------------------------
    # 6. Cast datatype
    # --------------------------------------------------------------
    if dtype:
        for col, d in dtype.items():
            if col not in df.columns:
                continue

            if d == "datetime64[ns]":
                if dt_format and dt_format.get(col):
                    df = df.withColumn(
                        col,
                        F.to_timestamp(F.col(col), dt_format[col])
                    )
                else:
                    df = df.withColumn(col, F.to_timestamp(F.col(col)))
            else:
                df = df.withColumn(col, F.col(col).cast(_map_dtype(d)))

    # --------------------------------------------------------------
    # 7. Drop null primary keys (same as pandas)
    # --------------------------------------------------------------
    if primary_keys:
        for pk in primary_keys:
            df = df.filter(F.col(pk).isNotNull())

    # --------------------------------------------------------------
    # 8. Deduplicate
    # --------------------------------------------------------------
    df = deduplicate(df, primary_keys, order_by)

    return df
