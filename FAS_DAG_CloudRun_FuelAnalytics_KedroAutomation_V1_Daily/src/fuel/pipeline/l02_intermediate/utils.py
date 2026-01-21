"""
Utility functions used in processing raw data (PySpark version).
"""

from typing import List, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

META_COL = "cols_changed"


def clean_col_names(df: DataFrame) -> DataFrame:
    """
    Clean column names: lowercase and strip spaces.
    """
    for col in df.columns:
        df = df.withColumnRenamed(col, col.lower().strip())
    return df


def _get_column_order(
    primary_keys: List[str], desc_by: List[str], all_columns: List[str]
) -> List[str]:
    """
    Determine column order:
    primary_keys + desc_by + remaining columns (sorted).
    """
    other_value_columns = sorted(
        set(all_columns) - (set(primary_keys) | set(desc_by))
    )
    return primary_keys + desc_by + other_value_columns


def deduplicate(
    data: DataFrame,
    primary_keys: Optional[List[str]] = None,
    desc_by: Optional[List[str]] = None,
) -> DataFrame:
    """
    Deduplicate dataframe.

    - If primary_keys provided:
        * sort descending by primary_keys + desc_by + remaining columns
        * keep first row per primary key
        * drop rows where primary_keys is null
    - Else:
        * drop duplicates on all columns
    """
    if primary_keys:
        if desc_by is None:
            desc_by = []

        all_columns = data.columns
        column_order = _get_column_order(primary_keys, desc_by, all_columns)

        # drop rows with null primary keys
        not_null_cond = [F.col(c).isNotNull() for c in primary_keys]
        not_null_data = data.filter(F.reduce(lambda a, b: a & b, not_null_cond))

        # build window spec
        window_spec = (
            Window.partitionBy(*primary_keys)
            .orderBy(*[F.col(c).desc_nulls_last() for c in column_order])
        )

        deduped = (
            not_null_data
            .withColumn("__row_number", F.row_number().over(window_spec))
            .filter(F.col("__row_number") == 1)
            .drop("__row_number")
        )
    else:
        deduped = data.dropDuplicates()

    return deduped
