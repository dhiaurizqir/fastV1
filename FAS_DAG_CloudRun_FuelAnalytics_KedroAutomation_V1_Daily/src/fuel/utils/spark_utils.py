"""
Universal Spark utilities for dataframe transformations.
Safe to use across multiple Kedro layers (L02, L03, L04).

IMPORTANT:
- Designed for PySpark DataFrame
- Mirrors pd_utils behavior (1:1)
"""

from functools import reduce
from typing import Any, Dict, List, Optional, Union, Literal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ============================================================================
# Core functions (equivalent to pd_utils)
# ============================================================================

def merge_dataframes(
    dataframes: List[DataFrame],
    on: Union[List[str], str],
    how: Literal["left", "right", "outer", "inner"] = "outer",
    fill_nans: Optional[Any] = None,
) -> DataFrame:
    """
    Merge multiple Spark DataFrames on given keys.
    Equivalent to pd_utils.merge_dataframes.
    """
    if isinstance(on, str):
        on = [on]

    casted_dfs = []
    for df in dataframes:
        for col in on:
            df = df.withColumn(col, F.col(col).cast("string"))
        casted_dfs.append(df)

    merged = reduce(lambda l, r: l.join(r, on=on, how=how), casted_dfs)

    if fill_nans is not None:
        merged = merged.fillna(fill_nans)

    return merged


def concat_dataframes(
    dfs: List[DataFrame],
    distinct: bool = False,
) -> DataFrame:
    """
    Union multiple Spark DataFrames by column name.
    """
    df = reduce(lambda l, r: l.unionByName(r, allowMissingColumns=True), dfs)
    return df.distinct() if distinct else df


def filter_dataframe(
    df: DataFrame,
    filter_dict: Dict[str, Union[List[Any], Any]],
) -> DataFrame:
    """
    Filter Spark DataFrame using equality or IN condition.
    """
    for col, val in filter_dict.items():
        if isinstance(val, (list, set, tuple)):
            df = df.filter(F.col(col).isin(list(val)))
        else:
            df = df.filter(F.col(col) == val)
    return df


def keep_with_cols(df: DataFrame, use_cols: List[str]) -> DataFrame:
    """
    Select columns and drop duplicates.
    """
    return df.select(*use_cols).dropDuplicates()


def dropna_dataframe(
    df: DataFrame,
    subset: Union[str, List[str]],
) -> DataFrame:
    """
    Drop rows where all specified columns are null.
    """
    if isinstance(subset, str):
        subset = [subset]
    return df.dropna(subset=subset, how="all")


def replace_values(
    df: DataFrame,
    replace_dict: Dict[str, Dict[str, str]],
    regex: bool = False,
) -> DataFrame:
    """
    Replace values in Spark DataFrame.
    """
    for col, mapping in replace_dict.items():
        for old, new in mapping.items():
            if regex:
                df = df.withColumn(col, F.regexp_replace(F.col(col), old, new))
            else:
                df = df.withColumn(
                    col,
                    F.when(F.col(col) == old, new).otherwise(F.col(col)),
                )
    return df


def cast_bool_columns_to_str(df: DataFrame) -> DataFrame:
    """
    Cast all boolean columns to string ("True"/"False").
    """
    for field in df.schema.fields:
        if field.dataType.simpleString() == "boolean":
            df = df.withColumn(
                field.name,
                F.when(F.col(field.name) == True, F.lit("True"))
                 .when(F.col(field.name) == False, F.lit("False"))
                 .otherwise(None)
            )
    return df


def reset_df_index(df: DataFrame) -> DataFrame:
    """
    Spark has no index; kept for API compatibility.
    """
    return df


def rename_columns(df: DataFrame, col_names: Dict[str, str]) -> DataFrame:
    """
    Rename Spark DataFrame columns using mapping.
    """
    for old, new in col_names.items():
        df = df.withColumnRenamed(old, new)
    return df


# ============================================================================
# Kedro NODE WRAPPERS (this is what your pipeline uses)
# ============================================================================

def merge_dataframes_node(
    params: Dict[str, Any],
    *dfs: DataFrame,
) -> DataFrame:
    """
    Thin Kedro node wrapper over merge_dataframes.
    Equivalent to pd_utils.merge_dataframes_node.
    """
    return merge_dataframes(dfs, **params)
