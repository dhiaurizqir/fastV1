from typing import Dict, List
from pyspark.sql import DataFrame, functions as F


def remove_outlier(
    df: DataFrame,
    outlier_column: str,
) -> DataFrame:
    """
    Remove outliers using IQR method (Spark version).

    Equivalent to pandas:
    Q1 - 1.5 * IQR <= value <= Q3 + 1.5 * IQR
    """
    q1, q3 = df.approxQuantile(outlier_column, [0.25, 0.75], 0.01)
    iqr = q3 - q1

    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    return df.filter(
        (F.col(outlier_column) >= lower_bound)
        & (F.col(outlier_column) <= upper_bound)
    )


def standarization_columns(
    df: DataFrame,
    col_names: Dict[str, str],
    uppercase_column: List[str],
    selected_cols: List[str],
) -> DataFrame:
    """
    Standardize column names, uppercase selected columns,
    replace 'UNKNOWN' ExcavatorModel with null, and select columns.
    """
    renamed_df = df

    # Rename columns (lowercase source key to match pandas behavior)
    for src_col, dst_col in col_names.items():
        renamed_df = renamed_df.withColumnRenamed(src_col.lower(), dst_col)

    # Uppercase selected columns
    for col in uppercase_column:
        if col in renamed_df.columns:
            renamed_df = renamed_df.withColumn(col, F.upper(F.col(col)))

    # Replace UNKNOWN ExcavatorModel with null
    if "ExcavatorModel" in renamed_df.columns:
        renamed_df = renamed_df.withColumn(
            "ExcavatorModel",
            F.when(F.col("ExcavatorModel") == "UNKNOWN", F.lit(None))
             .otherwise(F.col("ExcavatorModel"))
        )

    return renamed_df.select(*selected_cols)


def remove_outlier_and_standarization_cols(
    df: DataFrame,
    outlier_column: str,
    rename_cols: Dict[str, str],
    uppercase_column: List[str],
    selected_cols: List[str],
) -> DataFrame:
    """
    Remove outliers and then standardize columns.
    """
    df_no_outlier = remove_outlier(df, outlier_column)

    return standarization_columns(
        df_no_outlier,
        rename_cols,
        uppercase_column,
        selected_cols,
    )
