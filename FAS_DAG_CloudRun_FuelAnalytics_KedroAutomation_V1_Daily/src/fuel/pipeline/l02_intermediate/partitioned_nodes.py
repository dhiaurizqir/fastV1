"""
Nodes for processing partitioned datasets (PySpark version).
"""

from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from caseconverter import snakecase

from fuel.pipeline.l02_intermediate.typecasting_nodes import standard_processing


def _get_filename_map(
    input_filenames: List[str],
    relevant_filenames: Dict[str, str],
) -> Dict[str, str]:
    """
    Creates a map of input filename to output partition name.
    """
    if relevant_filenames:
        return {
            f: relevant_filenames[f]
            for f in input_filenames
            if f in relevant_filenames
        }
    else:
        return {f: snakecase(f) for f in input_filenames if len(f) > 0}


def partitioned_processing(
    datasets: Dict[str, DataFrame],
    relevant_filenames: Dict[str, str],
    dtype: Optional[Dict[str, str]] = None,
    col_names: Optional[Dict[str, str]] = None,
    primary_keys: Optional[List[str]] = None,
    order_by: Optional[List[str]] = None,
    partition_key: Optional[str] = None,
    usecols: Optional[List[str]] = None,
    dt_format: Optional[Dict[str, str]] = None,
) -> Dict[str, DataFrame]:
    """
    Process partitioned datasets to L02 layer (PySpark).

    Args:
        datasets: dict of {partition_filename: Spark DataFrame}
        relevant_filenames: map of input filename → output partition name
        dtype, col_names, primary_keys, order_by, partition_key, usecols, dt_format:
            forwarded directly to standard_processing

    Returns:
        Dict[str, DataFrame]: output partition name → processed DataFrame
    """
    input_filenames = list(datasets.keys())
    filename_map = _get_filename_map(input_filenames, relevant_filenames)

    output: Dict[str, DataFrame] = {}

    for in_fn, out_fn in filename_map.items():
        df = datasets[in_fn]

        processed_df = standard_processing(
            data=df,
            dtype=dtype,
            col_names=col_names,
            primary_keys=primary_keys,
            order_by=order_by,
            partition_key=partition_key,
            usecols=usecols,
            dt_format=dt_format,
        )

        output[out_fn] = processed_df

    return output
