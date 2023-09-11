"""Helper functions/utilities."""
from typing import List

from pyspark.sql import DataFrame, SparkSession


def spark_read_csv(filepath: str, spark: SparkSession) -> DataFrame:
    """Read csv's with useful spark options.

    Args:
        filepath (str): filepath to csv
        spark (SparkSession): spark session allowing custom spark configs

    Returns:
        spark (DataFrame): a dataframe
    """
    return (
        spark
        .read
        .csv(
            filepath,
            header=True,
            inferSchema=True,
        )
    )


def check_has_required_schema_subset(
    df: DataFrame,
    schema: List[tuple],
) -> None:
    """Check schema is subset of dataframe schema.

    Args:
        df (DataFrame): dataframe to check
        schema (List[tuple]): list of tuples specifying column and datatypes

    Raises:
        ValueError: column and type not in df schema
    """
    for column_and_type in schema:
        if column_and_type not in df.dtypes:
            raise ValueError(
                f'{column_and_type} needs to be in input df schema',
            )
