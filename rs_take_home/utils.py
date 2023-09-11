from typing import List

from pyspark.sql import DataFrame, SparkSession


def spark_read_csv(filepath: str, spark: SparkSession) -> DataFrame:
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
    for column_and_type in schema:
        if column_and_type not in df.dtypes:
            raise ValueError(
                f'{column_and_type} needs to be in input df schema',
            )
