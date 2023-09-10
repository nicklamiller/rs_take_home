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
