"""Data models for inputs."""
from typing import List

from pydantic import BaseModel, validator
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as fx

from rs_take_home import schemas
from rs_take_home.utils import check_has_required_schema_subset, spark_read_csv

_disease_id_parent_col = 'disease_id_parent'
_disease_id_child_col = 'disease_id_child'
_disease_id_col = 'disease_id'
_gene_id_col = 'gene_id'
_query_col = 'Query'


class BaseModelArbitrary(BaseModel):  # noqa: D101

    class Config:  # noqa: D106, WPS306, WPS431
        arbitrary_types_allowed = True


class Queries(BaseModelArbitrary):
    """Gene and disease queries."""

    queries: List[tuple]
    spark: SparkSession

    @property
    def df(self) -> DataFrame:
        """Queries turned into dataframe with useful columns.

        Returns:
            DataFrame: dataframe representation of queries
        """
        return (
            self.spark.createDataFrame(
                data=self.queries,
                schema=[_gene_id_col, _disease_id_col],
            )
            .withColumn(
                _query_col,
                fx.concat(
                    fx.lit('('),
                    fx.col(_gene_id_col),
                    fx.lit(', '),
                    fx.col(_disease_id_col),
                    fx.lit(')'),
                ),
            )
            .select(_query_col, _gene_id_col, _disease_id_col)
        )


class GeneDiseaseAssociations(BaseModelArbitrary):
    """Gene Disease Associations with validation."""

    df: DataFrame

    @validator('df')
    @classmethod
    def check_schema(cls, value):
        """Check schema is subset of dataframe schema.

        Args:
            value (DataFrame): dataframe to check schema

        Returns:
            ValueError: column and type not in df schema
        """
        check_has_required_schema_subset(
            value,
            schemas.gene_disease_associations_schema,
        )
        return value

    @classmethod
    def from_filepath(cls, filepath: str, spark: SparkSession):
        """Instantiate from filepath config.

        Args:
            filepath (str): filepath to gene disease associations csv
            spark (SparkSession): spark session allowing custom spark configs

        Returns:
            GeneDiseaseAssociations
        """
        return cls(df=spark_read_csv(filepath, spark))


class DiseaseHierarchy(BaseModelArbitrary):
    """Disease Hierarchy with validation."""

    df: DataFrame

    @validator('df')
    @classmethod
    def check_schema(cls, value):
        """Check schema is subset of dataframe schema.

        Args:
            value (DataFrame): dataframe to check schema

        Returns:
            ValueError: column and type not in df schema
        """
        check_has_required_schema_subset(
            value,
            schemas.disease_hierarchy_schema,
        )
        return value

    @classmethod
    def from_filepath(cls, filepath: str, spark: SparkSession):
        """Instantiate from filepath config.

        Args:
            filepath (str): filepath to disease hierarchy csv
            spark (SparkSession): spark session allowing custom spark configs

        Returns:
            DiseaseHierarchy
        """
        return cls(df=spark_read_csv(filepath, spark))
