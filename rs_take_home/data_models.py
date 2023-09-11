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


class BaseModelArbitrary(BaseModel):  # noqa: D101

    class Config:  # noqa: D106, WPS306, WPS431
        arbitrary_types_allowed = True


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

    def count_associations(
        self,
        *,
        list_disease_ids: List[str],
    ) -> int:
        """Count unique genes for disease id's.

        Args:
            list_disease_ids (List[str]): list of disease ids

        Returns:
            int: count of unique genes for disease_ids
        """
        return (
            self.df
            .filter(
                fx.col(_disease_id_col).isin(list_disease_ids),
            )
            .select(_gene_id_col)
            .distinct()
            .count()
        )


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

    def get_child_and_parent_diseases(
        self,
        disease_id: str,
    ) -> List[str]:
        """Get child and parent disease from disease hierarchy.

        Args:
            disease_id (str): disease id to get parents and children of

        Returns:
            List[str]: list of parent and children disese ids
        """
        child_diseases = self._get_related_diseases(
            disease_id=disease_id,
            reference_col=_disease_id_parent_col,
            col_to_check=_disease_id_child_col,
        )
        parent_diseases = self._get_related_diseases(
            disease_id=disease_id,
            reference_col=_disease_id_child_col,
            col_to_check=_disease_id_parent_col,
        )
        parent_child_diseases = [disease_id, *child_diseases, *parent_diseases]
        return list(set(parent_child_diseases))

    def _get_related_diseases(
        self,
        *,
        disease_id: str,
        reference_col: str,
        col_to_check: str,
    ) -> List[str]:
        return (
            self.df
            .filter(
                fx.col(reference_col) == disease_id,
            )
            .select(col_to_check)
            .toPandas()
            [col_to_check]
            .tolist()
        )
