from typing import List

from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as fx

from src.utils import spark_read_csv

_disease_id_parent_col = 'disease_id_parent'
_disease_id_child_col = 'disease_id_child'
_disease_id_col = 'disease_id'
_gene_id_col = 'gene_id'


class BaseModelArbitrary(BaseModel):

    class Config:  # noqa: WPS306, WPS431
        arbitrary_types_allowed = True


class GeneDiseaseAssociations(BaseModelArbitrary):
    df: DataFrame

    @classmethod
    def from_filepath(cls, filepath: str, spark: SparkSession):
        return cls(df=spark_read_csv(filepath, spark))

    def count_associations(
        self,
        *,
        list_disease_ids: List[str],
        gene_id: str,
    ) -> int:
        return (
            self.df
            .filter(
                (fx.col(_disease_id_col).isin(list_disease_ids))
                and (fx.col(_gene_id_col) == gene_id),
            )
            .distinct()
            .count()
        )


class DiseaseHierarchy(BaseModelArbitrary):
    df: DataFrame

    @classmethod
    def from_filepath(cls, filepath: str, spark: SparkSession):
        return cls(df=spark_read_csv(filepath, spark))

    def get_child_and_parent_diseases(
        self,
        disease_id: str,
    ) -> List[str]:
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