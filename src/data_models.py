from typing import List

from pydantic import BaseModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as fx

disease_id_parent_col = 'disease_id_parent'
disease_id_child_col = 'disease_id_child'


class BaseModelArbitrary(BaseModel):

    class Config:  # noqa: WPS306, WPS431
        arbitrary_types_allowed = True


class GeneDiseaseAssociations(BaseModelArbitrary):
    df: DataFrame


class DiseaseHierarchy(BaseModelArbitrary):
    df: DataFrame

    def get_children_and_parent_diseases(
        self,
        disease_id: str,
    ) -> List[str]:
        child_diseases = self._get_child_diseases(disease_id)
        parent_diseases = self._get_parent_diseases(disease_id)
        parent_child_diseases = [child_diseases, *parent_diseases]
        return list(set(parent_child_diseases))

    def _get_child_diseases(
        self,
        disease_id: str,
    ) -> DataFrame:
        return (
            self.df
            .filter(
                fx.col(disease_id_parent_col) == disease_id,
            )
            .select(disease_id_child_col)
            .toPandas()
            [disease_id_child_col]
            .tolist()
        )

    def _get_parent_diseases(
        self,
        disease_id: str,
    ) -> DataFrame:
        return (
            self.df
            .filter(
                fx.col(disease_id_child_col) == disease_id,
            )
            .select(disease_id_parent_col)
            .toPandas()
            [disease_id_parent_col]
            .tolist()
        )
