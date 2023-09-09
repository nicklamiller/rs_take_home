from typing import List

from pydantic import BaseModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as fx

_disease_id_parent_col = 'disease_id_parent'
_disease_id_child_col = 'disease_id_child'


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
        parent_child_diseases = [*child_diseases, *parent_diseases]
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
