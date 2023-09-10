from typing import List

from pyspark.sql import DataFrame, Row, SparkSession

from src.data_models import (
    BaseModelArbitrary,
    DiseaseHierarchy,
    GeneDiseaseAssociations,
)


class AssociationCounter(BaseModelArbitrary):
    disease_hierarchy: DiseaseHierarchy
    gene_disease_associations: GeneDiseaseAssociations
    spark: SparkSession

    def count_all_queries(
        self,
        queries: List[tuple],
    ) -> DataFrame:
        all_query_counts = []
        for query in queries:
            gene_id, disease_id = query
            single_query_count = self._count_single_query(
                disease_id=disease_id,
                gene_id=gene_id,
            )
            all_query_counts.append(single_query_count)
        return self.spark.createDataFrame(all_query_counts)

    def _count_single_query(
        self,
        *,
        disease_id: str,
        gene_id: str,
    ) -> Row:
        child_parent_diseases = (
            self.disease_hierarchy
            .get_child_and_parent_diseases(disease_id=disease_id)
        )
        gene_disease_counts = (
            self.gene_disease_associations
            .count_associations(
                list_disease_ids=child_parent_diseases,
                gene_id=gene_id,
            )
        )
        query = f'({gene_id}, {disease_id})'
        return Row(Query=query, Result=gene_disease_counts)
