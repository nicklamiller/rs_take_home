"""Count Associations from inputs."""
from typing import List

from pyspark.sql import DataFrame, Row, SparkSession

from rs_take_home.data_models import (
    BaseModelArbitrary,
    DiseaseHierarchy,
    GeneDiseaseAssociations,
    Queries,
)


class AssociationCounter(BaseModelArbitrary):
    """Gene and disease association counter."""

    disease_hierarchy: DiseaseHierarchy
    gene_disease_associations: GeneDiseaseAssociations
    queries: Queries
    spark: SparkSession

    @classmethod
    def from_config(cls, config: dict, queries: Queries, spark: SparkSession):
        """Instantiate from filepath config.

        Args:
            config (dict): dict with filepaths for inputs
            queries (Queries): contains information on queries
            spark (SparkSession): spark session allowing custom spark configs

        Returns:
            AssociationCounter
        """
        disease_hierarchy = DiseaseHierarchy.from_filepath(
            config['disease_hierarchy'], spark,
        )
        gene_disease_associations = GeneDiseaseAssociations.from_filepath(
            config['gene_disease_associations'], spark,
        )
        return cls(
            disease_hierarchy=disease_hierarchy,
            gene_disease_associations=gene_disease_associations,
            queries=queries,
            spark=spark,
        )

    def count_all_queries(
        self,
        queries: List[tuple],
    ) -> DataFrame:
        """Count all supplied gene-disease queries.

        Args:
            queries (List[tuple]): list of gene_id and disease_id tuples

        Returns:
            DataFrame: dataframe with queries and counts.
        """
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
            )
        )
        query = f'({gene_id}, {disease_id})'
        return Row(Query=query, Result=gene_disease_counts)
