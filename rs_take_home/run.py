"""Main entrypoint to count disease associations based on queries."""
from typing import List

from pyspark.sql import DataFrame, SparkSession

from rs_take_home.association_counter import AssociationCounter
from rs_take_home.data_models import (
    DiseaseHierarchy,
    GeneDiseaseAssociations,
    Queries,
)

_gene_disease_queries: List[tuple] = [
    ('ENSG00000101342', 'MONDO:0019557'),
    ('ENSG00000101347', 'MONDO:0015574'),
    ('ENSG00000213689', 'MONDO:0019557'),
    ('ENSG00000213689', 'MONDO:0018827'),
]


def _create_spark_session() -> SparkSession:
    return (
        SparkSession
        .builder
        .master('local[1]')
        .appName('main')
        .getOrCreate()
    )


def get_association_counts(
    *,
    gene_disease_associations_path: str,
    disease_hierarchy_path: str,
    list_of_queries: List[tuple] = _gene_disease_queries,
    spark: SparkSession = _create_spark_session(),  # noqa: B008, WPS404
) -> DataFrame:
    """Get disease gene association counts for all queries.

    Args:
        gene_disease_associations_path (str): filepath to associations
        disease_hierarchy_path (str): filepath to disease hierarchy
        list_of_queries (List[tuple]): list of (gene_id, diseaes_id)
        spark (SparkSession): spark session allowing custom spark configs

    Returns:
        association counts (DataFrame): dataframe summarizing counts
    """
    disease_hierarchy = DiseaseHierarchy.from_filepath(
        disease_hierarchy_path, spark,
    )
    queries = Queries(queries=list_of_queries, spark=spark)
    gene_disease_associations = GeneDiseaseAssociations.from_filepath(
        gene_disease_associations_path, spark,
    )
    association_counter = AssociationCounter(
        disease_hierarchy=disease_hierarchy,
        gene_disease_associations=gene_disease_associations,
        queries=queries,
        spark=spark,
    )
    return association_counter.count_associations()
