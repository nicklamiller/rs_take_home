import json
from typing import List

from pyspark.sql import SparkSession

from rs_take_home.association_counter import AssociationCounter
from rs_take_home.data_models import DiseaseHierarchy, GeneDiseaseAssociations

spark = (
    SparkSession
    .builder
    .master('local[1]')
    .appName('main')
    .getOrCreate()
)


with open('config/filepaths.json') as json_file:
    _filepaths_config = json.load(json_file)


_default_gene_disease_queries = [
    ('ENSG00000101342', 'MONDO:0019557'),
    ('ENSG00000101347', 'MONDO:0015574'),
    ('ENSG00000213689', 'MONDO:0019557'),
    ('ENSG00000213689', 'MONDO:0018827'),
]

_default_gene_disease_associations = (
    GeneDiseaseAssociations
    .from_filepath(_filepaths_config['gene_disease_associations'], spark)
)


_default_disease_hierarchy = (
    DiseaseHierarchy
    .from_filepath(_filepaths_config['disease_hierarchy'], spark)
)


def get_association_counts(
    gene_disease_associations: GeneDiseaseAssociations = None,
    disease_hierarchy: DiseaseHierarchy = None,
    queries: List[tuple] = _default_gene_disease_queries,
    spark: SparkSession = spark,
):
    if not gene_disease_associations:
        gene_disease_associations = _default_gene_disease_associations
    if not disease_hierarchy:
        disease_hierarchy = _default_disease_hierarchy
    association_counter = AssociationCounter(
        disease_hierarchy=disease_hierarchy,
        gene_disease_associations=gene_disease_associations,
        spark=spark,
    )
    return association_counter.count_all_queries(queries)
