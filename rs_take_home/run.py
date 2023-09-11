import json
from typing import List

from pyspark.sql import DataFrame, SparkSession

from rs_take_home.association_counter import AssociationCounter
from rs_take_home.data_models import DiseaseHierarchy, GeneDiseaseAssociations
from rs_take_home.utils import spark_read_csv

spark = (
    SparkSession
    .builder
    .master('local[1]')
    .appName('main')
    .getOrCreate()
)


with open('config/filepaths.json') as json_file:
    _filepaths_config = json.load(json_file)


_gene_disease_queries = [
    ('ENSG00000101342', 'MONDO:0019557'),
    ('ENSG00000101347', 'MONDO:0015574'),
    ('ENSG00000213689', 'MONDO:0019557'),
    ('ENSG00000213689', 'MONDO:0018827'),
]

_gene_disease_associations_df = (
    spark_read_csv(_filepaths_config['gene_disease_associations'], spark)
)


_disease_hierarchy_df = (
    spark_read_csv(_filepaths_config['disease_hierarchy'], spark)
)


def get_association_counts(
    *,
    gene_disease_associations_df: DataFrame = _gene_disease_associations_df,
    disease_hierarchy_df: DataFrame = _disease_hierarchy_df,
    queries: List[tuple] = _gene_disease_queries,
    spark: SparkSession = spark,
) -> DataFrame:
    disease_hierarchy = DiseaseHierarchy(df=disease_hierarchy_df)
    gene_disease_associations = GeneDiseaseAssociations(
        df=gene_disease_associations_df,
    )
    association_counter = AssociationCounter(
        disease_hierarchy=disease_hierarchy,
        gene_disease_associations=gene_disease_associations,
        spark=spark,
    )
    return association_counter.count_all_queries(queries)
