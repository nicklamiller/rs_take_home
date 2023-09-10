import json
from typing import List

from pyspark.sql import SparkSession

from rs_take_home.association_counter import AssociationCounter

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
    ('ENSG00000213689', 'MONDO:0019557'),
    ('ENSG00000184584', 'MONDO:0018827'),
]


def get_association_counts(
    queries: List[tuple] = _default_gene_disease_queries,
    config: dict = _filepaths_config,
    spark: SparkSession = spark,
):
    association_counter = AssociationCounter.from_config(config, spark)
    return association_counter.count_all_queries(queries)
