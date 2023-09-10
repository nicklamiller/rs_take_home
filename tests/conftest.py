import json

import pytest
from pyspark.sql import Row

from src.association_counter import AssociationCounter
from src.data_models import DiseaseHierarchy, GeneDiseaseAssociations


@pytest.fixture
def efo_0005809_diseases():
    return [
        'EFO:0005809',
        'EFO:0000540',
        'MONDO:0004670',
        'EFO:1002003',
        'MONDO:0000603',
    ]


@pytest.fixture
def gene_disease_queries():
    return [
        ('ENSG00000213689', 'MONDO:0019557'),
        ('ENSG00000184584', 'MONDO:0018827'),
    ]


@pytest.fixture
def query_counts_df(spark_session):
    return spark_session.createDataFrame([
        Row(Query='(ENSG00000213689, MONDO:0019557)', Result=2),
        Row(Query='(ENSG00000184584, MONDO:0018827)', Result=1),
    ])


@pytest.fixture
def filepaths_config():
    with open('config/filepaths.json') as json_file:
        filepaths_config = json.load(json_file)
    return filepaths_config


@pytest.fixture
def disease_hierarchy(
    filepaths_config,
    spark_session,
):
    return (
        DiseaseHierarchy
        .from_filepath(
            filepaths_config['disease_hierarchy'],
            spark=spark_session,
        )
    )


@pytest.fixture
def gene_disease_associations(
    filepaths_config,
    spark_session,
):
    return (
        GeneDiseaseAssociations
        .from_filepath(
            filepaths_config['gene_disease_associations'],
            spark=spark_session,
        )
    )


@pytest.fixture
def association_counter(
    disease_hierarchy,
    gene_disease_associations,
    spark_session,
):
    return AssociationCounter(
        disease_hierarchy=disease_hierarchy,
        gene_disease_associations=gene_disease_associations,
        spark=spark_session,
    )
