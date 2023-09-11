import json

import pytest
from pyspark.sql import Row

from rs_take_home.association_counter import AssociationCounter
from rs_take_home.data_models import DiseaseHierarchy, GeneDiseaseAssociations


@pytest.fixture
def mondo_0019557_diseases():
    return [
        'MONDO:0019557',
        'MONDO:0015574',
        'MONDO:0018827',
        'MONDO:0019293',
        'MONDO:0000603',
    ]


@pytest.fixture
def gene_disease_queries():
    return [
        ('ENSG00000101342', 'MONDO:0019557'),
        ('ENSG00000101347', 'MONDO:0015574'),
        ('ENSG00000213689', 'MONDO:0019557'),
        ('ENSG00000213689', 'MONDO:0018827'),
    ]


@pytest.fixture
def query_counts_df(spark_session):
    return spark_session.createDataFrame([
        Row(Query='(ENSG00000101342, MONDO:0019557)', Result=4),
        Row(Query='(ENSG00000101347, MONDO:0015574)', Result=3),
        Row(Query='(ENSG00000213689, MONDO:0019557)', Result=4),
        Row(Query='(ENSG00000213689, MONDO:0018827)', Result=3),
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
    filepaths_config,
    spark_session,
):
    return AssociationCounter.from_config(filepaths_config, spark_session)
