import json

import pytest
from pyspark.sql import Row
from pyspark_test import assert_pyspark_df_equal

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


def test_get_child_and_parent_diseases(
    disease_hierarchy,
    efo_0005809_diseases,
):
    children_parent_diseases = (
        disease_hierarchy
        .get_child_and_parent_diseases('EFO:0005809')
    )
    is_child_parent_disease = [
        _ in efo_0005809_diseases for _ in children_parent_diseases
    ]
    assert all(is_child_parent_disease)


def test_count_associations(
    gene_disease_queries,
    disease_hierarchy,
    gene_disease_associations,
):
    children_parent_diseases = (
        disease_hierarchy
        .get_child_and_parent_diseases(gene_disease_queries[0][1])
    )
    disease_count = (
        gene_disease_associations
        .count_associations(
            list_disease_ids=children_parent_diseases,
            gene_id=gene_disease_queries[0][0],
        )
    )
    assert disease_count == 2


def test_count_all_queries(
    gene_disease_queries,
    association_counter,
    query_counts_df,
):
    actual_query_counts_df = (
        association_counter.count_all_queries(gene_disease_queries)
    )
    assert_pyspark_df_equal(actual_query_counts_df, query_counts_df)
