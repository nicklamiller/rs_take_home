import pytest
from pydantic import ValidationError
from pyspark.sql import Row
from pyspark_test import assert_pyspark_df_equal

from rs_take_home.data_models import DiseaseHierarchy, GeneDiseaseAssociations
from rs_take_home.utils import spark_read_csv


@pytest.fixture
def wrong_schema_df(spark_session):
    return (
        spark_session.createDataFrame([
            Row(not_disease_id='made_up_id'),
            Row(not_disease_id='some_other_id'),
        ])
    )


@pytest.fixture
def queries_df(spark_session):
    return spark_read_csv('tests/fixtures/queries_df.csv', spark_session)


def test_queries_df(queries, queries_df):
    assert_pyspark_df_equal(
        queries.df,
        queries_df,
    )


def test_gene_disease_associations_check_schema(wrong_schema_df):
    with pytest.raises(ValidationError):
        GeneDiseaseAssociations(df=wrong_schema_df)


def test_disease_hierarchy_check_schema(wrong_schema_df):
    with pytest.raises(ValidationError):
        DiseaseHierarchy(df=wrong_schema_df)
