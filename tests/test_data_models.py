import pytest
from pydantic import ValidationError
from pyspark.sql import Row
from pyspark_test import assert_pyspark_df_equal

from rs_take_home.data_models import GeneDiseaseAssociations
from rs_take_home.utils import spark_read_csv


@pytest.fixture
def wrong_columns_df(spark_session):
    return (
        spark_session.createDataFrame([
            Row(not_disease_id='made_up_id'),
            Row(not_disease_id='some_other_id'),
        ])
    )


@pytest.fixture
def wrong_dtypes_df(spark_session):
    return (
        spark_session.createDataFrame([
            Row(gene_id=1),
            Row(disease_id=True),
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


@pytest.mark.parametrize(
    'wrong_schema_df',
    [
        (pytest.lazy_fixture('wrong_columns_df')),
        (pytest.lazy_fixture('wrong_dtypes_df')),
    ],
)
def test_gene_disease_associations_check_schema(wrong_schema_df):
    with pytest.raises(ValidationError):
        GeneDiseaseAssociations(df=wrong_schema_df)
