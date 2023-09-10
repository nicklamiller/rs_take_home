from pyspark_test import assert_pyspark_df_equal

from rs_take_home.run import get_association_counts


def test_get_association_counts(query_counts_df):
    actual_query_counts_df = get_association_counts()
    assert_pyspark_df_equal(actual_query_counts_df, query_counts_df)
