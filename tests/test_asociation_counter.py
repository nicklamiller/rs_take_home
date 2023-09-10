from pyspark_test import assert_pyspark_df_equal


def test_count_all_queries(
    gene_disease_queries,
    association_counter,
    query_counts_df,
):
    actual_query_counts_df = (
        association_counter.count_all_queries(gene_disease_queries)
    )
    assert_pyspark_df_equal(actual_query_counts_df, query_counts_df)
