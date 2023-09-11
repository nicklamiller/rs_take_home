from pyspark_test import assert_pyspark_df_equal

from rs_take_home.run import get_association_counts


def test_get_association_counts(
    filepaths_config,
    query_counts_df,
):
    actual_query_counts_df = get_association_counts(
        gene_disease_associations_path=filepaths_config['gene_disease_associations'],  # noqa: E501
        disease_hierarchy_path=filepaths_config['disease_hierarchy'],
    )
    assert_pyspark_df_equal(actual_query_counts_df, query_counts_df)
