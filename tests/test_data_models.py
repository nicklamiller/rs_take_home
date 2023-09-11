def test_get_child_and_parent_diseases(
    disease_hierarchy,
    mondo_0019557_diseases,
):
    child_parent_diseases = (
        disease_hierarchy
        .get_child_and_parent_diseases('MONDO:0019557')
    )
    is_child_parent_disease = [
        _ in mondo_0019557_diseases for _ in child_parent_diseases
    ]
    assert all(is_child_parent_disease)


def test_count_associations(
    gene_disease_queries,
    disease_hierarchy,
    gene_disease_associations,
):
    child_parent_diseases = (
        disease_hierarchy
        .get_child_and_parent_diseases(gene_disease_queries[0][1])
    )
    disease_count = (
        gene_disease_associations
        .count_associations(
            list_disease_ids=child_parent_diseases,
        )
    )
    assert disease_count == 4
