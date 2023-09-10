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
