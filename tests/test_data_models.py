import pytest

from src.data_models import DiseaseHierarchy


@pytest.fixture
def efo_0005809_diseases():
    return [
        'EFO:0000540',
        'MONDO:0004670',
        'EFO:1002003',
        'MONDO:0000603',
    ]


@pytest.fixture
def disease_hierarchy_df(spark_session):
    return (
        spark_session
        .read
        .csv(
            'data/example_disease_hierarchy.csv',
            header=True,
            inferSchema=True,
        )
    )


@pytest.fixture
def disease_hierarchy(disease_hierarchy_df):
    return DiseaseHierarchy(df=disease_hierarchy_df)


def test_get_children_and_parent_diseases(
    disease_hierarchy,
    efo_0005809_diseases,
):
    children_parent_diseases = (
        disease_hierarchy
        .get_children_and_parent_diseases('EFO:0005809')
    )
    is_child_parent_disease = [
        _ in efo_0005809_diseases for _ in children_parent_diseases
    ]
    assert all(is_child_parent_disease)
