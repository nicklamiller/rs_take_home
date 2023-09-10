import json

import pytest

from src.data_models import DiseaseHierarchy


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
