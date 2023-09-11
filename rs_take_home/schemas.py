"""Schemas for needed inputs."""
disease_hierarchy_schema = [
    ('disease_id_child', 'string'),
    ('disease_id_parent', 'string'),
]


gene_disease_associations_schema = [
    ('gene_id', 'string'),
    ('disease_id', 'string'),
]
