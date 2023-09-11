"""Count Associations from inputs."""
from functools import partial

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as fx

from rs_take_home.data_models import (
    BaseModelArbitrary,
    DiseaseHierarchy,
    GeneDiseaseAssociations,
    Queries,
)

_disease_id_parent_col = 'disease_id_parent'
_disease_id_child_col = 'disease_id_child'
_disease_id_col = 'disease_id'
_gene_id_col = 'gene_id'
_query_col = 'Query'
_related_disease_col = 'related_disease'


class AssociationCounter(BaseModelArbitrary):
    """Gene and disease association counter."""

    disease_hierarchy: DiseaseHierarchy
    gene_disease_associations: GeneDiseaseAssociations
    queries: Queries
    spark: SparkSession

    @classmethod
    def from_config(cls, config: dict, queries: Queries, spark: SparkSession):
        """Instantiate from filepath config.

        Args:
            config (dict): dict with filepaths for inputs
            queries (Queries): contains information on queries
            spark (SparkSession): spark session allowing custom spark configs

        Returns:
            AssociationCounter
        """
        disease_hierarchy = DiseaseHierarchy.from_filepath(
            config['disease_hierarchy'], spark,
        )
        gene_disease_associations = GeneDiseaseAssociations.from_filepath(
            config['gene_disease_associations'], spark,
        )
        return cls(
            disease_hierarchy=disease_hierarchy,
            gene_disease_associations=gene_disease_associations,
            queries=queries,
            spark=spark,
        )

    def count_associations(self) -> DataFrame:
        """Count associations based on queries.

        Returns:
            DataFrame: dataframe with queries and counts
        """
        queries_gene_ids_added = self._add_related_diseases_and_gene_ids()
        return (
            queries_gene_ids_added
            .groupBy(_query_col)
            .agg(
                fx.countDistinct(_gene_id_col).alias('Result'),
            )
            .orderBy(
                fx.col('Result').desc(),
                fx.col(_query_col).asc(),
            )
        )

    def _add_related_diseases_and_gene_ids(self) -> DataFrame:
        related_diseases = self._add_parent_child_diseases()
        gene_disease_associations_df = (
            self.gene_disease_associations.df
            .withColumnRenamed(_disease_id_col, _related_disease_col)
        )
        return (
            related_diseases
            .drop(_gene_id_col)
            .join(
                gene_disease_associations_df,
                on=_related_disease_col,
            )
        )

    def _add_parent_child_diseases(self) -> DataFrame:
        child_diseases_added = (
            self.queries.df
            .transform(
                partial(
                    self._add_related_ids,
                    join_column=_disease_id_parent_col,
                    id_to_add_column=_disease_id_child_col,
                ),
            )
        )
        parent_diseases_added = (
            self.queries.df
            .transform(
                partial(
                    self._add_related_ids,
                    join_column=_disease_id_child_col,
                    id_to_add_column=_disease_id_parent_col,
                ),
            )
        )

        return (
            self.queries.df
            .withColumn(_related_disease_col, fx.col(_disease_id_col))
            .unionByName(child_diseases_added)
            .unionByName(parent_diseases_added)
        )

    def _add_related_ids(
        self,
        df: DataFrame,
        *,
        join_column: str,
        id_to_add_column: str,
    ) -> DataFrame:
        return (
            fx.broadcast(df)
            .join(
                self.disease_hierarchy.df.withColumnRenamed(
                    join_column,
                    _disease_id_col,
                ),
                on=_disease_id_col,
            )
            .withColumnRenamed(id_to_add_column, _related_disease_col)
        )
