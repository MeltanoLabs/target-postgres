"""Postgres target class."""

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_postgres.sinks import PostgresSink


class TargetPostgres(Target):
    """Target for Postgres."""

    name = "target-postgres"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            required=True,
            description="SQLAlchemy connection string, example."
            + "`postgresql://postgres:postgres@localhost:5432/postgres`",
        ),
        th.Property(
            "default_target_schema",
            th.StringType,
            description="Postgres schema to send data to, example: tap-clickup",
        ),
    ).to_dict()
    default_sink_class = PostgresSink

    @property
    def max_parallelism(self) -> int:
        """Get max parallel sinks.

        The default is 8 if not overridden.

        Returns:
            Max number of sinks that can be drained in parallel.
        """
        # https://github.com/MeltanoLabs/target-postgres/issues/3
        return 1
