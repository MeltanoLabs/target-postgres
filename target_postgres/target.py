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
    ).to_dict()
    default_sink_class = PostgresSink
