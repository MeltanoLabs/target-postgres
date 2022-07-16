"""Postgres target class."""

from singer_sdk.target_base import Target
from singer_sdk import typing as th

from target_postgres.sinks import (
    PostgresSink,
)


class TargetPostgres(Target):
    """Sample target for Postgres."""

    name = "target-postgres"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            description="SQLAlchemy connection string",
        ),
    ).to_dict()
    default_sink_class = PostgresSink
