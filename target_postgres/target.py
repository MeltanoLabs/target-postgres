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
            "filepath",
            th.StringType,
            description="The path to the target output file"
        ),
        th.Property(
            "file_naming_scheme",
            th.StringType,
            description="The scheme with which output files will be named"
        ),
    ).to_dict()
    default_sink_class = PostgresSink
