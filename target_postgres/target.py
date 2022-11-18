"""Postgres target class."""

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_postgres.sinks import PostgresSink


class TargetPostgres(Target):
    """Target for Postgres."""

    name = "target-postgres"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "dialect",
            th.StringType,
            description="The Dialect of SQLAlchamey"
        ),
        th.Property(
            "driver_type",
            th.StringType,
            description="The Python Driver you will be using to connect to the SQL server"
        ),
        th.Property(
            "host",
            th.StringType,
            description="The FQDN of the Host serving out the SQL Instance"
        ),
        th.Property(
            "port",
            th.IntegerType,
            description="The port on which SQL awaiting connection"
        ),
        th.Property(
            "user",
            th.StringType,
            description="The User Account who has been granted access to the SQL Server"
        ),
        th.Property(
            "password",
            th.StringType,
            description="The Password for the User account"
        ),
        th.Property(
            "database",
            th.StringType,
            description="The Default database for this connection"
        )
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
