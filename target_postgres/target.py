"""Postgres target class."""
from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target
from pathlib import Path, PurePath

from target_postgres.sinks import PostgresSink
import urllib.parse


class TargetPostgres(Target):
    """Target for Postgres."""

    def __init__(
        self,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        """Initialize the target.

        Args:
            config: Target configuration. Can be a dictionary, a single path to a
                configuration file, or a list of paths to multiple configuration
                files.
            parse_env_config: Whether to look for configuration values in environment
                variables.
            validate_config: True to require validation of config settings.
        """
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )
        #There's a few ways to do this in JSON Schema but it is pretty schema draft dependent.
        #See https://stackoverflow.com/questions/38717933/jsonschema-attribute-conditionally-required
        assert (self.config.get("sqlalchemy_url") is not None) or (
                self.config.get("host") is not None and 
                self.config.get("port") is not None and 
                self.config.get("user") is not None and 
                self.config.get("password") is not None and
                self.config.get("dialect+driver") is not None), "Need either the sqlalchemy_url to be set or host, port, user, password, and dialect+driver to be set"

    name = "target-postgres"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "host",
            th.StringType,
            description="Hostname for postgres instance. Note if sqlalchemy_url is set this will be ignored."
        ),
        th.Property(
            "port",
            th.IntegerType,
            default=5432,
            description="The port on which postgres is awaiting connection. Note if sqlalchemy_url is set this will be ignored."
        ),
        th.Property(
            "user",
            th.StringType,
            description="User name used to authenticate. Note if sqlalchemy_url is set this will be ignored."
        ),
        th.Property(
            "password",
            th.StringType,
            description="Password used to authenticate. Note if sqlalchemy_url is set this will be ignored."
        ),
        th.Property(
            "database",
            th.StringType,
            description="Database name. Note if sqlalchemy_url is set this will be ignored."
        ),
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            description="SQLAlchemy connection string. This will override using host, user, password, port, dialect. Note that you must esacpe password special characters properly see https://docs.sqlalchemy.org/en/20/core/engines.html#escaping-special-characters-such-as-signs-in-passwords",
        ),
        th.Property(
            "dialect_+_driver",
            th.StringType,
            default="postgresql+psycopg2",
            description="Dialect+driver see https://docs.sqlalchemy.org/en/20/core/engines.html. Generally just leave this alone. Note if sqlalchemy_url is set this will be ignored."
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
