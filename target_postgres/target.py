"""Postgres target class."""
from __future__ import annotations

from pathlib import PurePath

import jsonschema
from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_postgres.sinks import PostgresSink


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
        # There's a few ways to do this in JSON Schema but it is schema draft dependent.
        # https://stackoverflow.com/questions/38717933/jsonschema-attribute-conditionally-required
        assert (self.config.get("sqlalchemy_url") is not None) or (
            self.config.get("host") is not None
            and self.config.get("port") is not None
            and self.config.get("user") is not None
            and self.config.get("password") is not None
            and self.config.get("dialect+driver") is not None
        ), (
            "Need either the sqlalchemy_url to be set or host, port, user,"
            + "password, and dialect+driver to be set"
        )

    name = "target-postgres"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "host",
            th.StringType,
            description=(
                "Hostname for postgres instance. "
                + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "port",
            th.IntegerType,
            default=5432,
            description=(
                "The port on which postgres is awaiting connection. "
                + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "user",
            th.StringType,
            description=(
                "User name used to authenticate. "
                + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "password",
            th.StringType,
            description=(
                "Password used to authenticate. "
                "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "database",
            th.StringType,
            description=(
                "Database name. "
                + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            description=(
                "SQLAlchemy connection string. "
                + "This will override using host, user, password, port, "
                + "dialect. Note that you must esacpe password special "
                + "characters properly see "
                + "https://docs.sqlalchemy.org/en/20/core/engines.html#escaping-special-characters-such-as-signs-in-passwords"  # noqa: E501
            ),
        ),
        th.Property(
            "dialect+driver",
            th.StringType,
            default="postgresql+psycopg2",
            description=(
                "Dialect+driver see "
                + "https://docs.sqlalchemy.org/en/20/core/engines.html. "
                + "Generally just leave this alone. "
                + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "default_target_schema",
            th.StringType,
            description="Postgres schema to send data to, example: tap-clickup",
        ),
        th.Property(
            "hard_delete",
            th.BooleanType,
            default=False,
            description=(
                "When activate version is sent from a tap this specefies "
                + "if we should delete the records that don't match, or mark "
                + "them with a date in the `_sdc_deleted_at` column."
            ),
        ),
        th.Property(
            "add_record_metadata",
            th.BooleanType,
            default=True,
            description=(
                "Note that this must be enabled for activate_version to work!"
                + "This adds _sdc_extracted_at, _sdc_batched_at, and more to every "
                + "table. See https://sdk.meltano.com/en/latest/implementation/record_metadata.html "  # noqa: E501
                + "for more information."
            ),
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

    def _process_record_message(self, message_dict: dict) -> None:
        """Process a RECORD message.

        Args:
            message_dict: TODO
        """
        stream_name = message_dict["stream"]
        if self.mapper.stream_maps.get(stream_name) is None:
            raise Exception(f"Schema message has not been sent for {stream_name}")
        try:
            super()._process_record_message(message_dict)
        except jsonschema.exceptions.ValidationError as e:
            self.logger.error(
                f"Exception is being thrown for stream_name: {stream_name}"
            )
            raise e

    def _process_schema_message(self, message_dict: dict) -> None:
        """Process a SCHEMA messages.

        Args:
            message_dict: The newly received schema message.
        """
        self._assert_line_requires(message_dict, requires={"stream", "schema"})
        self._assert_line_requires(message_dict["schema"], requires={"properties"})
        super()._process_schema_message(message_dict)
