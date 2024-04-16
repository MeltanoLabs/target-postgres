"""Postgres target class."""

from __future__ import annotations

from pathlib import PurePath

from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget

from target_postgres.sinks import PostgresSink


class TargetPostgres(SQLTarget):
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
        self.max_parallelism = 1
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )
        # There's a few ways to do this in JSON Schema but it is schema draft dependent.
        # https://stackoverflow.com/questions/38717933/jsonschema-attribute-conditionally-required # noqa: E501
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

        # If sqlalchemy_url is not being used and ssl_enable is on, ssl_mode must have
        # one of six allowable values. If ssl_mode is verify-ca or verify-full, a
        # certificate authority must be provided to verify against.
        assert (
            (self.config.get("sqlalchemy_url") is not None)
            or (self.config.get("ssl_enable") is False)
            or (
                self.config.get("ssl_mode") in {"disable", "allow", "prefer", "require"}
            )
            or (
                self.config.get("ssl_mode") in {"verify-ca", "verify-full"}
                and self.config.get("ssl_certificate_authority") is not None
            )
        ), (
            "ssl_enable is true but invalid values are provided for ssl_mode and/or"
            + "ssl_certificate_authority."
        )

        # If sqlalchemy_url is not being used and ssl_client_certificate_enable is on,
        # the client must provide a certificate and associated private key.
        assert (
            (self.config.get("sqlalchemy_url") is not None)
            or (self.config.get("ssl_client_certificate_enable") is False)
            or (
                self.config.get("ssl_client_certificate") is not None
                and self.config.get("ssl_client_private_key") is not None
            )
        ), (
            "ssl_client_certificate_enable is true but one or both of"
            + " ssl_client_certificate or ssl_client_private_key are unset."
        )

        assert self.config.get("add_record_metadata") or not self.config.get(
            "activate_version"
        ), (
            "Activate version messages can't be processed unless add_record_metadata "
            "is set to true. To ignore Activate version messages instead, Set the "
            "`activate_version` configuration to False."
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
                + "dialect, and all ssl settings. Note that you must escape password "
                + "special characters properly. See "
                + "https://docs.sqlalchemy.org/en/20/core/engines.html#escaping-special-characters-such-as-signs-in-passwords"
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
            default="melty",
        ),
        th.Property(
            "activate_version",
            th.BooleanType,
            default=True,
            description=(
                "If set to false, the tap will ignore activate version messages. If "
                + "set to true, add_record_metadata must be set to true as well."
            ),
        ),
        th.Property(
            "hard_delete",
            th.BooleanType,
            default=False,
            description=(
                "When activate version is sent from a tap this specefies "
                + "if we should delete the records that don't match, or mark "
                + "them with a date in the `_sdc_deleted_at` column. This config "
                + "option is ignored if `activate_version` is set to false."
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
        th.Property(
            "interpret_content_encoding",
            th.BooleanType,
            default=False,
            description=(
                "If set to true, the target will interpret the content encoding of the "
                "schema to determine how to store the data. Using this option may "
                "result in a more efficient storage of the data but may also result "
                "in an error if the data is not encoded as expected."
            ),
        ),
        th.Property(
            "ssl_enable",
            th.BooleanType,
            default=False,
            description=(
                "Whether or not to use ssl to verify the server's identity. Use"
                + " ssl_certificate_authority and ssl_mode for further customization."
                + " To use a client certificate to authenticate yourself to the server,"
                + " use ssl_client_certificate_enable instead."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssl_client_certificate_enable",
            th.BooleanType,
            default=False,
            description=(
                "Whether or not to provide client-side certificates as a method of"
                + " authentication to the server. Use ssl_client_certificate and"
                + " ssl_client_private_key for further customization. To use SSL to"
                + " verify the server's identity, use ssl_enable instead."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssl_mode",
            th.StringType,
            default="verify-full",
            description=(
                "SSL Protection method, see [postgres documentation](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION)"
                + " for more information. Must be one of disable, allow, prefer,"
                + " require, verify-ca, or verify-full."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssl_certificate_authority",
            th.StringType,
            default="~/.postgresql/root.crl",
            description=(
                "The certificate authority that should be used to verify the server's"
                + " identity. Can be provided either as the certificate itself (in"
                + " .env) or as a filepath to the certificate."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssl_client_certificate",
            th.StringType,
            default="~/.postgresql/postgresql.crt",
            description=(
                "The certificate that should be used to verify your identity to the"
                + " server. Can be provided either as the certificate itself (in .env)"
                + " or as a filepath to the certificate."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssl_client_private_key",
            th.StringType,
            default="~/.postgresql/postgresql.key",
            description=(
                "The private key for the certificate you provided. Can be provided"
                + " either as the certificate itself (in .env) or as a filepath to the"
                + " certificate."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssl_storage_directory",
            th.StringType,
            default=".secrets",
            description=(
                "The folder in which to store SSL certificates provided as raw values."
                + " When a certificate/key is provided as a raw value instead of as a"
                + " filepath, it must be written to a file before it can be used. This"
                + " configuration option determines where that file is created."
            ),
        ),
        th.Property(
            "ssh_tunnel",
            th.ObjectType(
                th.Property(
                    "enable",
                    th.BooleanType,
                    required=False,
                    default=False,
                    description=(
                        "Enable an ssh tunnel (also known as bastion host), see the "
                        "other ssh_tunnel.* properties for more details"
                    ),
                ),
                th.Property(
                    "host",
                    th.StringType,
                    required=False,
                    description=(
                        "Host of the bastion host, this is the host "
                        "we'll connect to via ssh"
                    ),
                ),
                th.Property(
                    "username",
                    th.StringType,
                    required=False,
                    description="Username to connect to bastion host",
                ),
                th.Property(
                    "port",
                    th.IntegerType,
                    required=False,
                    default=22,
                    description="Port to connect to bastion host",
                ),
                th.Property(
                    "private_key",
                    th.StringType,
                    required=False,
                    secret=True,
                    description="Private Key for authentication to the bastion host",
                ),
                th.Property(
                    "private_key_password",
                    th.StringType,
                    required=False,
                    secret=True,
                    default=None,
                    description=(
                        "Private Key Password, leave None if no password is set"
                    ),
                ),
            ),
            required=False,
            description="SSH Tunnel Configuration, this is a json object",
        ),
    ).to_dict()
    default_sink_class = PostgresSink
