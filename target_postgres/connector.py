"""Connector class for target."""
from __future__ import annotations

import atexit
import io
import signal
from os import chmod, path
from typing import cast

import paramiko
import sqlalchemy
from singer_sdk import SQLConnector
from singer_sdk import typing as th
from sqlalchemy.dialects.postgresql import ARRAY, BIGINT, JSONB
from sqlalchemy.engine import URL
from sqlalchemy.engine.url import make_url
from sqlalchemy.types import TIMESTAMP
from sshtunnel import SSHTunnelForwarder


class PostgresConnector(SQLConnector):
    """Sets up SQL Alchemy, and other Postgres related stuff."""

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = True  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    def __init__(self, config: dict | None = None) -> None:
        url: URL = make_url(self.get_sqlalchemy_url(config=config))
        ssh_config = config.get("ssh_tunnel", {})
        self.ssh_tunnel = None

        if ssh_config.get("enable", False):
            # Return a new URL with SSH tunnel parameters
            self.ssh_tunnel: SSHTunnelForwarder = SSHTunnelForwarder(
                ssh_address_or_host=(ssh_config["host"], ssh_config["port"]),
                ssh_username=ssh_config["username"],
                ssh_private_key=self.guess_key_type(ssh_config["private_key"]),
                ssh_private_key_password=ssh_config.get("private_key_password"),
                remote_bind_address=(url.host, url.port),
            )
            self.ssh_tunnel.start()
            # On program exit clean up, want to also catch signals
            atexit.register(self.clean_up)
            signal.signal(signal.SIGTERM, self.catch_signal)
            # Probably overkill to catch SIGINT, but needed for SIGTERM
            signal.signal(signal.SIGINT, self.catch_signal)

            # Swap the URL to use the tunnel
            url = url.set(
                host=self.ssh_tunnel.local_bind_host,
                port=self.ssh_tunnel.local_bind_port,
            )

        super().__init__(
            config,
            sqlalchemy_url=url.render_as_string(hide_password=False),
        )

    def prepare_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: list[str],
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,
    ) -> sqlalchemy.Table:
        """Adapt target table to provided schema if possible.

        Args:
            full_table_name: the target table name.
            schema: the JSON Schema for the table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.
        """
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        meta = sqlalchemy.MetaData(bind=self._engine, schema=schema_name)
        if not self.table_exists(full_table_name=full_table_name):
            table = self.create_empty_table(
                table_name=table_name,
                meta=meta,
                schema=schema,
                primary_keys=primary_keys,
                partition_keys=partition_keys,
                as_temp_table=as_temp_table,
            )
            return table
        for property_name, property_def in schema["properties"].items():
            self.prepare_column(
                full_table_name, property_name, self.to_sql_type(property_def)
            )
        meta.reflect(only=[table_name])

        return meta.tables[full_table_name]

    def create_sqlalchemy_connection(self) -> sqlalchemy.engine.Connection:
        """Return a new SQLAlchemy connection using the provided config.

        Read more details about why this doesn't work on postgres here.
        DML/DDL doesn't work with this being on according to these docs

        https://docs.sqlalchemy.org/en/14/core/connections.html#using-server-side-cursors-a-k-a-stream-results

        Returns:
            A newly created SQLAlchemy engine object.
        """
        return self.create_sqlalchemy_engine().connect()

    def drop_table(self, table: sqlalchemy.Table):
        """Drop table data."""
        table.drop(bind=self.connection)

    def clone_table(
        self, new_table_name, table, metadata, connection, temp_table
    ) -> sqlalchemy.Table:
        """Clone a table."""
        new_columns = []
        for column in table.columns:
            new_columns.append(
                sqlalchemy.Column(
                    column.name,
                    column.type,
                )
            )
        if temp_table is True:
            new_table = sqlalchemy.Table(
                new_table_name, metadata, *new_columns, prefixes=["TEMPORARY"]
            )
        else:
            new_table = sqlalchemy.Table(new_table_name, metadata, *new_columns)
        new_table.create(bind=connection)
        return new_table

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Return a JSON Schema representation of the provided type.

        By default will call `typing.to_sql_type()`.

        Developers may override this method to accept additional input argument types,
        to support non-standard types, or to provide custom typing logic.
        If overriding this method, developers should call the default implementation
        from the base class for all unhandled cases.

        Args:
            jsonschema_type: The JSON Schema representation of the source type.

        Returns:
            The SQLAlchemy type representation of the data type.
        """
        if "integer" in jsonschema_type["type"]:
            return BIGINT()
        if "object" in jsonschema_type["type"]:
            return JSONB()
        if "array" in jsonschema_type["type"]:
            return ARRAY(JSONB())
        if jsonschema_type.get("format") == "date-time":
            return TIMESTAMP()
        return th.to_sql_type(jsonschema_type)

    def create_empty_table(
        self,
        table_name: str,
        meta: sqlalchemy.MetaData,
        schema: dict,
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,
    ) -> sqlalchemy.Table:
        """Create an empty target table.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.

        Raises:
            NotImplementedError: if temp tables are unsupported and as_temp_table=True.
            RuntimeError: if a variant schema is passed with no properties defined.
        """
        columns: list[sqlalchemy.Column] = []
        primary_keys = primary_keys or []
        try:
            properties: dict = schema["properties"]
        except KeyError:
            raise RuntimeError(
                f"Schema for table_name: '{table_name}'"
                f"does not define properties: {schema}"
            )

        for property_name, property_jsonschema in properties.items():
            is_primary_key = property_name in primary_keys
            columns.append(
                sqlalchemy.Column(
                    property_name,
                    self.to_sql_type(property_jsonschema),
                    primary_key=is_primary_key,
                )
            )
        if as_temp_table:
            new_table = sqlalchemy.Table(
                table_name, meta, *columns, prefixes=["TEMPORARY"]
            )
            new_table.create(bind=self.connection)
            return new_table

        new_table = sqlalchemy.Table(table_name, meta, *columns)
        new_table.create(bind=self.connection)
        return new_table

    def get_column_add_ddl(
        self,
        table_name: str,
        column_name: str,
        column_type: sqlalchemy.types.TypeEngine,
    ) -> sqlalchemy.DDL:
        """Get the create column DDL statement.

        Override this if your database uses a different syntax for creating columns.

        Args:
            table_name: Fully qualified table name of column to alter.
            column_name: Column name to create.
            column_type: New column sqlalchemy type.

        Returns:
            A sqlalchemy DDL instance.
        """
        column = sqlalchemy.Column(column_name, column_type)

        return sqlalchemy.DDL(
            "ALTER TABLE %(table_name)s ADD COLUMN %(column_name)s %(column_type)s",
            {
                "table_name": table_name,
                "column_name": column.compile(dialect=self._engine.dialect),
                "column_type": column.type.compile(dialect=self._engine.dialect),
            },
        )

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Generate a SQLAlchemy URL.

        Args:
            config: The configuration for the connector.
        """
        if config.get("sqlalchemy_url"):
            return cast(str, config["sqlalchemy_url"])

        else:
            sqlalchemy_url = URL.create(
                drivername=config["dialect+driver"],
                username=config["user"],
                password=config["password"],
                host=config["host"],
                port=config["port"],
                database=config["database"],
                query=self.get_sqlalchemy_query(config),
            )
            return cast(str, sqlalchemy_url)

    def get_sqlalchemy_query(self, config: dict) -> dict:
        """Get query values to be used for sqlalchemy URL creation.

        Args:
            config: The configuration for the connector.

        Returns:
            A dictionary with key-value pairs for the sqlalchemy query.
        """
        query = {}

        # ssl_enable is for verifying the server's identity to the client.
        if config["ssl_enable"]:
            ssl_mode = config["ssl_mode"]
            query.update({"sslmode": ssl_mode})
            query["sslrootcert"] = self.filepath_or_certificate(
                value=config["ssl_certificate_authority"],
                alternative_name=config["ssl_storage_directory"] + "/root.crt",
            )

        # ssl_client_certificate_enable is for verifying the client's identity to the
        # server.
        if config["ssl_client_certificate_enable"]:
            query["sslcert"] = self.filepath_or_certificate(
                value=config["ssl_client_certificate"],
                alternative_name=config["ssl_storage_directory"] + "/cert.crt",
            )
            query["sslkey"] = self.filepath_or_certificate(
                value=config["ssl_client_private_key"],
                alternative_name=config["ssl_storage_directory"] + "/pkey.key",
                restrict_permissions=True,
            )
        return query

    def filepath_or_certificate(
        self,
        value: str,
        alternative_name: str,
        restrict_permissions: bool = False,
    ) -> str:
        """Provide the appropriate key-value pair based on a filepath or raw value.

        For SSL configuration options, support is provided for either raw values in
        .env file or filepaths to a file containing a certificate. This function
        attempts to parse a value as a filepath, and if no file is found, assumes the
        value is a certificate and creates a file named `alternative_name` to store the
        file.

        Args:
            value: Either a filepath or a raw value to be written to a file.
            alternative_name: The filename to use in case `value` is not a filepath.
            restrict_permissions: Whether to restrict permissions on a newly created
                file. On UNIX systems, private keys cannot have public access.

        Returns:
            A dictionary with key-value pairs for the sqlalchemy query

        """
        if path.isfile(value):
            return value
        else:
            with open(alternative_name, "wb") as alternative_file:
                alternative_file.write(value.encode("utf-8"))
            if restrict_permissions:
                chmod(alternative_name, 0o600)
            return alternative_name

    def guess_key_type(self, key_data: str) -> paramiko.PKey:
        """Guess the type of the private key.

        We are duplicating some logic from the ssh_tunnel package here,
        we could try to use their function instead.

        Args:
            key_data: The private key data to guess the type of.

        Returns:
            The private key object.

        Raises:
            ValueError: If the key type could not be determined.
        """
        for key_class in (
            paramiko.RSAKey,
            paramiko.DSSKey,
            paramiko.ECDSAKey,
            paramiko.Ed25519Key,
        ):
            try:
                key = key_class.from_private_key(io.StringIO(key_data))  # type: ignore[attr-defined]  # noqa: E501
            except paramiko.SSHException:
                continue
            else:
                return key

        errmsg = "Could not determine the key type."
        raise ValueError(errmsg)

    def clean_up(self) -> None:
        """Stop the SSH Tunnel."""
        if self.ssh_tunnel is not None:
            self.ssh_tunnel.stop()

    def catch_signal(self, signum, frame) -> None:
        """Catch signals and exit cleanly.

        Args:
            signum: The signal number
            frame: The current stack frame
        """
        exit(1)  # Calling this to be sure atexit is called, so clean_up gets called
