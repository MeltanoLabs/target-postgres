"""Handles Postgres interactions."""

from __future__ import annotations

import atexit
import io
import itertools
import math
import signal
import sys
import typing as t
from contextlib import contextmanager
from functools import cached_property
from os import chmod, path
from typing import cast

import paramiko
import simplejson
import sqlalchemy as sa
from singer_sdk import SQLConnector
from singer_sdk.connectors.sql import JSONSchemaToSQL
from sqlalchemy.dialects.postgresql import (
    ARRAY,
    BIGINT,
    BYTEA,
    JSONB,
    SMALLINT,
    UUID,
)
from sqlalchemy.engine import URL
from sqlalchemy.engine.url import make_url
from sqlalchemy.types import (
    BOOLEAN,
    DATE,
    DATETIME,
    DECIMAL,
    INTEGER,
    TEXT,
    TIME,
    TIMESTAMP,
    TypeDecorator,
)
from sshtunnel import SSHTunnelForwarder

if t.TYPE_CHECKING:
    from singer_sdk.connectors.sql import FullyQualifiedName


class JSONSchemaToPostgres(JSONSchemaToSQL):
    """Convert JSON Schema types to Postgres types."""

    def __init__(
        self,
        *args: t.Any,
        content_encoding: bool = True,
        **kwargs: t.Any,
    ) -> None:
        """Initialize the JSONSchemaToPostgres instance."""
        super().__init__(*args, **kwargs)
        self.content_encoding = content_encoding

    def handle_raw_string(self, schema):
        """Handle a raw string type."""
        if self.content_encoding and schema.get("contentEncoding") == "base16":
            return HexByteString()

        return TEXT()


class PostgresConnector(SQLConnector):
    """Sets up SQL Alchemy, and other Postgres related stuff."""

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = True  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    #: Maximum length of a VARCHAR column.
    #: https://www.postgresql.org/docs/current/datatype-character.html
    max_varchar_length: int | None = 10_485_760

    def __init__(self, config: dict) -> None:
        """Initialize a connector to a Postgres database.

        Args:
            config: Configuration for the connector.
        """
        url: URL = make_url(self.get_sqlalchemy_url(config=config))
        ssh_config = config.get("ssh_tunnel", {})
        self.ssh_tunnel: SSHTunnelForwarder

        if ssh_config.get("enable", False):
            # Return a new URL with SSH tunnel parameters
            self.ssh_tunnel = SSHTunnelForwarder(
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

    @cached_property
    def interpret_content_encoding(self) -> bool:
        """Whether to interpret schema contentEncoding to set the column type.

        It is an opt-in feature because it might result in data loss if the
        actual data does not match the schema's advertised encoding.

        Returns:
            True if the feature is enabled, False otherwise.
        """
        return self.config.get("interpret_content_encoding", False)

    @cached_property
    def sanitize_null_text_characters(self) -> bool:
        """Whether to sanitize null text characters.

        Returns:
            True if the feature is enabled, False otherwise.
        """
        return self.config.get("sanitize_null_text_characters", False)

    def prepare_table(  # type: ignore[override]  # noqa: PLR0913
        self,
        full_table_name: str | FullyQualifiedName,
        schema: dict,
        primary_keys: t.Sequence[str],
        connection: sa.engine.Connection,
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,
    ) -> sa.Table:
        """Adapt target table to provided schema if possible.

        Args:
            full_table_name: the target table name.
            schema: the JSON Schema for the table.
            primary_keys: list of key properties.
            connection: the database connection.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.

        Returns:
            The table object.
        """
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        meta = sa.MetaData(schema=schema_name)
        table: sa.Table
        if not self.table_exists(full_table_name=full_table_name):
            return self.create_empty_table(
                table_name=table_name,
                meta=meta,
                schema=schema,
                primary_keys=primary_keys,
                partition_keys=partition_keys,
                as_temp_table=as_temp_table,
                connection=connection,
            )
        meta.reflect(connection, only=[table_name])
        table = meta.tables[
            full_table_name
        ]  # So we don't mess up the casing of the Table reference

        columns = self.get_table_columns(
            schema_name=cast("str", schema_name),
            table_name=table_name,
            connection=connection,
        )

        for property_name, property_def in schema["properties"].items():
            column_object = None
            if property_name in columns:
                column_object = columns[property_name]

            self.prepare_column(
                full_table_name=table.fullname,
                column_name=property_name,
                sql_type=self.to_sql_type(property_def),
                connection=connection,
                column_object=column_object,
            )

        return meta.tables[full_table_name]

    def copy_table_structure(
        self,
        full_table_name: str | FullyQualifiedName,
        from_table: sa.Table,
        connection: sa.engine.Connection,
        as_temp_table: bool = False,
    ) -> sa.Table:
        """Copy table structure.

        Args:
            full_table_name: the target table name potentially including schema
            from_table: the  source table
            connection: the database connection.
            as_temp_table: True to create a temp table.

        Returns:
            The new table object.
        """
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        meta = sa.MetaData(schema=schema_name)
        new_table: sa.Table
        if self.table_exists(full_table_name=full_table_name):
            raise RuntimeError("Table already exists")

        columns = [column._copy() for column in from_table.columns]
        if as_temp_table:
            new_table = sa.Table(table_name, meta, *columns, prefixes=["TEMPORARY"])
            new_table.create(bind=connection)
            return new_table
        new_table = sa.Table(table_name, meta, *columns)
        new_table.create(bind=connection)
        return new_table

    @contextmanager
    def _connect(self) -> t.Iterator[sa.engine.Connection]:
        with self._engine.connect().execution_options() as conn:
            yield conn

    def drop_table(self, table: sa.Table, connection: sa.engine.Connection):
        """Drop table data."""
        table.drop(bind=connection)

    def clone_table(
        self, new_table_name, table, metadata, connection, temp_table
    ) -> sa.Table:
        """Clone a table."""
        new_columns = [sa.Column(column.name, column.type) for column in table.columns]
        if temp_table is True:
            new_table = sa.Table(
                new_table_name, metadata, *new_columns, prefixes=["TEMPORARY"]
            )
        else:
            new_table = sa.Table(new_table_name, metadata, *new_columns)
        new_table.create(bind=connection)
        return new_table

    def _handle_array_type(self, jsonschema: dict) -> ARRAY | JSONB:
        """Handle array type."""
        items = jsonschema.get("items")
        # Case 1: items is a string
        if isinstance(items, str):
            return ARRAY(self.to_sql_type({"type": items}))

        # Case 2: items are more complex
        if isinstance(items, dict):
            # Case 2.1: items are variants
            if "type" not in items:
                return ARRAY(JSONB())

            items_type = items["type"]

            # Case 2.2: items are a single type
            if isinstance(items_type, str):
                return ARRAY(self.to_sql_type({"type": items_type}))

            # Case 2.3: items are a list of types
            if isinstance(items_type, list):
                return ARRAY(self.to_sql_type({"type": items_type}))

        # Case 3: tuples
        return ARRAY(JSONB()) if isinstance(items, list) else JSONB()

    def _handle_integer_type(self, jsonschema: dict) -> SMALLINT | INTEGER | BIGINT:
        """Handle integer type."""
        minimum = jsonschema.get("minimum", -math.inf)
        maximum = jsonschema.get("maximum", math.inf)
        if minimum >= -(2**15) and maximum < 2**15:
            return SMALLINT()
        if minimum >= -(2**31) and maximum < 2**31:
            return INTEGER()

        return BIGINT()

    @cached_property
    def jsonschema_to_sql(self) -> JSONSchemaToSQL:
        """Return a JSONSchemaToSQL instance with custom type handling."""
        to_sql = JSONSchemaToPostgres(
            content_encoding=self.interpret_content_encoding,
            max_varchar_length=self.max_varchar_length,
        )
        to_sql.fallback_type = TEXT
        to_sql.register_type_handler("integer", self._handle_integer_type)
        to_sql.register_type_handler("object", JSONB)
        to_sql.register_type_handler("array", self._handle_array_type)
        to_sql.register_format_handler("date-time", TIMESTAMP)
        to_sql.register_format_handler("uuid", UUID)
        to_sql.register_format_handler("email", TEXT)
        to_sql.register_format_handler("uri", TEXT)
        to_sql.register_format_handler("hostname", TEXT)
        to_sql.register_format_handler("ipv4", TEXT)
        to_sql.register_format_handler("ipv6", TEXT)
        to_sql.register_sql_datatype_handler("smallint", SMALLINT)
        to_sql.register_sql_datatype_handler("integer", INTEGER)
        to_sql.register_sql_datatype_handler("bigint", BIGINT)
        return to_sql

    def to_sql_type(self, jsonschema_type: dict) -> sa.types.TypeEngine:
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
        json_type_array = []

        if jsonschema_type.get("type", False):
            if isinstance(jsonschema_type["type"], str):
                json_type_array.append(jsonschema_type)
            elif isinstance(jsonschema_type["type"], list):
                for entry in jsonschema_type["type"]:
                    json_type_dict = {"type": entry}
                    if jsonschema_type.get("format", False):
                        json_type_dict["format"] = jsonschema_type["format"]
                    if encoding := jsonschema_type.get("contentEncoding", False):
                        json_type_dict["contentEncoding"] = encoding
                    # Figure out array type, but only if there's a single type
                    # (no array union types)
                    if (
                        "items" in jsonschema_type
                        and "type" in jsonschema_type["items"]
                        and isinstance(jsonschema_type["items"]["type"], str)
                    ):
                        json_type_dict["items"] = jsonschema_type["items"]["type"]
                    json_type_array.append(json_type_dict)
            else:
                msg = "Invalid format for jsonschema type: not str or list."
                raise RuntimeError(msg)
        elif jsonschema_type.get("anyOf", False):
            json_type_array.extend(iter(jsonschema_type["anyOf"]))
        else:
            msg = (
                "Neither type nor anyOf are present. Unable to determine type. "
                "Defaulting to string."
            )
            return NOTYPE()
        sql_type_array = []
        for json_type in json_type_array:
            picked_type = self.pick_individual_type(jsonschema_type=json_type)
            if picked_type is not None:
                sql_type_array.append(picked_type)

        return PostgresConnector.pick_best_sql_type(sql_type_array=sql_type_array)

    def pick_individual_type(self, jsonschema_type: dict):
        """Select the correct sql type assuming jsonschema_type has only a single type.

        Args:
            jsonschema_type: A jsonschema_type array containing only a single type.

        Returns:
            An instance of the appropriate SQL type class based on jsonschema_type.
        """
        if "null" in jsonschema_type["type"]:
            return None

        return self.jsonschema_to_sql.to_sql_type(jsonschema_type)

    @staticmethod
    def pick_best_sql_type(sql_type_array: list):
        """Select the best SQL type from an array of instances of SQL type classes.

        Args:
            sql_type_array: The array of instances of SQL type classes.

        Returns:
            An instance of the best SQL type class based on defined precedence order.
        """
        precedence_order = [
            HexByteString,
            ARRAY,
            JSONB,
            UUID,
            TEXT,
            TIMESTAMP,
            DATETIME,
            DATE,
            TIME,
            DECIMAL,
            BIGINT,
            INTEGER,
            SMALLINT,
            BOOLEAN,
            NOTYPE,
        ]

        for sql_type, obj in itertools.product(precedence_order, sql_type_array):
            if isinstance(obj, sql_type):
                return obj
        return TEXT()

    def create_empty_table(  # type: ignore[override]  # noqa: PLR0913
        self,
        table_name: str,
        meta: sa.MetaData,
        schema: dict,
        connection: sa.engine.Connection,
        primary_keys: t.Sequence[str] | None = None,
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,
    ) -> sa.Table:
        """Create an empty target table.

        Args:
            table_name: the target table name.
            meta: the SQLAlchemy metadata object.
            schema: the JSON schema for the new table.
            connection: the database connection.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.

        Returns:
            The new table object.

        Raises:
            NotImplementedError: if temp tables are unsupported and as_temp_table=True.
            RuntimeError: if a variant schema is passed with no properties defined.
        """
        columns: list[sa.Column] = []
        primary_keys = primary_keys or []
        try:
            properties: dict = schema["properties"]
        except KeyError:
            raise RuntimeError(
                f"Schema for table_name: '{table_name}'"
                f"does not define properties: {schema}"
            ) from None

        for property_name, property_jsonschema in properties.items():
            is_primary_key = property_name in primary_keys
            columns.append(
                sa.Column(
                    property_name,
                    self.to_sql_type(property_jsonschema),
                    primary_key=is_primary_key,
                    autoincrement=False,  # See: https://github.com/MeltanoLabs/target-postgres/issues/193 # noqa: E501
                )
            )
        if as_temp_table:
            new_table = sa.Table(table_name, meta, *columns, prefixes=["TEMPORARY"])
            new_table.create(bind=connection)
            return new_table

        new_table = sa.Table(table_name, meta, *columns)
        new_table.create(bind=connection)
        return new_table

    def prepare_column(
        self,
        full_table_name: str | FullyQualifiedName,
        column_name: str,
        sql_type: sa.types.TypeEngine,
        connection: sa.engine.Connection | None = None,
        column_object: sa.Column | None = None,
    ) -> None:
        """Adapt target table to provided schema if possible.

        Args:
            full_table_name: the fully qualified table name.
            column_name: the target column name.
            sql_type: the SQLAlchemy type.
            connection: a database connection. optional.
            column_object: a SQLAlchemy column. optional.
        """
        if connection is None:
            super().prepare_column(full_table_name, column_name, sql_type)
            return

        _, schema_name, table_name = self.parse_full_table_name(full_table_name)

        column_exists = column_object is not None or self.column_exists(
            full_table_name, column_name, connection=connection
        )

        if not column_exists:
            self._create_empty_column(
                # We should migrate every function to use sa.Table
                # instead of having to know what the function wants
                table_name=table_name,
                column_name=column_name,
                sql_type=sql_type,
                schema_name=cast("str", schema_name),
                connection=connection,
            )
            return

        self._adapt_column_type(
            schema_name=cast("str", schema_name),
            table_name=table_name,
            column_name=column_name,
            sql_type=sql_type,
            connection=connection,
            column_object=column_object,
        )

    def _create_empty_column(  # type: ignore[override]
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        sql_type: sa.types.TypeEngine,
        connection: sa.engine.Connection,
    ) -> None:
        """Create a new column.

        Args:
            schema_name: The schema name.
            table_name: The table name.
            column_name: The name of the new column.
            sql_type: SQLAlchemy type engine to be used in creating the new column.
            connection: The database connection.

        Raises:
            NotImplementedError: if adding columns is not supported.
        """
        if not self.allow_column_add:
            msg = "Adding columns is not supported."
            raise NotImplementedError(msg)

        column_add_ddl = self.get_column_add_ddl(
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
            column_type=sql_type,
        )
        connection.execute(column_add_ddl)

    def get_column_add_ddl(  # type: ignore[override]
        self,
        table_name: str,
        schema_name: str,
        column_name: str,
        column_type: sa.types.TypeEngine,
    ) -> sa.DDL:
        """Get the create column DDL statement.

        Args:
            table_name: Fully qualified table name of column to alter.
            schema_name: Schema name.
            column_name: Column name to create.
            column_type: New column sqlalchemy type.

        Returns:
            A sqlalchemy DDL instance.
        """
        column = sa.Column(column_name, column_type)

        return sa.DDL(
            (
                'ALTER TABLE "%(schema_name)s"."%(table_name)s"'
                "ADD COLUMN %(column_name)s %(column_type)s"
            ),
            {
                "schema_name": schema_name,
                "table_name": table_name,
                "column_name": column.compile(dialect=self._engine.dialect),
                "column_type": column.type.compile(dialect=self._engine.dialect),
            },
        )

    def _adapt_column_type(  # type: ignore[override]  # noqa: PLR0913
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        sql_type: sa.types.TypeEngine,
        connection: sa.engine.Connection,
        column_object: sa.Column | None,
    ) -> None:
        """Adapt table column type to support the new JSON schema type.

        Args:
            schema_name: The schema name.
            table_name: The table name.
            column_name: The target column name.
            sql_type: The new SQLAlchemy type.
            connection: The database connection.
            column_object: The existing column object.

        Raises:
            NotImplementedError: if altering columns is not supported.
        """
        current_type: sa.types.TypeEngine
        if column_object is not None:
            current_type = t.cast("sa.types.TypeEngine", column_object.type)
        else:
            current_type = self._get_column_type(
                schema_name=schema_name,
                table_name=table_name,
                column_name=column_name,
                connection=connection,
            )

        # remove collation if present and save it
        current_type_collation = self.remove_collation(current_type)

        # Check if the existing column type and the sql type are the same
        if str(sql_type) == str(current_type):
            # The current column and sql type are the same
            # Nothing to do
            return

        # Not the same type, generic type or compatible types
        # calling merge_sql_types for assistance
        compatible_sql_type = self.merge_sql_types([current_type, sql_type])

        if str(compatible_sql_type) == str(current_type):
            # Nothing to do
            return

        # Put the collation level back before altering the column
        if current_type_collation:
            self.update_collation(compatible_sql_type, current_type_collation)

        if not self.allow_column_alter:
            msg = (
                "Altering columns is not supported. Could not convert column "
                f"'{schema_name}.{table_name}.{column_name}' from '{current_type}' to "
                f"'{compatible_sql_type}'."
            )
            raise NotImplementedError(msg)

        alter_column_ddl = self.get_column_alter_ddl(
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
            column_type=compatible_sql_type,
        )
        connection.execute(alter_column_ddl)

    def get_column_alter_ddl(  # type: ignore[override]
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        column_type: sa.types.TypeEngine,
    ) -> sa.DDL:
        """Get the alter column DDL statement.

        Override this if your database uses a different syntax for altering columns.

        Args:
            schema_name: Schema name.
            table_name: Fully qualified table name of column to alter.
            column_name: Column name to alter.
            column_type: New column type string.

        Returns:
            A sqlalchemy DDL instance.
        """
        column = sa.Column(column_name, column_type)
        return sa.DDL(
            (
                'ALTER TABLE "%(schema_name)s"."%(table_name)s"'
                "ALTER COLUMN %(column_name)s %(column_type)s"
            ),
            {
                "schema_name": schema_name,
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
            return cast("str", config["sqlalchemy_url"])

        sqlalchemy_url = URL.create(
            drivername=config["dialect+driver"],
            username=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            database=config["database"],
            query=self.get_sqlalchemy_query(config),
        )
        return cast("str", sqlalchemy_url)

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
            query["sslmode"] = ssl_mode
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
                key = key_class.from_private_key(io.StringIO(key_data))
            except paramiko.SSHException:  # noqa: PERF203
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
        sys.exit(1)  # Calling this to be sure atexit is called, so clean_up gets called

    def _get_column_type(  # type: ignore[override]
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        connection: sa.engine.Connection,
    ) -> sa.types.TypeEngine:
        """Get the SQL type of the declared column.

        Args:
            schema_name: The schema name.
            table_name: The table name.
            column_name: The name of the column.
            connection: The database connection.

        Returns:
            The type of the column.

        Raises:
            KeyError: If the provided column name does not exist.
        """
        try:
            column = self.get_table_columns(
                schema_name=schema_name,
                table_name=table_name,
                connection=connection,
            )[column_name]
        except KeyError as ex:
            msg = (
                f"Column `{column_name}` does not exist in table"
                "`{schema_name}.{table_name}`."
            )
            raise KeyError(msg) from ex

        return t.cast("sa.types.TypeEngine", column.type)

    def get_table_columns(  # type: ignore[override]
        self,
        schema_name: str,
        table_name: str,
        connection: sa.engine.Connection,
        column_names: list[str] | None = None,
    ) -> dict[str, sa.Column]:
        """Return a list of table columns.

        Overrode to support schema_name

        Args:
            schema_name: schema name.
            table_name: table name to get columns for.
            connection: database connection.
            column_names: A list of column names to filter to.

        Returns:
            An ordered list of column objects.
        """
        inspector = sa.inspect(connection)
        columns = inspector.get_columns(table_name, schema_name)

        return {
            col_meta["name"]: sa.Column(
                col_meta["name"],
                col_meta["type"],
                nullable=col_meta.get("nullable", False),
            )
            for col_meta in columns
            if not column_names
            or col_meta["name"].casefold() in {col.casefold() for col in column_names}
        }

    def column_exists(  # type: ignore[override]
        self,
        full_table_name: str | FullyQualifiedName,
        column_name: str,
        connection: sa.engine.Connection,
    ) -> bool:
        """Determine if the target column already exists.

        Args:
            full_table_name: the target table name.
            column_name: the target column name.
            connection: the database connection.

        Returns:
            True if table exists, False if not.
        """
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        assert schema_name is not None
        assert table_name is not None
        return column_name in self.get_table_columns(
            schema_name=schema_name, table_name=table_name, connection=connection
        )


class NOTYPE(TypeDecorator):
    """Type to use when none is provided in the schema."""

    impl = TEXT
    cache_ok = True

    def process_bind_param(self, value, dialect):
        """Return value as is unless it is dict or list.

        Used internally by SQL Alchemy. Should not be used directly.
        """
        if value is not None and isinstance(value, (dict, list)):
            value = simplejson.dumps(value, use_decimal=True)
        return value

    @property
    def python_type(self):
        """Return the Python type for this column."""
        return object

    def as_generic(self, *args: t.Any, **kwargs: t.Any):
        """Return the generic type for this column."""
        return TEXT()


class HexByteString(TypeDecorator):
    """Convert Python string representing Hex data to bytes and vice versa.

    This is used to store binary data in more efficient format in the database.
    The string is encoded using the base16 encoding, as defined in RFC 4648
    https://json-schema.org/draft/2020-12/draft-bhutton-json-schema-validation-00#rfc.section.8.3
    For convenience, data prefixed with `0x` or containing an odd number of characters
    is supported although it's not part of the standard.
    """

    impl = BYTEA

    def process_bind_param(self, value, dialect):
        """Convert hex string to bytes."""
        if value is None:
            return None

        if isinstance(value, str):
            if value.startswith("\\x") or value.startswith("0x"):
                value = value[2:]

            if len(value) % 2:
                value = f"0{value}"

            try:
                value = bytes.fromhex(value)
            except ValueError as ex:
                raise ValueError(f"Invalid hexadecimal string: {value}") from ex

        if not isinstance(value, (bytearray, memoryview, bytes)):
            raise TypeError(
                "HexByteString columns support only bytes or hex string values. "
                f"{type(value)} is not supported"
            )

        return value
