"""Connector class for target."""
from __future__ import annotations

from typing import cast

import sqlalchemy
from singer_sdk import SQLConnector
from singer_sdk import typing as th
from sqlalchemy.dialects.postgresql import ARRAY, BIGINT, JSONB
from sqlalchemy.engine import URL
from sqlalchemy.types import TIMESTAMP


class PostgresConnector(SQLConnector):
    """Sets up SQL Alchemy, and other Postgres related stuff."""

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = True  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    def create_sqlalchemy_connection(self) -> sqlalchemy.engine.Connection:
        """Return a new SQLAlchemy connection using the provided config.

        Read more details about why this doesn't work on postgres here.
        DML/DDL doesn't work with this being on according to these docs

        https://docs.sqlalchemy.org/en/14/core/connections.html#using-server-side-cursors-a-k-a-stream-results

        Returns:
            A newly created SQLAlchemy engine object.
        """
        return self.create_sqlalchemy_engine().connect()

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
            )
            return cast(str, sqlalchemy_url)

    def truncate_table(self, name):
        """Clear table data."""
        self.connection.execute(f"TRUNCATE TABLE {name}")

    def drop_table(self, name):
        """Drop table data."""
        self.connection.execute(f"DROP TABLE {name}")

    def create_temp_table_from_table(self, from_table_name, temp_table_name):
        """Temp table from another table."""
        ddl = sqlalchemy.DDL(
            "CREATE TEMP TABLE %(temp_table_name)s AS "
            "SELECT * FROM %(from_table_name)s LIMIT 0",
            {"temp_table_name": temp_table_name, "from_table_name": from_table_name},
        )
        self.connection.execute(ddl)

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
        full_table_name: str,
        schema: dict,
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,
    ) -> None:
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
        if as_temp_table:
            raise NotImplementedError("Temporary tables are not supported.")

        _ = partition_keys  # Not supported in generic implementation.

        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        meta = sqlalchemy.MetaData(schema=schema_name)
        columns: list[sqlalchemy.Column] = []
        primary_keys = primary_keys or []
        try:
            properties: dict = schema["properties"]
        except KeyError:
            raise RuntimeError(
                f"Schema for '{full_table_name}' does not define properties: {schema}"
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

        _ = sqlalchemy.Table(table_name, meta, *columns)
        meta.create_all(self._engine)

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
