from __future__ import annotations

import typing as t
from contextlib import contextmanager
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
        table: sqlalchemy.Table = None
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
        meta.reflect(only=[table_name])
        table = meta.tables[
            full_table_name
        ]  # So we don't mess up the casing of the Table reference
        for property_name, property_def in schema["properties"].items():
            self.prepare_column(
                schema_name=schema_name,
                table=table,
                column_name=property_name,
                sql_type=self.to_sql_type(property_def),
            )

        return meta.tables[full_table_name]

    def copy_table_structure(
        self,
        full_table_name: str,
        from_table: sqlalchemy.Table,
        as_temp_table: bool = False,
    ) -> sqlalchemy.Table:
        """Copy table structure.

        Args:
            full_table_name: the target table name potentially including schema
            fromtable: the  source table
            as_temp_table: True to create a temp table.
        """
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        meta = sqlalchemy.MetaData(bind=self._engine, schema=schema_name)
        new_table: sqlalchemy.Table = None
        columns = []
        if self.table_exists(full_table_name=full_table_name):
            raise RuntimeError("Table already exists")
        for column in from_table.columns:
            columns.append(column.copy())
        if as_temp_table:
            new_table = sqlalchemy.Table(
                table_name, meta, *columns, prefixes=["TEMPORARY"]
            )
            new_table.create(bind=self.connection)
            return new_table
        else:
            new_table = sqlalchemy.Table(table_name, meta, *columns)
            new_table.create(bind=self.connection)
            return new_table

    def create_sqlalchemy_connection(self) -> sqlalchemy.engine.Connection:
        """Return a new SQLAlchemy connection using the provided config.

        Read more details about why server side cursors don't work on postgres here.
        DML/DDL doesn't work with this being on according to these docs

        https://docs.sqlalchemy.org/en/14/core/connections.html#using-server-side-cursors-a-k-a-stream-results

        Returns:
            A newly created SQLAlchemy engine object.
        """
        return self.create_sqlalchemy_engine().connect()

    @contextmanager
    def _connect(self) -> t.Iterator[sqlalchemy.engine.Connection]:
        with self._engine.connect().execution_options() as conn:
            yield conn

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

    def drop_table(self, table: sqlalchemy.Table):
        """Drop table data."""
        with self._connect() as connection:
            table.drop(bind=connection)

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
        with self._connect() as connection:
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

    def prepare_column(
        self,
        schema_name: str,
        table: sqlalchemy.Table,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Adapt target table to provided schema if possible.

        Args:
            full_table_name: the target table name.
            column_name: the target column name.
            sql_type: the SQLAlchemy type.
            schema_name: the schema name.
        """
        if not self.column_exists(table.fullname, column_name):
            self._create_empty_column(
                # We should migrate every function to use sqlalchemy.Table
                # instead of having to know what the function wants
                table_name=table.name,
                column_name=column_name,
                sql_type=sql_type,
                schema_name=schema_name,
            )
            return

        self._adapt_column_type(
            schema_name=schema_name,
            table_name=table.name,
            column_name=column_name,
            sql_type=sql_type,
        )

    def _create_empty_column(
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Create a new column.

        Args:
            full_table_name: The target table name.
            column_name: The name of the new column.
            sql_type: SQLAlchemy type engine to be used in creating the new column.

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
        with self._connect() as conn, conn.begin():
            conn.execute(column_add_ddl)

    def get_column_add_ddl(
        self,
        table_name: str,
        schema_name: str,
        column_name: str,
        column_type: sqlalchemy.types.TypeEngine,
    ) -> sqlalchemy.DDL:
        """Get the create column DDL statement.

        Args:
            table_name: Fully qualified table name of column to alter.
            schema_name: Schema name.
            column_name: Column name to create.
            column_type: New column sqlalchemy type.

        Returns:
            A sqlalchemy DDL instance.
        """
        column = sqlalchemy.Column(column_name, column_type)

        return sqlalchemy.DDL(
            'ALTER TABLE "%(schema_name)s"."%(table_name)s" ADD COLUMN %(column_name)s %(column_type)s',
            {
                "schema_name": schema_name,
                "table_name": table_name,
                "column_name": column.compile(dialect=self._engine.dialect),
                "column_type": column.type.compile(dialect=self._engine.dialect),
            },
        )

    def _adapt_column_type(
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Adapt table column type to support the new JSON schema type.

        Args:
            full_table_name: The target table name.
            column_name: The target column name.
            sql_type: The new SQLAlchemy type.

        Raises:
            NotImplementedError: if altering columns is not supported.
        """
        current_type: sqlalchemy.types.TypeEngine = self._get_column_type(
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
        )

        # remove collation if present and save it
        current_type_collation = self.remove_collation(current_type)

        # Check if the existing column type and the sql type are the same
        if str(sql_type) == str(current_type):
            # The current column and sql type are the same
            # Nothing to do
            return

        # Not the same type, generic type or compatible types
        # calling merge_sql_types for assistnace
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
        with self._connect() as conn:
            conn.execute(alter_column_ddl)

    def get_column_alter_ddl(
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        column_type: sqlalchemy.types.TypeEngine,
    ) -> sqlalchemy.DDL:
        """Get the alter column DDL statement.

        Override this if your database uses a different syntax for altering columns.

        Args:
            table_name: Fully qualified table name of column to alter.
            column_name: Column name to alter.
            column_type: New column type string.

        Returns:
            A sqlalchemy DDL instance.
        """
        column = sqlalchemy.Column(column_name, column_type)
        return sqlalchemy.DDL(
            'ALTER TABLE "%(schema_name)s"."%(table_name)s" ALTER COLUMN %(column_name)s %(column_type)s',
            {
                "schema_name": schema_name,
                "table_name": table_name,
                "column_name": column.compile(dialect=self._engine.dialect),
                "column_type": column.type.compile(dialect=self._engine.dialect),
            },
        )

    def _get_column_type(
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
    ) -> sqlalchemy.types.TypeEngine:
        """Get the SQL type of the declared column.

        Args:
            full_table_name: The name of the table.
            column_name: The name of the column.

        Returns:
            The type of the column.

        Raises:
            KeyError: If the provided column name does not exist.
        """
        try:
            column = self.get_table_columns(
                schema_name=schema_name,
                table_name=table_name,
            )[column_name]
        except KeyError as ex:
            msg = (
                f"Column `{column_name}` does not exist in table"
                "`{schema_name}.{table_name}`."
            )
            raise KeyError(msg) from ex

        return t.cast(sqlalchemy.types.TypeEngine, column.type)

    def get_table_columns(
        self,
        schema_name: str,
        table_name: str,
        column_names: list[str] | None = None,
    ) -> dict[str, sqlalchemy.Column]:
        """Return a list of table columns.

        Overrode to support schema_name

        Args:
            schema_name: schema name.
            table_name: table name to get columns for.
            column_names: A list of column names to filter to.

        Returns:
            An ordered list of column objects.
        """
        inspector = sqlalchemy.inspect(self._engine)
        columns = inspector.get_columns(table_name, schema_name)

        return {
            col_meta["name"]: sqlalchemy.Column(
                col_meta["name"],
                col_meta["type"],
                nullable=col_meta.get("nullable", False),
            )
            for col_meta in columns
            if not column_names
            or col_meta["name"].casefold() in {col.casefold() for col in column_names}
        }

    def column_exists(self, full_table_name: str, column_name: str) -> bool:
        """Determine if the target column already exists.

        Args:
            full_table_name: the target table name.
            column_name: the target column name.

        Returns:
            True if table exists, False if not.
        """
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        assert schema_name is not None
        assert table_name is not None
        return column_name in self.get_table_columns(
            schema_name=schema_name, table_name=table_name
        )
