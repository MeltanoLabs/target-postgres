"""Connector class for target."""
import sqlalchemy
from singer_sdk import SQLConnector
from typing import List, Optional
import sqlalchemy


class PostgresConnector(SQLConnector):
    """Sets up SQL Alchemy, and other Postgres related stuff."""

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = True # Whether MERGE UPSERT is supported.
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
    
    def merge_sql_types(
        self, sql_types: List[sqlalchemy.types.TypeEngine]
    ) -> sqlalchemy.types.TypeEngine:
        """Return a compatible SQL type for the selected type list.

        Args:
            sql_types: List of SQL types.

        Returns:
            A SQL type that is compatible with the input types.

        Raises:
            ValueError: If sql_types argument has zero members.
        """
        if not sql_types:
            raise ValueError("Expected at least one member in `sql_types` argument.")

        if len(sql_types) == 1:
            return sql_types[0]

        sql_types = self._sort_types(sql_types)

        if len(sql_types) > 2:
            return self.merge_sql_types(
                [self.merge_sql_types([sql_types[0], sql_types[1]])] + sql_types[2:]
            )

        assert len(sql_types) == 2
       
        # SQL Alchemy overrides column type comparisons (only difference
        # between super is these 2 lines)
        if str(sql_types[0]) == str(sql_types[1]):
            return sql_types[0]

        generic_type = type(sql_types[0].as_generic())
        if isinstance(generic_type, type):
            if issubclass(
                generic_type,
                (sqlalchemy.types.String, sqlalchemy.types.Unicode),
            ):
                return sql_types[0]

        elif isinstance(
            generic_type,
            (sqlalchemy.types.String, sqlalchemy.types.Unicode),
        ):
            return sql_types[0]

        raise ValueError(
            f"Unable to merge sql types: {', '.join([str(t) for t in sql_types])}"
        )
    
    
    def create_empty_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: Optional[List[str]] = None,
        partition_keys: Optional[List[str]] = None,
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

        meta = sqlalchemy.MetaData()
        columns: List[sqlalchemy.Column] = []
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

        _ = sqlalchemy.Table(full_table_name, meta, *columns)
        meta.create_all(self._engine)
    
    def truncate_table(self, name):
        self.connection.execute("TRUNCATE TABLE :name", name=name)

    def create_temp_table_from_table(self, from_table_name, temp_table_name):
        self.connection.execute("""
        CREATE TEMP TABLE :temp_table_name AS 
        SELECT * FROM :from_table_name LIMIT 0 
        """,
        temp_table_name=temp_table_name,
        from_table_name=from_table_name)

