"""Postgres target sink class, which handles writing streams."""

import uuid
from typing import Any, Dict, Iterable, List, Optional, Union, cast

import sqlalchemy
from pendulum import now
from singer_sdk.sinks import SQLSink
from sqlalchemy import Column, MetaData, Table, insert, select, update
from sqlalchemy.sql import Executable
from sqlalchemy.sql.expression import bindparam

from target_postgres.connector import PostgresConnector


class PostgresSink(SQLSink):
    """Postgres target sink class."""

    connector_class = PostgresConnector

    def __init__(self, *args, **kwargs):
        """Initialize SQL Sink. See super class for more details."""
        super().__init__(*args, **kwargs)
        self.temp_table_name = self.generate_temp_table_name()

    @property
    def append_only(self) -> bool:
        """Return True if the target is append only."""
        return self._append_only

    @append_only.setter
    def append_only(self, value: bool) -> None:
        """Set the append_only attribute."""
        self._append_only = value

    @property
    def connector(self) -> PostgresConnector:
        """Return the connector object.

        Returns:
            The connector object.
        """
        return cast(PostgresConnector, self._connector)

    def setup(self) -> None:
        """Set up Sink.

        This method is called on Sink creation, and creates the required Schema and
        Table entities in the target database.
        """
        if self.key_properties is None or self.key_properties == []:
            self.append_only = True
        else:
            self.append_only = False
        if self.schema_name:
            self.connector.prepare_schema(self.schema_name)
        with self.connector._connect() as connection, connection.begin():
            self.connector.prepare_table(
                full_table_name=self.full_table_name,
                schema=self.schema,
                primary_keys=self.key_properties,
                connection=connection,
                as_temp_table=False,
            )

    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.

        Writes a batch to the SQL target. Developers may override this method
        in order to provide a more efficient upload/upsert process.

        Args:
            context: Stream partition or context dictionary.
        """
        # Use one connection so we do this all in a single transaction
        with self.connector._connect() as connection, connection.begin():
            # Check structure of table
            table: sqlalchemy.Table = self.connector.prepare_table(
                full_table_name=self.full_table_name,
                schema=self.schema,
                primary_keys=self.key_properties,
                as_temp_table=False,
                connection=connection,
            )
            # Create a temp table (Creates from the table above)
            temp_table: sqlalchemy.Table = self.connector.copy_table_structure(
                full_table_name=self.temp_table_name,
                from_table=table,
                as_temp_table=True,
                connection=connection,
            )
            # Insert into temp table
            self.bulk_insert_records(
                table=temp_table,
                schema=self.schema,
                primary_keys=self.key_properties,
                records=context["records"],
                connection=connection,
            )
            # Merge data from Temp table to main table
            self.upsert(
                from_table=temp_table,
                to_table=table,
                schema=self.schema,
                join_keys=self.key_properties,
                connection=connection,
            )
            # Drop temp table
            self.connector.drop_table(table=temp_table, connection=connection)

    def generate_temp_table_name(self):
        """Uuid temp table name."""
        # sqlalchemy.exc.IdentifierError: Identifier
        # 'temp_test_optional_attributes_388470e9_fbd0_47b7_a52f_d32a2ee3f5f6'
        # exceeds maximum length of 63 characters
        # Is hit if we have a long table name, there is no limit on Temporary tables
        # in postgres, used a guid just in case we are using the same session
        return f"{str(uuid.uuid4()).replace('-','_')}"

    def bulk_insert_records(  # type: ignore[override]
        self,
        table: sqlalchemy.Table,
        schema: dict,
        records: Iterable[Dict[str, Any]],
        primary_keys: List[str],
        connection: sqlalchemy.engine.Connection,
    ) -> Optional[int]:
        """Bulk insert records to an existing destination table.

        The default implementation uses a generic SQLAlchemy bulk insert operation.
        This method may optionally be overridden by developers in order to provide
        faster, native bulk uploads.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.

        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        columns = self.column_representation(schema)
        insert: str = cast(
            str,
            self.generate_insert_statement(
                table.name,
                columns,
            ),
        )
        self.logger.info("Inserting with SQL: %s", insert)
        # Only one record per PK, we want to take the last one
        data_to_insert: List[Dict[str, Any]] = []

        if self.append_only is False:
            insert_records: Dict[str, Dict] = {}  # pk : record
            for record in records:
                insert_record = {}
                for column in columns:
                    insert_record[column.name] = record.get(column.name)
                # No need to check for a KeyError here because the SDK already
                # guaruntees that all key properties exist in the record.
                primary_key_value = "".join([str(record[key]) for key in primary_keys])
                insert_records[primary_key_value] = insert_record
            data_to_insert = list(insert_records.values())
        else:
            for record in records:
                insert_record = {}
                for column in columns:
                    insert_record[column.name] = record.get(column.name)
                data_to_insert.append(insert_record)
        connection.execute(insert, data_to_insert)
        return True

    def upsert(
        self,
        from_table: sqlalchemy.Table,
        to_table: sqlalchemy.Table,
        schema: dict,
        join_keys: List[str],
        connection: sqlalchemy.engine.Connection,
    ) -> Optional[int]:
        """Merge upsert data from one table to another.

        Args:
            from_table: The source table.
            to_table: The destination table.
            schema: Singer Schema message.
            join_keys: The merge upsert keys, or `None` to append.
            connection: The database connection.

        Return:
            The number of records copied, if detectable, or `None` if the API does not
            report number of records affected/inserted.

        """
        if self.append_only is True:
            # Insert
            select_stmt = select(from_table.columns).select_from(from_table)
            insert_stmt = to_table.insert().from_select(
                names=from_table.columns, select=select_stmt
            )
            connection.execute(insert_stmt)
        else:
            join_predicates = []
            to_table_key: sqlalchemy.Column
            for key in join_keys:
                from_table_key: sqlalchemy.Column = from_table.columns[key]
                to_table_key = to_table.columns[key]
                join_predicates.append(from_table_key == to_table_key)

            join_condition = sqlalchemy.and_(*join_predicates)

            where_predicates = []
            for key in join_keys:
                to_table_key = to_table.columns[key]
                where_predicates.append(to_table_key.is_(None))
            where_condition = sqlalchemy.and_(*where_predicates)

            select_stmt = (
                select(from_table.columns)
                .select_from(from_table.outerjoin(to_table, join_condition))
                .where(where_condition)
            )
            insert_stmt = insert(to_table).from_select(
                names=from_table.columns, select=select_stmt
            )

            connection.execute(insert_stmt)

            # Update
            where_condition = join_condition
            update_columns = {}
            for column_name in self.schema["properties"].keys():
                from_table_column: sqlalchemy.Column = from_table.columns[column_name]
                to_table_column: sqlalchemy.Column = to_table.columns[column_name]
                update_columns[to_table_column] = from_table_column

            update_stmt = update(to_table).where(where_condition).values(update_columns)
            connection.execute(update_stmt)

        return None

    def column_representation(
        self,
        schema: dict,
    ) -> List[Column]:
        """Return a sqlalchemy table representation for the current schema."""
        columns: list[Column] = []
        for property_name, property_jsonschema in schema["properties"].items():
            columns.append(
                Column(
                    property_name,
                    self.connector.to_sql_type(property_jsonschema),
                )
            )
        return columns

    def generate_insert_statement(
        self,
        full_table_name: str,
        columns: List[Column],  # type: ignore[override]
    ) -> Union[str, Executable]:
        """Generate an insert statement for the given records.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.

        Returns:
            An insert statement.
        """
        metadata = MetaData()
        table = Table(full_table_name, metadata, *columns)
        return insert(table)

    def conform_name(self, name: str, object_type: Optional[str] = None) -> str:
        """Conforming names of tables, schemas, column names."""
        return name

    @property
    def schema_name(self) -> Optional[str]:
        """Return the schema name or `None` if using names with no schema part.

                Note that after the next SDK release (after 0.14.0) we can remove this
                as it's already upstreamed.

        Returns:
            The target schema name.
        """
        # Look for a default_target_scheme in the configuraion fle
        default_target_schema: str = self.config.get("default_target_schema", None)
        parts = self.stream_name.split("-")

        # 1) When default_target_scheme is in the configuration use it
        # 2) if the streams are in <schema>-<table> format use the
        #    stream <schema>
        # 3) Return None if you don't find anything
        if default_target_schema:
            return default_target_schema

        if len(parts) in {2, 3}:
            # Stream name is a two-part or three-part identifier.
            # Use the second-to-last part as the schema name.
            stream_schema = self.conform_name(parts[-2], "schema")
            return stream_schema

        # Schema name not detected.
        return None

    def activate_version(self, new_version: int) -> None:
        """Bump the active version of the target table.

        Args:
            new_version: The version number to activate.
        """
        # There's nothing to do if the table doesn't exist yet
        # (which it won't the first time the stream is processed)
        if not self.connector.table_exists(self.full_table_name):
            return

        deleted_at = now()
        # Different from SingerSDK as we need to handle types the
        # same as SCHEMA messsages
        datetime_type = self.connector.to_sql_type(
            {"type": "string", "format": "date-time"}
        )

        # Different from SingerSDK as we need to handle types the
        # same as SCHEMA messsages
        integer_type = self.connector.to_sql_type({"type": "integer"})

        with self.connector._connect() as connection, connection.begin():
            if not self.connector.column_exists(
                full_table_name=self.full_table_name,
                column_name=self.version_column_name,
                connection=connection,
            ):
                self.connector.prepare_column(  # type: ignore[call-arg]
                    self.full_table_name,
                    self.version_column_name,  # type: ignore[arg-type]
                    sql_type=integer_type,
                    connection=connection,
                )

            metadata = MetaData()
            target_table = Table(
                self.table_name,
                metadata,
                autoload_with=connection.engine,
                schema=self.schema_name,
            )

            self.logger.info("Hard delete: %s", self.config.get("hard_delete"))
            if self.config["hard_delete"] is True:
                delete_stmt = sqlalchemy.delete(target_table).where(
                    sqlalchemy.or_(
                        target_table.c[self.version_column_name].is_(None),
                        target_table.c[self.version_column_name] <= new_version,
                    )
                )
                connection.execute(delete_stmt)
                return

            if not self.connector.column_exists(
                full_table_name=self.full_table_name,
                column_name=self.soft_delete_column_name,
                connection=connection,
            ):
                self.connector.prepare_column(  # type: ignore[call-arg]
                    self.full_table_name,
                    self.soft_delete_column_name,  # type: ignore[arg-type]
                    sql_type=datetime_type,
                    connection=connection,
                )
            # Need to deal with the case where data doesn't exist for the version column
            update_stmt = (
                update(target_table)
                .values(
                    {
                        target_table.c[self.soft_delete_column_name]: bindparam(
                            "deletedate"
                        )
                    }
                )
                .where(
                    sqlalchemy.and_(
                        sqlalchemy.or_(
                            target_table.c[self.version_column_name]
                            < bindparam("version"),
                            target_table.c[self.version_column_name].is_(None),
                        ),
                        target_table.c[self.soft_delete_column_name].is_(None),
                    )
                )
            )
            bind_params = {"deletedate": deleted_at, "version": new_version}
            connection.execute(update_stmt, bind_params)
