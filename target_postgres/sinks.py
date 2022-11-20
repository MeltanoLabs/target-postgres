"""Postgres target sink class, which handles writing streams."""
import uuid
from typing import Any, Dict, Iterable, List, Optional, Union

from singer_sdk.sinks import SQLSink
from sqlalchemy import Column, MetaData, Table, insert
from sqlalchemy.sql import Executable

from target_postgres.connector import PostgresConnector


class PostgresSink(SQLSink):
    """Postgres target sink class."""

    connector_class = PostgresConnector

    def __init__(self, *args, **kwargs):
        """Constructor."""
        self.temp_table_name = self.generate_temp_table_name()
        super().__init__(*args, **kwargs)

    def setup(self) -> None:
        """Set up Sink.

        This method is called on Sink creation, and creates the required Schema and
        Table entities in the target database.
        """
        if self.schema_name:
            self.connector.prepare_schema(self.schema_name)
        self.connector.prepare_table(
            full_table_name=self.full_table_name,
            schema=self.schema,
            primary_keys=self.key_properties,
            as_temp_table=False,
        )

    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.

        Writes a batch to the SQL target. Developers may override this method
        in order to provide a more efficient upload/upsert process.

        Args:
            context: Stream partition or context dictionary.
        """
        # First we need to be sure the main table is already created
        self.connector.prepare_table(
            full_table_name=self.full_table_name,
            schema=self.schema,
            primary_keys=self.key_properties,
            as_temp_table=False,
        )
        # Create a temp table (Creates from the table above)
        self.connector.create_temp_table_from_table(
            from_table_name=self.full_table_name, temp_table_name=self.temp_table_name
        )
        # Insert into temp table
        self.bulk_insert_records(
            full_table_name=self.temp_table_name,
            schema=self.schema,
            primary_keys=self.key_properties,
            records=context["records"],
        )
        # Merge data from Temp table to main table
        self.merge_upsert_from_table(
            from_table_name=self.temp_table_name,
            to_table_name=self.full_table_name,
            schema=self.schema,
            join_keys=self.key_properties,
        )
        # Drop temp table
        self.connector.drop_table(self.temp_table_name)

    # Copied purely to help with type hints
    @property
    def connector(self) -> PostgresConnector:
        """The connector object.

        Returns:
            The connector object.
        """
        return self._connector

    def generate_temp_table_name(self):
        """Uuid temp table name."""
        return f"temp_{str(uuid.uuid4()).replace('-','_')}"

    def merge_upsert_from_table(
        self,
        from_table_name: str,
        to_table_name: str,
        schema: dict,
        join_keys: List[str],
    ) -> Optional[int]:
        """Merge upsert data from one table to another.

        Args:
            from_table_name: The source table name.
            to_table_name: The destination table name.
            join_keys: The merge upsert keys, or `None` to append.
            schema: Singer Schema message.

        Return:
            The number of records copied, if detectable, or `None` if the API does not
            report number of records affected/inserted.

        """
        # TODO think about sql injeciton,
        # issue here https://github.com/MeltanoLabs/target-postgres/issues/22

        # INSERT
        join_condition = " and ".join(
            [f'temp."{key}" = target."{key}"' for key in join_keys]
        )
        where_condition = " and ".join([f'target."{key}" is null' for key in join_keys])

        insert_sql = f"""
        INSERT INTO {to_table_name}
        SELECT
        temp.*
        FROM {from_table_name} AS temp
        LEFT JOIN {to_table_name} AS target ON {join_condition}
        WHERE {where_condition}
        """
        self.connection.execute(insert_sql)

        # UPDATE
        columns = ", ".join(
            [
                f'"{column_name}"=temp."{column_name}"'
                for column_name in self.schema["properties"].keys()
            ]
        )
        where_condition = join_condition
        update_sql = f"""
        UPDATE {to_table_name} AS target
        SET {columns}
        FROM {from_table_name} AS temp
        WHERE {where_condition}
        """
        self.connection.execute(update_sql)

    def bulk_insert_records(
        self,
        full_table_name: str,
        schema: dict,
        records: Iterable[Dict[str, Any]],
        primary_keys: List[str],
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
        insert = self.generate_insert_statement(
            full_table_name,
            columns,
        )
        self.logger.info("Inserting with SQL: %s", insert)
        # Only one record per PK, we want to take the last one
        insert_records: Dict[str, Dict] = {}  # pk : record
        for record in records:
            insert_record = {}
            for column in columns:
                insert_record[column.name] = record.get(column.name)
            primary_key_value = "".join([str(record[key]) for key in primary_keys])
            insert_records[primary_key_value] = insert_record

        self.connector.connection.execute(insert, list(insert_records.values()))
        return True

    def column_representation(
        self,
        schema: dict,
    ) -> List[Column]:
        """Returns a sql alchemy table representation for the current schema."""
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
        columns: List[Column],
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
