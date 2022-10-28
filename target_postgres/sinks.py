"""Postgres target sink class, which handles writing streams."""
from singer_sdk.sinks import SQLSink

from target_postgres.connector import PostgresConnector
from typing import Optional, List, Iterable, Any, Dict, Union
from sqlalchemy import Table, insert, Column, bindparam, JSON, ARRAY, MetaData
import sqlalchemy
import uuid
import sqlalchemy.dialects.postgresql
from sqlalchemy.sql import Executable
import psycopg2.extras


class PostgresSink(SQLSink):
    """Postgres target sink class."""

    connector_class  = PostgresConnector

    def __init__(self, *args, **kwargs):
        self.temp_table_name = self.generate_temp_table_name()
        super().__init__(*args, **kwargs)
    
    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.

        Writes a batch to the SQL target. Developers may override this method
        in order to provide a more efficient upload/upsert process.

        Args:
            context: Stream partition or context dictionary.
        """
        #First we need to be sure the main table is already created
        self.connector.prepare_table(
            full_table_name=self.full_table_name,
            schema=self.schema,
            primary_keys=self.key_properties,
            as_temp_table=False,
        )
        #Create a temp table (Creates from the table above)
        self.connector.create_temp_table_from_table(
                from_table_name=self.full_table_name,
                temp_table_name=self.temp_table_name
        )
        #Insert into temp table
        self.bulk_insert_records(
            full_table_name=self.temp_table_name,
            schema=self.schema,
            records=context["records"],
        )
        #Merge data from Temp table to main table
        self.merge_upsert_from_table(
            from_table_name=self.temp_table_name,
            to_table_name=self.full_table_name,
            schema=self.schema,
            join_keys=self.key_properties
        )
        self.connector.truncate_table(self.temp_table_name)
    
    #Copied purely to help with type hints
    @property
    def connector(self) -> PostgresConnector:
        """The connector object.

        Returns:
            The connector object.
        """
        return self._connector

    def generate_temp_table_name(self):
        return f"temp_{str(uuid.uuid4()).replace('-','_')}"
    
    def merge_upsert_from_table(
            self, from_table_name: str, to_table_name: str, schema: dict, join_keys: List[str]
    ) -> Optional[int]:
        """Merge upsert data from one table to another.

        Args:
            target_table_name: The destination table name.
            from_table_name: The source table name.
            join_keys: The merge upsert keys, or `None` to append.

        Return:
            The number of records copied, if detectable, or `None` if the API does not
            report number of records affected/inserted.

        Raises:
            NotImplementedError: if the merge upsert capability does not exist or is
                undefined.
        """
        #TODO think about sql injeciton, issue here https://github.com/MeltanoLabs/target-postgres/issues/22


        #INSERT
        join_condition = " and ".join([f"temp.{key} = target.{key}" for key in join_keys])
        where_condition = " and ".join([f"target.{key} is null" for key in join_keys])

        insert_sql = f"""
        INSERT INTO {to_table_name}
        SELECT 
        temp.* 
        FROM {from_table_name} AS temp
        LEFT JOIN {to_table_name} AS target ON {join_condition}
        WHERE {where_condition}
        """
        self.connection.execute(insert_sql)

        #UPDATE
        columns = ", ".join([f"{column_name}=temp.{column_name}" for column_name in self.schema["properties"].keys()])
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
        records: Iterable[Dict[str, Any]]
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

        self.connector.connection.execute(insert, records)
        return True

    def column_representation(
        self,
        schema: dict,
    ) -> List[Column]:
        """Returns a sql alchemy table representation for the current schema.
        Note this is just for iterating the columns this isn't an actual Table representation of what's in Postgres (silly I know sorry!)
        https://github.com/MeltanoLabs/target-postgres/issues/21 would be a better implementation of this
        """
        columns: list[Column] = []
        conformed_properties  = self.conform_schema(schema)["properties"]
        for property_name, property_jsonschema in conformed_properties.items():
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
