"""Postgres target sink class, which handles writing streams."""
from singer_sdk.sinks import SQLSink

from target_postgres.connector import PostgresConnector
from typing import Optional, List
import sqlalchemy
import uuid
from singer_sdk.helpers._singer import CatalogEntry, MetadataMapping


class PostgresSink(SQLSink):
    """Postgres target sink class."""

    connector_class = PostgresConnector

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
            records=context["records"],
        )
        self.connector.truncate_table(self.temp_table_name)

    def generate_temp_table_name(self):
        return f"temp_{uuid.uuid4()}"
    
    def merge_upsert_from_table(
        self, from_table_name: str, to_table_name: str, join_keys: List[str]
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
        raise NotImplementedError()

    
