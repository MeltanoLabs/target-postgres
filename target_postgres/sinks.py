"""Postgres target sink class, which handles writing streams."""
from typing import Any, Dict, Iterable, Optional 

from sqlalchemy import exc
from sqlalchemy.dialects.postgresql import insert

from singer_sdk.sinks import SQLSink

from target_postgres.connector import PostgresConnector


class PostgresSink(SQLSink):
    """Postgres target sink class."""

    connector_class = PostgresConnector

    def generate_insert_statement(
        self,
        full_table_name: str,
        schema: dict,
    ) -> str:
        """Generate an insert statement for the given records.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.

        Returns:
            An insert statement.
        """
        statement = insert(self.connector.get_table(full_table_name))

        return statement

    def bulk_insert_records(
        self,
        full_table_name: str,
        schema: dict,
        records: Iterable[Dict[str, Any]],
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
        insert_sql = self.generate_insert_statement(
            full_table_name,
            schema,
        )

        try:
            with self.connector.connection.engine.begin() as conn:
                conn.execute(
                    insert_sql,
                    records,
                )
        except exc.SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            self.logger.info(error)

        if isinstance(records, list):
            return len(records)  # If list, we can quickly return record count.

        return None  # Unknown record count.