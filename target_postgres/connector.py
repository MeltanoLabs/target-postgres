from typing import Dict, Any, cast
from singer_sdk import SQLConnector
import sqlalchemy

class PostgresConnector(SQLConnector):
    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    # Can't support stream_results
    # We get sqlalchemy.exc.ProgrammingError: (psycopg2.ProgrammingError) can't call .executemany() on named cursors
    def create_sqlalchemy_connection(self) -> sqlalchemy.engine.Connection:
        """Return a new SQLAlchemy connection using the provided config.

        Read more details about why this doesn't work on postgres here.
        DML/DDL doesn't work with this being on according to these docs

        https://docs.sqlalchemy.org/en/14/core/connections.html#using-server-side-cursors-a-k-a-stream-results

        Returns:
            A newly created SQLAlchemy engine object.
        """
        return (
            self.create_sqlalchemy_engine()
            .connect()
        )
