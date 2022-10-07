"""Connector class for target."""
import sqlalchemy
from sqlalchemy.engine import URL

from singer_sdk import SQLConnector


class PostgresConnector(SQLConnector):
    """Sets up SQL Alchemy, and other Postgres related stuff."""

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
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
        """Generates a SQLAlchemy URL for sqlbuzz.

        Args:
            config: The configuration for the connector.
        """
        if config['dialect'] == "postgresql" :
            url_drivername:str = config['dialect']
        else:
            self.logger.error("Invalid dialect given")
            exit(1)

        if config['driver_type'] in ["psycopg2","pg8000","asyncpg","psycopg2cffi","pypostgresql","pygresql"]:
            url_drivername += f"+{config['driver_type']}"
        else:
            self.logger.error("Invalid driver_type given")
            exit(1)

        self.logger.info(url_drivername)
        config_url = URL.create(
            url_drivername,
            config['user'],
            config['password'],
            host = config['host'],
            database = config['database']
        )

        if 'port' in config:
            config_url.set(port={config['port']})
        
        return (config_url)
        