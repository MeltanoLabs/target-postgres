"""Postgres target sink class, which handles writing streams."""
from singer_sdk.sinks import SQLSink
from target_postgres.connector import PostgresConnector

class PostgresSink(SQLSink):
    connector_class = PostgresConnector
    """Postgres target sink class."""
    


