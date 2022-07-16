"""Postgres target sink class, which handles writing streams."""
from singer_sdk.sinks import SQLSink

from target_postgres.connector import PostgresConnector


class PostgresSink(SQLSink):
    """Postgres target sink class."""

    connector_class = PostgresConnector
