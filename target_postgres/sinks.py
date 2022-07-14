"""Postgres target sink class, which handles writing streams."""


from singer_sdk.sinks import RecordSink


class PostgresSink(RecordSink):
    """Postgres target sink class."""

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        # Sample:
        # ------
        # client.write(record)
