"""A simple tap with one big record and schema."""

import importlib.resources
import json

from singer_sdk import SchemaDirectory, Stream, StreamSchema, Tap

from . import data

DATA_DIR = importlib.resources.files(data)
SCHEMA_DIR = SchemaDirectory(DATA_DIR)


class AAPL(Stream):
    """An AAPL stream."""

    name = "aapl"
    schema = StreamSchema(SCHEMA_DIR, key="fundamentals")

    def get_records(self, _):
        """Generate a single record."""
        with DATA_DIR.joinpath("AAPL.json").open() as f:
            record = json.load(f)

        yield record


class Fundamentals(Tap):
    """Singer tap for fundamentals."""

    name = "fundamentals"

    def discover_streams(self):
        """Get financial streams."""
        return [AAPL(self)]
