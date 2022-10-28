""" Attempt at making some standard Target Tests. """
# flake8: noqa
import io
from contextlib import redirect_stdout
from pathlib import Path

import pytest
from singer_sdk.testing import sync_end_to_end

from target_postgres.target import TargetPostgres
from target_postgres.tests.samples.aapl.aapl import Fundamentals
from target_postgres.tests.samples.sample_tap_countries.countries_tap import (
    SampleTapCountries,
)


@pytest.fixture()
def postgres_config():
    return {"sqlalchemy_url": "postgresql://postgres:postgres@localhost:5432/postgres"}


@pytest.fixture
def postgres_target(postgres_config) -> TargetPostgres:
    return TargetPostgres(config=postgres_config)


def singer_file_to_target(file_name, target) -> None:
    """Singer file to Target, emulates a tap run

    Equivalent to running cat file_path | target-name --config config.json.
    Note that this function loads all lines into memory, so it is
    not good very large files.

    Args:
        file_name: name to file in .tests/data_files to be sent into target
        Target: Target to pass data from file_path into..
    """
    file_path = Path(__file__).parent / Path("./data_files") / Path(file_name)
    buf = io.StringIO()
    with redirect_stdout(buf):
        with open(file_path, "r") as f:
            for line in f:
                print(line.rstrip("\r\n"))  # File endings are here,
                # and print adds another line ending so we need to remove one.
    buf.seek(0)
    target.listen(buf)


# TODO should set schemas for each tap individually so we don't collide
# Test name would work well
def test_countries_to_postgres(postgres_config):
    tap = SampleTapCountries(config={}, state=None)
    target = TargetPostgres(config=postgres_config)
    sync_end_to_end(tap, target)


def test_aapl_to_postgres(postgres_config):
    tap = Fundamentals(config={}, state=None)
    target = TargetPostgres(config=postgres_config)
    sync_end_to_end(tap, target)


# TODO this test should throw an exception
def test_record_before_schema(postgres_target):
    file_name = "record_before_schema.singer"
    singer_file_to_target(file_name, postgres_target)


# TODO this test should throw an exception
def test_invalid_schema(postgres_target):
    file_name = "invalid_schema.singer"
    singer_file_to_target(file_name, postgres_target)


# TODO this test should throw an exception
def test_record_missing_key_property(postgres_target):
    file_name = "record_missing_key_property.singer"
    singer_file_to_target(file_name, postgres_target)


# TODO this test should throw an exception
def test_record_missing_required_property(postgres_target):
    file_name = "record_missing_required_property.singer"
    singer_file_to_target(file_name, postgres_target)


# TODO test that data is correctly set
# see target-sqllit/tests/test_target_sqllite.py
def test_record_missing_required_property(postgres_target):
    file_name = "camelcase.singer"
    singer_file_to_target(file_name, postgres_target)


# TODO test that data is correctly set
def test_special_chars_in_attributes(postgres_target):
    file_name = "special_chars_in_attributes.singer"
    singer_file_to_target(file_name, postgres_target)


# TODO test that data is correctly set
def test_optional_attributes(postgres_target):
    file_name = "optional_attributes.singer"
    singer_file_to_target(file_name, postgres_target)


# TODO test that data is correctly set
def test_schema_no_properties(postgres_target):
    file_name = "schema_no_properties.singer"
    singer_file_to_target(file_name, postgres_target)


# TODO test that data is correct
def test_schema_updates(postgres_target):
    file_name = "schema_updates.singer"
    singer_file_to_target(file_name, postgres_target)


# TODO test that data is correct
def test_multiple_state_messages(postgres_target):
    file_name = "multiple_state_messages.singer"
    singer_file_to_target(file_name, postgres_target)


# TODO test that data is correct
def test_relational_data(postgres_target):
    file_name = "user_location_data.singer"
    singer_file_to_target(file_name, postgres_target)

    file_name = "user_location_data_upsert_data.singer"
    singer_file_to_target(file_name, postgres_target)


# TODO test that data is correct
def test_no_primary_keys(postgres_target):
    file_name = "no_primary_keys.singer"
    singer_file_to_target(file_name, postgres_target)

    file_name = "no_primary_keys_append.singer"
    singer_file_to_target(file_name, postgres_target)


# TODO test that data is correct
def test_duplicate_records(postgres_target):
    file_name = "duplicate_records.singer"
    singer_file_to_target(file_name, postgres_target)


# TODO test that data is correct
def test_array_data(postgres_target):
    file_name = "array_data.singer"
    singer_file_to_target(file_name, postgres_target)


# TODO test that data is correct
def test_encoded_string_data(postgres_target):
    file_name = "encoded_strings.singer"
    singer_file_to_target(file_name, postgres_target)


def test_tap_appl(postgres_target):
    file_name = "tap_aapl.singer"
    singer_file_to_target(file_name, postgres_target)


def test_tap_countries(postgres_target):
    file_name = "tap_countries.singer"
    singer_file_to_target(file_name, postgres_target)
