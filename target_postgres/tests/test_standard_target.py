""" Attempt at making some standard Target Tests. """
# flake8: noqa
import copy
import io
import uuid
from contextlib import redirect_stdout
from pathlib import Path

import jsonschema
import pytest
import sqlalchemy
from singer_sdk.testing import sync_end_to_end
from sqlalchemy import create_engine, engine_from_config

from target_postgres.connector import PostgresConnector
from target_postgres.target import TargetPostgres
from target_postgres.tests.samples.aapl.aapl import Fundamentals
from target_postgres.tests.samples.sample_tap_countries.countries_tap import (
    SampleTapCountries,
)


@pytest.fixture(scope="session")
def postgres_config():
    return {
        "dialect+driver": "postgresql+psycopg2",
        "host": "localhost",
        "user": "postgres",
        "password": "postgres",
        "database": "postgres",
        "port": 5432,
        "add_record_metadata": True,
        "hard_delete": False,
    }


@pytest.fixture
def postgres_target(postgres_config) -> TargetPostgres:
    return TargetPostgres(config=postgres_config)


def sqlalchemy_engine(config) -> sqlalchemy.engine.Engine:
    return create_engine(
        f"{config['dialect+driver']}://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    )


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
        with open(file_path) as f:
            for line in f:
                print(line.rstrip("\r\n"))  # File endings are here,
                # and print adds another line ending so we need to remove one.
    buf.seek(0)
    target.listen(buf)


# TODO should set schemas for each tap individually so we don't collide


def test_sqlalchemy_url_config(postgres_config):
    """Be sure that passing a sqlalchemy_url works"""
    host = postgres_config["host"]
    user = postgres_config["user"]
    password = postgres_config["password"]
    database = postgres_config["database"]
    port = postgres_config["port"]

    config = {
        "sqlalchemy_url": f"postgresql://{user}:{password}@{host}:{port}/{database}"
    }
    tap = SampleTapCountries(config={}, state=None)
    target = TargetPostgres(config=config)
    sync_end_to_end(tap, target)


def test_port_default_config():
    """Test that the default config is passed into the engine when the config doesn't provide it"""
    config = {
        "dialect+driver": "postgresql+psycopg2",
        "host": "localhost",
        "user": "postgres",
        "password": "postgres",
        "database": "postgres",
    }
    dialect_driver = config["dialect+driver"]
    host = config["host"]
    user = config["user"]
    password = config["password"]
    database = config["database"]
    target_config = TargetPostgres(config=config).config
    connector = PostgresConnector(target_config)

    engine: sqlalchemy.engine.Engine = connector.create_sqlalchemy_engine()
    assert (
        str(engine.url)
        == f"{dialect_driver}://{user}:{password}@{host}:5432/{database}"
    )


def test_port_config():
    """Test that the port config works"""
    config = {
        "dialect+driver": "postgresql+psycopg2",
        "host": "localhost",
        "user": "postgres",
        "password": "postgres",
        "database": "postgres",
        "port": 5433,
    }
    dialect_driver = config["dialect+driver"]
    host = config["host"]
    user = config["user"]
    password = config["password"]
    database = config["database"]
    target_config = TargetPostgres(config=config).config
    connector = PostgresConnector(target_config)

    engine: sqlalchemy.engine.Engine = connector.create_sqlalchemy_engine()
    assert (
        str(engine.url)
        == f"{dialect_driver}://{user}:{password}@{host}:5433/{database}"
    )


# Test name would work well
def test_countries_to_postgres(postgres_config):
    tap = SampleTapCountries(config={}, state=None)
    target = TargetPostgres(config=postgres_config)
    sync_end_to_end(tap, target)


def test_aapl_to_postgres(postgres_config):
    """Expect to fail with ValueError due to primary key https://github.com/MeltanoLabs/target-postgres/issues/54"""
    with pytest.raises(ValueError):
        tap = Fundamentals(config={}, state=None)
        target = TargetPostgres(config=postgres_config)
        sync_end_to_end(tap, target)


def test_record_before_schema(postgres_target):
    with pytest.raises(Exception) as e:
        file_name = "record_before_schema.singer"
        singer_file_to_target(file_name, postgres_target)

    assert (
        str(e.value) == "Schema message has not been sent for test_record_before_schema"
    )


def test_invalid_schema(postgres_target):
    with pytest.raises(Exception) as e:
        file_name = "invalid_schema.singer"
        singer_file_to_target(file_name, postgres_target)
    assert (
        str(e.value) == "Line is missing required properties key(s): {'type': 'object'}"
    )


def test_record_missing_key_property(postgres_target):
    with pytest.raises(Exception) as e:
        file_name = "record_missing_key_property.singer"
        singer_file_to_target(file_name, postgres_target)
    assert "Primary key not found in record." in str(e.value)


def test_record_missing_required_property(postgres_target):
    with pytest.raises(jsonschema.exceptions.ValidationError):
        file_name = "record_missing_required_property.singer"
        singer_file_to_target(file_name, postgres_target)


@pytest.mark.xfail
def test_camelcase(postgres_target):
    """https://github.com/MeltanoLabs/target-postgres/issues/64 will address fixing this"""
    file_name = "camelcase.singer"
    singer_file_to_target(file_name, postgres_target)


@pytest.mark.xfail
def test_special_chars_in_attributes(postgres_target):
    file_name = "special_chars_in_attributes.singer"
    singer_file_to_target(file_name, postgres_target)


# TODO test that data is correctly set
def test_optional_attributes(postgres_target):
    file_name = "optional_attributes.singer"
    singer_file_to_target(file_name, postgres_target)


def test_schema_no_properties(postgres_target):
    """Expect to fail with ValueError due to primary key https://github.com/MeltanoLabs/target-postgres/issues/54"""
    with pytest.raises(ValueError):
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

    file_name = "user_location_upsert_data.singer"
    singer_file_to_target(file_name, postgres_target)


def test_no_primary_keys(postgres_target):
    """Expect to fail with ValueError due to primary key https://github.com/MeltanoLabs/target-postgres/issues/54"""
    with pytest.raises(ValueError):
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
    """
    We removed NUL characters from the original encoded_strings.singer as postgres doesn't allow them.
    https://www.postgresql.org/docs/current/functions-string.html#:~:text=chr(0)%20is%20disallowed%20because%20text%20data%20types%20cannot%20store%20that%20character.
    chr(0) is disallowed because text data types cannot store that character.

    Note you will recieve a  ValueError: A string literal cannot contain NUL (0x00) characters. Which seems like a reasonable error.
    See issue https://github.com/MeltanoLabs/target-postgres/issues/60 for more details.
    """

    file_name = "encoded_strings.singer"
    singer_file_to_target(file_name, postgres_target)


def test_tap_appl(postgres_target):
    """Expect to fail with ValueError due to primary key https://github.com/MeltanoLabs/target-postgres/issues/54"""
    with pytest.raises(ValueError):
        file_name = "tap_aapl.singer"
        singer_file_to_target(file_name, postgres_target)


def test_tap_countries(postgres_target):
    file_name = "tap_countries.singer"
    singer_file_to_target(file_name, postgres_target)


def test_missing_value(postgres_target):
    file_name = "missing_value.singer"
    singer_file_to_target(file_name, postgres_target)


def test_large_int(postgres_target):
    file_name = "large_int.singer"
    singer_file_to_target(file_name, postgres_target)


def test_reserved_keywords(postgres_target):
    """Postgres has a number of resereved keywords listed here https://www.postgresql.org/docs/current/sql-keywords-appendix.html.

    The target should work regradless of the column names"""
    file_name = "reserved_keywords.singer"
    singer_file_to_target(file_name, postgres_target)


def test_new_array_column(postgres_target):
    """Create a new Array column with an existing table"""
    file_name = "new_array_column.singer"
    singer_file_to_target(file_name, postgres_target)


def test_activate_version_hard_delete(postgres_config):
    """Activate Version Hard Delete Test"""
    file_name = "activate_version_hard.singer"
    postgres_config_hard_delete_true = copy.deepcopy(postgres_config)
    postgres_config_hard_delete_true["hard_delete"] = True
    pg_hard_delete_true = TargetPostgres(config=postgres_config_hard_delete_true)
    singer_file_to_target(file_name, pg_hard_delete_true)
    engine = sqlalchemy_engine(postgres_config)
    with engine.connect() as connection:
        result = connection.execute("SELECT * FROM test_activate_version_hard")
        assert result.rowcount == 7
        # Add a record like someone would if they weren't using the tap target combo
        result = connection.execute(
            "INSERT INTO test_activate_version_hard(code, \"name\") VALUES('Manual1', 'Meltano')"
        )
        result = connection.execute(
            "INSERT INTO test_activate_version_hard(code, \"name\") VALUES('Manual2', 'Meltano')"
        )
        result = connection.execute("SELECT * FROM test_activate_version_hard")
        assert result.rowcount == 9

    singer_file_to_target(file_name, pg_hard_delete_true)

    # Should remove the 2 records we added manually
    with engine.connect() as connection:
        result = connection.execute("SELECT * FROM test_activate_version_hard")
        assert result.rowcount == 7


def test_activate_version_soft_delete(postgres_config):
    """Activate Version Soft Delete Test"""
    file_name = "activate_version_soft.singer"
    engine = sqlalchemy_engine(postgres_config)
    with engine.connect() as connection:
        result = connection.execute("DROP TABLE IF EXISTS test_activate_version_soft")
    postgres_config_soft_delete = copy.deepcopy(postgres_config)
    postgres_config_soft_delete["hard_delete"] = False
    pg_soft_delete = TargetPostgres(config=postgres_config_soft_delete)
    singer_file_to_target(file_name, pg_soft_delete)

    with engine.connect() as connection:
        result = connection.execute("SELECT * FROM test_activate_version_soft")
        assert result.rowcount == 7
        # Add a record like someone would if they weren't using the tap target combo
        result = connection.execute(
            "INSERT INTO test_activate_version_soft(code, \"name\") VALUES('Manual1', 'Meltano')"
        )
        result = connection.execute(
            "INSERT INTO test_activate_version_soft(code, \"name\") VALUES('Manual2', 'Meltano')"
        )
        result = connection.execute("SELECT * FROM test_activate_version_soft")
        assert result.rowcount == 9

    singer_file_to_target(file_name, pg_soft_delete)

    # Should have all records including the 2 we added manually
    with engine.connect() as connection:
        result = connection.execute("SELECT * FROM test_activate_version_soft")
        assert result.rowcount == 9

        result = connection.execute(
            "SELECT * FROM test_activate_version_soft where _sdc_deleted_at is NOT NULL"
        )
        assert result.rowcount == 2


def test_activate_version_deletes_data_properly(postgres_config):
    """Activate Version should"""
    table_name = "test_activate_version_deletes_data_properly"
    file_name = f"{table_name}.singer"
    engine = sqlalchemy_engine(postgres_config)
    with engine.connect() as connection:
        result = connection.execute(f"DROP TABLE IF EXISTS {table_name}")

    postgres_config_soft_delete = copy.deepcopy(postgres_config)
    postgres_config_soft_delete["hard_delete"] = True
    pg_hard_delete = TargetPostgres(config=postgres_config_soft_delete)
    singer_file_to_target(file_name, pg_hard_delete)
    # Will populate us with 7 records
    with engine.connect() as connection:
        result = connection.execute(
            f"INSERT INTO {table_name} (code, \"name\") VALUES('Manual1', 'Meltano')"
        )
        result = connection.execute(
            f"INSERT INTO {table_name} (code, \"name\") VALUES('Manual2', 'Meltano')"
        )
        result = connection.execute(f"SELECT * FROM {table_name}")
        assert result.rowcount == 9

    # Only has a schema and one activate_version message, should delete all records as it's a higher version than what's currently in the table
    file_name = f"{table_name}_2.singer"
    singer_file_to_target(file_name, pg_hard_delete)
    with engine.connect() as connection:
        result = connection.execute(f"SELECT * FROM {table_name}")
        assert result.rowcount == 0
