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
        "ssl_enable": True,
        "ssl_client_certificate_enable": True,
        "ssl_mode": "verify-full",
        "ssl_certificate_authority": "./ssl/root.crt",
        "ssl_client_certificate": "./ssl/cert.crt",
        "ssl_client_private_key": "./ssl/pkey.key",
        "add_record_metadata": True,
        "hard_delete": False,
        "default_target_schema": "melty",
    }


@pytest.fixture(scope="session")
def postgres_config_no_ssl():
    return {
        "dialect+driver": "postgresql+psycopg2",
        "host": "localhost",
        "user": "postgres",
        "password": "postgres",
        "database": "postgres",
        "port": 5433,
        "add_record_metadata": True,
        "hard_delete": False,
        "default_target_schema": "melty",
    }


@pytest.fixture
def postgres_target(postgres_config) -> TargetPostgres:
    return TargetPostgres(config=postgres_config)


@pytest.fixture
def engine(postgres_config_no_ssl) -> sqlalchemy.engine.Engine:
    return create_engine(
        f"{(postgres_config_no_ssl)['dialect+driver']}://"
        f"{(postgres_config_no_ssl)['user']}:{(postgres_config_no_ssl)['password']}@"
        f"{(postgres_config_no_ssl)['host']}:{(postgres_config_no_ssl)['port']}/"
        f"{(postgres_config_no_ssl)['database']}"
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


def test_sqlalchemy_url_config(postgres_config_no_ssl):
    """Be sure that passing a sqlalchemy_url works

    postgres_config_no_ssl is used because an SQLAlchemy URL will override all SSL
    settings and preclude connecting to a database using SSL.
    """
    host = postgres_config_no_ssl["host"]
    user = postgres_config_no_ssl["user"]
    password = postgres_config_no_ssl["password"]
    database = postgres_config_no_ssl["database"]
    port = postgres_config_no_ssl["port"]

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


def test_camelcase(postgres_target):
    file_name = "camelcase.singer"
    singer_file_to_target(file_name, postgres_target)


def test_special_chars_in_attributes(postgres_target):
    file_name = "special_chars_in_attributes.singer"
    singer_file_to_target(file_name, postgres_target)


# TODO test that data is correctly set
def test_optional_attributes(postgres_target):
    file_name = "optional_attributes.singer"
    singer_file_to_target(file_name, postgres_target)


def test_schema_no_properties(postgres_target):
    """Expect to fail with ValueError"""
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


def test_no_primary_keys(postgres_config_no_ssl, engine):
    """We run both of these tests twice just to ensure that no records are removed and append only works properly"""
    table_name = "test_no_pk"
    postgres_target = TargetPostgres(config=postgres_config_no_ssl)
    full_table_name = postgres_target.config["default_target_schema"] + "." + table_name
    with engine.connect() as connection:
        result = connection.execute(f"DROP TABLE IF EXISTS {full_table_name}")
    file_name = f"{table_name}.singer"
    singer_file_to_target(file_name, postgres_target)

    file_name = f"{table_name}_append.singer"
    singer_file_to_target(file_name, postgres_target)

    file_name = f"{table_name}.singer"
    singer_file_to_target(file_name, postgres_target)

    file_name = f"{table_name}_append.singer"
    singer_file_to_target(file_name, postgres_target)

    # Will populate us with 22 records, we run this twice
    with engine.connect() as connection:
        result = connection.execute(f"SELECT * FROM {full_table_name}")
        assert result.rowcount == 16


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


def test_anyof(postgres_target):
    """Test that anyOf is handled correctly"""
    file_name = "anyof.singer"
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


def test_activate_version_hard_delete(postgres_config_no_ssl, engine):
    """Activate Version Hard Delete Test"""
    table_name = "test_activate_version_hard"
    file_name = f"{table_name}.singer"
    full_table_name = postgres_config_no_ssl["default_target_schema"] + "." + table_name
    postgres_config_hard_delete_true = copy.deepcopy(postgres_config_no_ssl)
    postgres_config_hard_delete_true["hard_delete"] = True
    pg_hard_delete_true = TargetPostgres(config=postgres_config_hard_delete_true)
    singer_file_to_target(file_name, pg_hard_delete_true)
    with engine.connect() as connection:
        result = connection.execute(f"SELECT * FROM {full_table_name}")
        assert result.rowcount == 7
        # Add a record like someone would if they weren't using the tap target combo
        result = connection.execute(
            f"INSERT INTO {full_table_name}(code, \"name\") VALUES('Manual1', 'Meltano')"
        )
        result = connection.execute(
            f"INSERT INTO {full_table_name}(code, \"name\") VALUES('Manual2', 'Meltano')"
        )
        result = connection.execute(f"SELECT * FROM {full_table_name}")
        assert result.rowcount == 9

    singer_file_to_target(file_name, pg_hard_delete_true)

    # Should remove the 2 records we added manually
    with engine.connect() as connection:
        result = connection.execute(f"SELECT * FROM {full_table_name}")
        assert result.rowcount == 7


def test_activate_version_soft_delete(postgres_config_no_ssl, engine):
    """Activate Version Soft Delete Test"""
    table_name = "test_activate_version_soft"
    file_name = f"{table_name}.singer"
    full_table_name = postgres_config_no_ssl["default_target_schema"] + "." + table_name
    with engine.connect() as connection:
        result = connection.execute(f"DROP TABLE IF EXISTS {full_table_name}")
    postgres_config_soft_delete = copy.deepcopy(postgres_config_no_ssl)
    postgres_config_soft_delete["hard_delete"] = False
    pg_soft_delete = TargetPostgres(config=postgres_config_soft_delete)
    singer_file_to_target(file_name, pg_soft_delete)

    with engine.connect() as connection:
        result = connection.execute(f"SELECT * FROM {full_table_name}")
        assert result.rowcount == 7
        # Add a record like someone would if they weren't using the tap target combo
        result = connection.execute(
            f"INSERT INTO {full_table_name}(code, \"name\") VALUES('Manual1', 'Meltano')"
        )
        result = connection.execute(
            f"INSERT INTO {full_table_name}(code, \"name\") VALUES('Manual2', 'Meltano')"
        )
        result = connection.execute(f"SELECT * FROM {full_table_name}")
        assert result.rowcount == 9

    singer_file_to_target(file_name, pg_soft_delete)

    # Should have all records including the 2 we added manually
    with engine.connect() as connection:
        result = connection.execute(f"SELECT * FROM {full_table_name}")
        assert result.rowcount == 9

        result = connection.execute(
            f"SELECT * FROM {full_table_name} where _sdc_deleted_at is NOT NULL"
        )
        assert result.rowcount == 2


def test_activate_version_deletes_data_properly(postgres_config_no_ssl, engine):
    """Activate Version should"""
    table_name = "test_activate_version_deletes_data_properly"
    file_name = f"{table_name}.singer"
    full_table_name = postgres_config_no_ssl["default_target_schema"] + "." + table_name
    with engine.connect() as connection:
        result = connection.execute(f"DROP TABLE IF EXISTS {full_table_name}")

    postgres_config_soft_delete = copy.deepcopy(postgres_config_no_ssl)
    postgres_config_soft_delete["hard_delete"] = True
    pg_hard_delete = TargetPostgres(config=postgres_config_soft_delete)
    singer_file_to_target(file_name, pg_hard_delete)
    # Will populate us with 7 records
    with engine.connect() as connection:
        result = connection.execute(
            f"INSERT INTO {full_table_name} (code, \"name\") VALUES('Manual1', 'Meltano')"
        )
        result = connection.execute(
            f"INSERT INTO {full_table_name} (code, \"name\") VALUES('Manual2', 'Meltano')"
        )
        result = connection.execute(f"SELECT * FROM {full_table_name}")
        assert result.rowcount == 9

    # Only has a schema and one activate_version message, should delete all records as it's a higher version than what's currently in the table
    file_name = f"{table_name}_2.singer"
    singer_file_to_target(file_name, pg_hard_delete)
    with engine.connect() as connection:
        result = connection.execute(f"SELECT * FROM {full_table_name}")
        assert result.rowcount == 0


def test_postgres_ssl_no_config(postgres_config_no_ssl):
    """Test that connection will fail when no SSL configuration options are provided.

    postgres_config_no_ssl has no configuration options for SSL, but port 5432 is a
    database that requires SSL. An error is expected because connecting to this database
    without SSL enabled shouldn't be possible.
    """

    tap = SampleTapCountries(config={}, state=None)

    postgres_config_modified = copy.deepcopy(postgres_config_no_ssl)
    postgres_config_modified["port"] = 5432

    with pytest.raises(sqlalchemy.exc.OperationalError):
        target = TargetPostgres(config=postgres_config_modified)
        sync_end_to_end(tap, target)


def test_postgres_ssl_no_pkey(postgres_config):
    """Test that connection will fail when no private key is provided."""

    postgres_config_modified = copy.deepcopy(postgres_config)
    postgres_config_modified["ssl_client_private_key"] = None

    # This is an AssertionError because checking that a private key exists is asserted
    # for when ssl_client_certificate_enable is on.
    with pytest.raises(AssertionError):
        TargetPostgres(config=postgres_config_modified)


def test_postgres_ssl_public_pkey(postgres_config):
    """Test that connection will fail when private key access is not restricted."""

    tap = SampleTapCountries(config={}, state=None)

    postgres_config_modified = copy.deepcopy(postgres_config)
    postgres_config_modified["ssl_client_private_key"] = "./ssl/public_pkey.key"

    # If the private key exists but access is too public, the target won't fail until
    # the it attempts to establish a connection to the database.
    with pytest.raises(sqlalchemy.exc.OperationalError):
        target = TargetPostgres(config=postgres_config_modified)
        sync_end_to_end(tap, target)


def test_postgres_ssl_no_client_cert(postgres_config):
    """Test that connection will fail when client certificate is not provided."""
    postgres_config_modified = copy.deepcopy(postgres_config)
    postgres_config_modified["ssl_client_certificate"] = None

    # This is an AssertionError because checking that a certificate exists is asserted
    # for when ssl_client_certificate_enable is on.
    with pytest.raises(AssertionError):
        TargetPostgres(config=postgres_config_modified)


def test_postgres_ssl_invalid_cn(postgres_config):
    """Test that connection will fail due to non-matching common names.

    The server is configured with certificates that state it is hosted at "localhost",
    which won't match the loopback address "127.0.0.1". Because verify-full (the
    default) requires them to match, an error is expected.
    """
    tap = SampleTapCountries(config={}, state=None)

    postgres_config_modified = copy.deepcopy(postgres_config)
    postgres_config_modified["host"] = "127.0.0.1"
    postgres_config_modified["ssl_mode"] = "verify-full"

    with pytest.raises(sqlalchemy.exc.OperationalError):
        target = TargetPostgres(config=postgres_config_modified)
        sync_end_to_end(tap, target)


def test_postgres_ssl_verify_ca(postgres_config):
    """Test that connection will succeed despite non-matching common names.

    When verify-ca is used, it does not matter that "localhost" and "127.0.0.1" don't
    match, so no error is expected.
    """
    tap = SampleTapCountries(config={}, state=None)

    postgres_config_modified = copy.deepcopy(postgres_config)
    postgres_config_modified["host"] = "127.0.0.1"
    postgres_config_modified["ssl_mode"] = "verify-ca"

    target = TargetPostgres(config=postgres_config_modified)
    sync_end_to_end(tap, target)


def test_postgres_ssl_unsupported(postgres_config):
    """Test that a connection to a database without SSL configured will fail.

    Port 5433 is established as the "postgres_no_ssl" service and uses a database
    configuration that doesn't have SSL configured. Because the default ssl mode
    (verify-full) requires SSL, an error is expected.
    """
    tap = SampleTapCountries(config={}, state=None)

    postgres_config_modified = copy.deepcopy(postgres_config)
    postgres_config_modified["port"] = 5433  # Alternate service: postgres_no_ssl

    with pytest.raises(sqlalchemy.exc.OperationalError):
        target = TargetPostgres(config=postgres_config_modified)
        sync_end_to_end(tap, target)


def test_postgres_ssl_prefer(postgres_config):
    """Test that a connection without SSL will succeed when ssl_mode=prefer.

    ssl_mode=prefer uses opportunistic encryption, but shouldn't fail if the database
    doesn't support SSL, so no error is expected.
    """
    tap = SampleTapCountries(config={}, state=None)

    postgres_config_modified = copy.deepcopy(postgres_config)
    postgres_config_modified["port"] = 5433  # Alternative service: postgres_no_ssl
    postgres_config_modified["ssl_mode"] = "prefer"

    target = TargetPostgres(config=postgres_config_modified)
    sync_end_to_end(tap, target)
