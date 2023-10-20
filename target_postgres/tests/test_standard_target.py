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
from singer_sdk.exceptions import MissingKeyPropertiesError
from singer_sdk.testing import sync_end_to_end
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.types import TEXT, TIMESTAMP

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


@pytest.fixture(scope="session")
def postgres_config_ssh_tunnel():
    return {
        "sqlalchemy_url": "postgresql://postgres:postgres@10.5.0.5:5432/main",
        "ssh_tunnel": {
            "enable": True,
            "host": "127.0.0.1",
            "port": 2223,
            "username": "melty",
            "private_key": "-----BEGIN OPENSSH PRIVATE KEY-----\n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\n-----END OPENSSH PRIVATE KEY-----",  # noqa: E501
        },
    }


@pytest.fixture
def postgres_target(postgres_config) -> TargetPostgres:
    return TargetPostgres(config=postgres_config)


def create_engine(target_postgres: TargetPostgres) -> sqlalchemy.engine.Engine:
    return TargetPostgres.default_sink_class.connector_class(
        config=target_postgres.config
    )._engine


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


def remove_metadata_columns(row: dict) -> dict:
    new_row = {}
    for column in row.keys():
        if not column.startswith("_sdc"):
            new_row[column] = row[column]
    return new_row


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

    engine: sqlalchemy.engine.Engine = connector._engine
    assert (
        engine.url.render_as_string(hide_password=False)
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

    engine: sqlalchemy.engine.Engine = connector._engine
    assert (
        engine.url.render_as_string(hide_password=False)
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


def test_invalid_schema(postgres_target):
    with pytest.raises(Exception) as e:
        file_name = "invalid_schema.singer"
        singer_file_to_target(file_name, postgres_target)
    assert (
        str(e.value) == "Line is missing required properties key(s): {'type': 'object'}"
    )


def test_record_missing_key_property(postgres_target):
    with pytest.raises(MissingKeyPropertiesError) as e:
        file_name = "record_missing_key_property.singer"
        singer_file_to_target(file_name, postgres_target)
    assert "Record is missing one or more key_properties." in str(e.value)


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


def test_relational_data(postgres_target):
    engine = create_engine(postgres_target)
    file_name = "user_location_data.singer"
    singer_file_to_target(file_name, postgres_target)

    file_name = "user_location_upsert_data.singer"
    singer_file_to_target(file_name, postgres_target)

    schema_name = postgres_target.config["default_target_schema"]

    with engine.connect() as connection:
        expected_test_users = [
            {"id": 1, "name": "Johny"},
            {"id": 2, "name": "George"},
            {"id": 3, "name": "Jacob"},
            {"id": 4, "name": "Josh"},
            {"id": 5, "name": "Jim"},
            {"id": 8, "name": "Thomas"},
            {"id": 12, "name": "Paul"},
            {"id": 13, "name": "Mary"},
        ]

        full_table_name = f"{schema_name}.test_users"
        result = connection.execute(
            sqlalchemy.text(f"SELECT * FROM {full_table_name} ORDER BY id")
        )
        result_dict = [remove_metadata_columns(row._asdict()) for row in result.all()]
        assert result_dict == expected_test_users

        expected_test_locations = [
            {"id": 1, "name": "Philly"},
            {"id": 2, "name": "NY"},
            {"id": 3, "name": "San Francisco"},
            {"id": 6, "name": "Colorado"},
            {"id": 8, "name": "Boston"},
        ]

        full_table_name = f"{schema_name}.test_locations"
        result = connection.execute(
            sqlalchemy.text(f"SELECT * FROM {full_table_name} ORDER BY id")
        )
        result_dict = [remove_metadata_columns(row._asdict()) for row in result.all()]
        assert result_dict == expected_test_locations

        expected_test_user_in_location = [
            {
                "id": 1,
                "user_id": 1,
                "location_id": 4,
                "info": {"weather": "rainy", "mood": "sad"},
            },
            {
                "id": 2,
                "user_id": 2,
                "location_id": 3,
                "info": {"weather": "sunny", "mood": "satisfied"},
            },
            {
                "id": 3,
                "user_id": 1,
                "location_id": 3,
                "info": {"weather": "sunny", "mood": "happy"},
            },
            {
                "id": 6,
                "user_id": 3,
                "location_id": 2,
                "info": {"weather": "sunny", "mood": "happy"},
            },
            {
                "id": 14,
                "user_id": 4,
                "location_id": 1,
                "info": {"weather": "cloudy", "mood": "ok"},
            },
        ]

        full_table_name = f"{schema_name}.test_user_in_location"
        result = connection.execute(
            sqlalchemy.text(f"SELECT * FROM {full_table_name} ORDER BY id")
        )
        result_dict = [remove_metadata_columns(row._asdict()) for row in result.all()]
        assert result_dict == expected_test_user_in_location


def test_no_primary_keys(postgres_target):
    """We run both of these tests twice just to ensure that no records are removed and append only works properly"""
    engine = create_engine(postgres_target)
    table_name = "test_no_pk"
    full_table_name = postgres_target.config["default_target_schema"] + "." + table_name
    with engine.connect() as connection, connection.begin():
        result = connection.execute(
            sqlalchemy.text(f"DROP TABLE IF EXISTS {full_table_name}")
        )
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
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert result.rowcount == 16


def test_no_type(postgres_target):
    file_name = "test_no_type.singer"
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
    engine = create_engine(postgres_target)
    table_name = "commits"
    file_name = f"{table_name}.singer"
    schema = postgres_target.config["default_target_schema"]
    singer_file_to_target(file_name, postgres_target)
    with engine.connect() as connection:
        meta = sqlalchemy.MetaData()
        table = sqlalchemy.Table(
            "commits", meta, schema=schema, autoload_with=connection
        )
        for column in table.c:
            # {"type":"string"}
            if column.name == "id":
                assert isinstance(column.type, TEXT)

            # Any of nullable date-time.
            # Note that postgres timestamp is equivalent to jsonschema date-time.
            # {"anyOf":[{"type":"string","format":"date-time"},{"type":"null"}]}
            if column.name in {"authored_date", "committed_date"}:
                assert isinstance(column.type, TIMESTAMP)

            # Any of nullable array of strings or single string.
            # {"anyOf":[{"type":"array","items":{"type":["null","string"]}},{"type":"string"},{"type":"null"}]}
            if column.name == "parent_ids":
                assert isinstance(column.type, ARRAY)

            # Any of nullable string.
            # {"anyOf":[{"type":"string"},{"type":"null"}]}
            if column.name == "commit_message":
                assert isinstance(column.type, TEXT)

            # Any of nullable string or integer.
            # {"anyOf":[{"type":"string"},{"type":"integer"},{"type":"null"}]}
            if column.name == "legacy_id":
                assert isinstance(column.type, TEXT)


def test_new_array_column(postgres_target):
    """Create a new Array column with an existing table"""
    file_name = "new_array_column.singer"
    singer_file_to_target(file_name, postgres_target)


def test_activate_version_hard_delete(postgres_config_no_ssl):
    """Activate Version Hard Delete Test"""
    table_name = "test_activate_version_hard"
    file_name = f"{table_name}.singer"
    full_table_name = postgres_config_no_ssl["default_target_schema"] + "." + table_name
    postgres_config_hard_delete_true = copy.deepcopy(postgres_config_no_ssl)
    postgres_config_hard_delete_true["hard_delete"] = True
    pg_hard_delete_true = TargetPostgres(config=postgres_config_hard_delete_true)
    engine = create_engine(pg_hard_delete_true)
    singer_file_to_target(file_name, pg_hard_delete_true)
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert result.rowcount == 7
    with engine.connect() as connection, connection.begin():
        # Add a record like someone would if they weren't using the tap target combo
        result = connection.execute(
            sqlalchemy.text(
                f"INSERT INTO {full_table_name}(code, \"name\") VALUES('Manual1', 'Meltano')"
            )
        )
        result = connection.execute(
            sqlalchemy.text(
                f"INSERT INTO {full_table_name}(code, \"name\") VALUES('Manual2', 'Meltano')"
            )
        )
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert result.rowcount == 9

    singer_file_to_target(file_name, pg_hard_delete_true)

    # Should remove the 2 records we added manually
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert result.rowcount == 7


def test_activate_version_soft_delete(postgres_target):
    """Activate Version Soft Delete Test"""
    engine = create_engine(postgres_target)
    table_name = "test_activate_version_soft"
    file_name = f"{table_name}.singer"
    full_table_name = postgres_target.config["default_target_schema"] + "." + table_name
    with engine.connect() as connection, connection.begin():
        result = connection.execute(
            sqlalchemy.text(f"DROP TABLE IF EXISTS {full_table_name}")
        )
    postgres_config_soft_delete = copy.deepcopy(postgres_target._config)
    postgres_config_soft_delete["hard_delete"] = False
    pg_soft_delete = TargetPostgres(config=postgres_config_soft_delete)
    singer_file_to_target(file_name, pg_soft_delete)

    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert result.rowcount == 7
    with engine.connect() as connection, connection.begin():
        # Add a record like someone would if they weren't using the tap target combo
        result = connection.execute(
            sqlalchemy.text(
                f"INSERT INTO {full_table_name}(code, \"name\") VALUES('Manual1', 'Meltano')"
            )
        )
        result = connection.execute(
            sqlalchemy.text(
                f"INSERT INTO {full_table_name}(code, \"name\") VALUES('Manual2', 'Meltano')"
            )
        )
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert result.rowcount == 9

    singer_file_to_target(file_name, pg_soft_delete)

    # Should have all records including the 2 we added manually
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert result.rowcount == 9

        result = connection.execute(
            sqlalchemy.text(
                f"SELECT * FROM {full_table_name} where _sdc_deleted_at is NOT NULL"
            )
        )
        assert result.rowcount == 2


def test_activate_version_deletes_data_properly(postgres_target):
    """Activate Version should"""
    engine = create_engine(postgres_target)
    table_name = "test_activate_version_deletes_data_properly"
    file_name = f"{table_name}.singer"
    full_table_name = postgres_target.config["default_target_schema"] + "." + table_name
    with engine.connect() as connection, connection.begin():
        result = connection.execute(
            sqlalchemy.text(f"DROP TABLE IF EXISTS {full_table_name}")
        )

    postgres_config_soft_delete = copy.deepcopy(postgres_target._config)
    postgres_config_soft_delete["hard_delete"] = True
    pg_hard_delete = TargetPostgres(config=postgres_config_soft_delete)
    singer_file_to_target(file_name, pg_hard_delete)
    # Will populate us with 7 records
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert result.rowcount == 7
    with engine.connect() as connection, connection.begin():
        result = connection.execute(
            sqlalchemy.text(
                f"INSERT INTO {full_table_name} (code, \"name\") VALUES('Manual1', 'Meltano')"
            )
        )
        result = connection.execute(
            sqlalchemy.text(
                f"INSERT INTO {full_table_name} (code, \"name\") VALUES('Manual2', 'Meltano')"
            )
        )
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert result.rowcount == 9
    # Only has a schema and one activate_version message, should delete all records as it's a higher version than what's currently in the table
    file_name = f"{table_name}_2.singer"
    singer_file_to_target(file_name, pg_hard_delete)
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert result.rowcount == 0


def test_reserved_keywords(postgres_target):
    """Target should work regardless of column names

    Postgres has a number of resereved keywords listed here https://www.postgresql.org/docs/current/sql-keywords-appendix.html.
    """
    file_name = "reserved_keywords.singer"
    singer_file_to_target(file_name, postgres_target)


def test_uppercase_stream_name_with_column_alter(postgres_target):
    """Column Alters need to work with uppercase stream names"""
    file_name = "uppercase_stream_name_with_column_alter.singer"
    singer_file_to_target(file_name, postgres_target)


def test_activate_version_uppercase_stream_name(postgres_config_no_ssl):
    """Activate Version should work with uppercase stream names"""
    file_name = "test_activate_version_uppercase_stream_name.singer"
    postgres_config_hard_delete = copy.deepcopy(postgres_config_no_ssl)
    postgres_config_hard_delete["hard_delete"] = True
    pg_hard_delete = TargetPostgres(config=postgres_config_hard_delete)
    singer_file_to_target(file_name, pg_hard_delete)


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


def test_postgres_ssh_tunnel(postgres_config_ssh_tunnel):
    """Test that using an ssh tunnel is successful."""
    tap = SampleTapCountries(config={}, state=None)

    target = TargetPostgres(config=postgres_config_ssh_tunnel)
    sync_end_to_end(tap, target)
