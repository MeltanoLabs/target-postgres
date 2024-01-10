""" Postgres target tests """
# flake8: noqa
import copy
import datetime
import io
import typing as t
from contextlib import redirect_stdout
from decimal import Decimal
from pathlib import Path

import jsonschema
import pytest
import sqlalchemy
from singer_sdk.exceptions import MissingKeyPropertiesError
from singer_sdk.testing import get_target_test_class, sync_end_to_end
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.types import BIGINT, TEXT, TIMESTAMP

from target_postgres.connector import PostgresConnector
from target_postgres.target import TargetPostgres
from target_postgres.tests.samples.aapl.aapl import Fundamentals
from target_postgres.tests.samples.sample_tap_countries.countries_tap import (
    SampleTapCountries,
)

from .core import (
    create_engine,
    postgres_config,
    postgres_config_no_ssl,
    postgres_config_ssh_tunnel,
)

METADATA_COLUMN_PREFIX = "_sdc"


# The below syntax is documented at https://docs.pytest.org/en/stable/deprecations.html#calling-fixtures-directly
@pytest.fixture(scope="session", name="postgres_config")
def postgres_config_fixture():
    return postgres_config()


@pytest.fixture(scope="session", name="postgres_config_no_ssl")
def postgres_config_no_ssl_fixture():
    return postgres_config_no_ssl()


@pytest.fixture(scope="session", name="postgres_config_ssh_tunnel")
def postgres_config_ssh_tunnel_fixture():
    return postgres_config_ssh_tunnel()


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
        with open(file_path) as f:
            for line in f:
                print(line.rstrip("\r\n"))  # File endings are here,
                # and print adds another line ending so we need to remove one.
    buf.seek(0)
    target.listen(buf)


# TODO should set schemas for each tap individually so we don't collide


class AssertionHelper:
    def __init__(self, target: TargetPostgres, metadata_column_prefix: str):
        self.target = target
        self.metadata_column_prefix = metadata_column_prefix

    def remove_metadata_columns(self, row: dict) -> dict:
        new_row = {}
        for column in row.keys():
            if not column.startswith(self.metadata_column_prefix):
                new_row[column] = row[column]
        return new_row

    def verify_data(
        self,
        table_name: str,
        number_of_rows: int = 1,
        primary_key: t.Union[str, None] = None,
        check_data: t.Union[t.Dict, t.List[t.Dict], None] = None,
    ):
        """Checks whether the data in a table matches a provided data sample.

        Args:
            target: The target to obtain a database connection from.
            table_name: The schema and table name of the table to check data for.
            primary_key: The primary key of the table.
            number_of_rows: The expected number of rows that should be in the table.
            check_data: A dictionary representing the full contents of the first row in the
                table, as determined by lowest primary_key value, or else a list of
                dictionaries representing every row in the table.
        """
        engine = create_engine(self.target)
        full_table_name = f"{self.target.config['default_target_schema']}.{table_name}"
        with engine.connect() as connection:
            if primary_key is not None and check_data is not None:
                if isinstance(check_data, dict):
                    result = connection.execute(
                        sqlalchemy.text(
                            f"SELECT * FROM {full_table_name} ORDER BY {primary_key}"
                        )
                    )
                    assert result.rowcount == number_of_rows
                    result_dict = self.remove_metadata_columns(result.first()._asdict())
                    assert result_dict == check_data
                elif isinstance(check_data, list):
                    result = connection.execute(
                        sqlalchemy.text(
                            f"SELECT * FROM {full_table_name} ORDER BY {primary_key}"
                        )
                    )
                    assert result.rowcount == number_of_rows
                    result_dict = [
                        self.remove_metadata_columns(row._asdict())
                        for row in result.all()
                    ]
                    assert result_dict == check_data
                else:
                    raise ValueError("Invalid check_data - not dict or list of dicts")
            else:
                result = connection.execute(
                    sqlalchemy.text(f"SELECT COUNT(*) FROM {full_table_name}")
                )
                assert result.first()[0] == number_of_rows
        engine.dispose()

    def verify_schema(
        self,
        table_name: str,
        check_columns: dict = None,
    ):
        """Checks whether the schema of a database table matches the provided column definitions.

        Args:
            target: The target to obtain a database connection from.
            table_name: The schema and table name of the table to check data for.
            check_columns: A dictionary mapping column names to their definitions. Currently,
                it is all about the `type` attribute which is compared.
            metadata_column_prefix: The prefix string for metadata columns. Usually `_sdc`.
        """
        engine = create_engine(self.target)
        schema = self.target.config["default_target_schema"]
        with engine.connect() as connection:
            meta = sqlalchemy.MetaData()
            table = sqlalchemy.Table(
                table_name, meta, schema=schema, autoload_with=connection
            )
            for column in table.c:
                # Ignore `_sdc` metadata columns when verifying table schema.
                if column.name.startswith(self.metadata_column_prefix):
                    continue
                try:
                    column_type_expected = check_columns[column.name]["type"]
                except KeyError:
                    raise ValueError(
                        f"Invalid check_columns - missing definition for column: {column.name}"
                    )
                if not isinstance(column.type, column_type_expected):
                    raise TypeError(
                        f"Column '{column.name}' (with type '{column.type}') "
                        f"does not match expected type: {column_type_expected}"
                    )
        engine.dispose()


@pytest.fixture
def helper(postgres_target) -> AssertionHelper:
    return AssertionHelper(
        target=postgres_target, metadata_column_prefix=METADATA_COLUMN_PREFIX
    )


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


def test_optional_attributes(postgres_target, helper):
    file_name = "optional_attributes.singer"
    singer_file_to_target(file_name, postgres_target)
    row = {"id": 1, "optional": "This is optional"}
    helper.verify_data("test_optional_attributes", 4, "id", row)


def test_schema_no_properties(postgres_target):
    """Expect to fail with ValueError"""
    file_name = "schema_no_properties.singer"
    singer_file_to_target(file_name, postgres_target)


# TODO test that data is correct
def test_large_numeric_primary_key(postgres_target):
    """Check that large numeric (jsonschema: number) pkeys don't cause failure.

    See: https://github.com/MeltanoLabs/target-postgres/issues/193
    """
    file_name = "large_numeric_primary_key.singer"
    singer_file_to_target(file_name, postgres_target)


# TODO test that data is correct
def test_schema_updates(postgres_target, helper):
    file_name = "schema_updates.singer"
    singer_file_to_target(file_name, postgres_target)
    row = {
        "id": 1,
        "a1": Decimal("101"),
        "a2": "string1",
        "a3": None,
        "a4": None,
        "a5": None,
        "a6": None,
    }
    helper.verify_data("test_schema_updates", 6, "id", row)


def test_multiple_state_messages(postgres_target, helper):
    file_name = "multiple_state_messages.singer"
    singer_file_to_target(file_name, postgres_target)
    row = {"id": 1, "metric": 100}
    helper.verify_data("test_multiple_state_messages_a", 6, "id", row)
    row = {"id": 1, "metric": 110}
    helper.verify_data("test_multiple_state_messages_b", 6, "id", row)


# TODO test that data is correct
def test_multiple_schema_messages(postgres_target, caplog):
    """Test multiple identical schema messages.

    Multiple schema messages with the same schema should not cause 'schema has changed'
    logging statements. See: https://github.com/MeltanoLabs/target-postgres/issues/124

    Caplog docs: https://docs.pytest.org/en/latest/how-to/logging.html#caplog-fixture
    """
    file_name = "multiple_schema_messages.singer"
    singer_file_to_target(file_name, postgres_target)
    assert "Schema has changed for stream" not in caplog.text


def test_relational_data(postgres_target, helper):
    file_name = "user_location_data.singer"
    singer_file_to_target(file_name, postgres_target)

    file_name = "user_location_upsert_data.singer"
    singer_file_to_target(file_name, postgres_target)

    users = [
        {"id": 1, "name": "Johny"},
        {"id": 2, "name": "George"},
        {"id": 3, "name": "Jacob"},
        {"id": 4, "name": "Josh"},
        {"id": 5, "name": "Jim"},
        {"id": 8, "name": "Thomas"},
        {"id": 12, "name": "Paul"},
        {"id": 13, "name": "Mary"},
    ]
    locations = [
        {"id": 1, "name": "Philly"},
        {"id": 2, "name": "NY"},
        {"id": 3, "name": "San Francisco"},
        {"id": 6, "name": "Colorado"},
        {"id": 8, "name": "Boston"},
    ]
    user_in_location = [
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

    helper.verify_data("test_users", 8, "id", users)
    helper.verify_data("test_locations", 5, "id", locations)
    helper.verify_data("test_user_in_location", 5, "id", user_in_location)


def test_no_primary_keys(postgres_target, helper):
    """We run both of these tests twice just to ensure that no records are removed and append only works properly"""
    engine = create_engine(postgres_target)
    table_name = "test_no_pk"
    full_table_name = postgres_target.config["default_target_schema"] + "." + table_name
    with engine.connect() as connection, connection.begin():
        connection.execute(sqlalchemy.text(f"DROP TABLE IF EXISTS {full_table_name}"))
    file_name = f"{table_name}.singer"
    singer_file_to_target(file_name, postgres_target)

    file_name = f"{table_name}_append.singer"
    singer_file_to_target(file_name, postgres_target)

    file_name = f"{table_name}.singer"
    singer_file_to_target(file_name, postgres_target)

    file_name = f"{table_name}_append.singer"
    singer_file_to_target(file_name, postgres_target)

    helper.verify_data(table_name, 16)


def test_no_type(postgres_target):
    file_name = "test_no_type.singer"
    singer_file_to_target(file_name, postgres_target)


def test_duplicate_records(postgres_target, helper):
    file_name = "duplicate_records.singer"
    singer_file_to_target(file_name, postgres_target)
    row = {"id": 1, "metric": 100}
    helper.verify_data("test_duplicate_records", 2, "id", row)


def test_array_boolean(postgres_target, helper):
    file_name = "array_boolean.singer"
    singer_file_to_target(file_name, postgres_target)
    row = {"id": 1, "value": [True, False]}
    helper.verify_data("array_boolean", 3, "id", row)
    helper.verify_schema(
        "array_boolean",
        check_columns={
            "id": {"type": BIGINT},
            "value": {"type": ARRAY},
        },
    )


def test_array_float_vector(postgres_target, helper):
    pgvector_sa = pytest.importorskip("pgvector.sqlalchemy")

    file_name = "array_float_vector.singer"
    singer_file_to_target(file_name, postgres_target)
    row = {
        "id": 1,
        "value": "[1.1,2.1,1.1,1.3]",
    }
    helper.verify_data("array_float_vector", 3, "id", row)

    helper.verify_schema(
        "array_float_vector",
        check_columns={
            "id": {"type": BIGINT},
            "value": {"type": pgvector_sa.Vector},
        },
    )


def test_array_number(postgres_target, helper):
    file_name = "array_number.singer"
    singer_file_to_target(file_name, postgres_target)
    row = {"id": 1, "value": [Decimal("42.42"), Decimal("84.84"), 23]}
    helper.verify_data("array_number", 3, "id", row)
    helper.verify_schema(
        "array_number",
        check_columns={
            "id": {"type": BIGINT},
            "value": {"type": ARRAY},
        },
    )


def test_array_string(postgres_target, helper):
    file_name = "array_string.singer"
    singer_file_to_target(file_name, postgres_target)
    row = {"id": 1, "value": ["apple", "orange", "pear"]}
    helper.verify_data("array_string", 4, "id", row)
    helper.verify_schema(
        "array_string",
        check_columns={
            "id": {"type": BIGINT},
            "value": {"type": ARRAY},
        },
    )


def test_array_timestamp(postgres_target, helper):
    file_name = "array_timestamp.singer"
    singer_file_to_target(file_name, postgres_target)
    row = {"id": 1, "value": ["2023-12-13T01:15:02", "2023-12-13T01:16:02"]}
    helper.verify_data("array_timestamp", 3, "id", row)
    helper.verify_schema(
        "array_timestamp",
        check_columns={
            "id": {"type": BIGINT},
            "value": {"type": ARRAY},
        },
    )


def test_object_mixed(postgres_target, helper):
    file_name = "object_mixed.singer"
    singer_file_to_target(file_name, postgres_target)
    row = {
        "id": 1,
        "value": {
            "string": "foo",
            "integer": 42,
            "float": Decimal("42.42"),
            "timestamp": "2023-12-13T01:15:02",
            "array_boolean": [True, False],
            "array_float": [Decimal("42.42"), Decimal("84.84")],
            "array_integer": [42, 84],
            "array_string": ["foo", "bar"],
            "nested_object": {"foo": "bar"},
        },
    }
    helper.verify_data("object_mixed", 1, "id", row)
    helper.verify_schema(
        "object_mixed",
        check_columns={
            "id": {"type": BIGINT},
            "value": {"type": JSONB},
        },
    )


def test_encoded_string_data(postgres_target, helper):
    """
    We removed NUL characters from the original encoded_strings.singer as postgres doesn't allow them.
    https://www.postgresql.org/docs/current/functions-string.html#:~:text=chr(0)%20is%20disallowed%20because%20text%20data%20types%20cannot%20store%20that%20character.
    chr(0) is disallowed because text data types cannot store that character.

    Note you will recieve a  ValueError: A string literal cannot contain NUL (0x00) characters. Which seems like a reasonable error.
    See issue https://github.com/MeltanoLabs/target-postgres/issues/60 for more details.
    """

    file_name = "encoded_strings.singer"
    singer_file_to_target(file_name, postgres_target)
    row = {"id": 1, "info": "simple string 2837"}
    helper.verify_data("test_strings", 11, "id", row)
    row = {"id": 1, "info": {"name": "simple", "value": "simple string 2837"}}
    helper.verify_data("test_strings_in_objects", 11, "id", row)
    row = {"id": 1, "strings": ["simple string", "απλή συμβολοσειρά", "简单的字串"]}
    helper.verify_data("test_strings_in_arrays", 6, "id", row)


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


def test_anyof(postgres_target, helper):
    """Test that anyOf is handled correctly"""
    table_name = "commits"
    file_name = f"{table_name}.singer"
    singer_file_to_target(file_name, postgres_target)

    helper.verify_schema(
        table_name,
        check_columns={
            # {"type":"string"}
            "id": {"type": TEXT},
            # Any of nullable date-time.
            # Note that postgres timestamp is equivalent to jsonschema date-time.
            # {"anyOf":[{"type":"string","format":"date-time"},{"type":"null"}]}
            "authored_date": {"type": TIMESTAMP},
            "committed_date": {"type": TIMESTAMP},
            # Any of nullable array of strings or single string.
            # {"anyOf":[{"type":"array","items":{"type":["null","string"]}},{"type":"string"},{"type":"null"}]}
            "parent_ids": {"type": ARRAY},
            # Any of nullable string.
            # {"anyOf":[{"type":"string"},{"type":"null"}]}
            "commit_message": {"type": TEXT},
            # Any of nullable string or integer.
            # {"anyOf":[{"type":"string"},{"type":"integer"},{"type":"null"}]}
            "legacy_id": {"type": TEXT},
        },
    )


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


def test_activate_version_soft_delete(postgres_config_no_ssl):
    """Activate Version Soft Delete Test"""
    table_name = "test_activate_version_soft"
    file_name = f"{table_name}.singer"
    full_table_name = postgres_config_no_ssl["default_target_schema"] + "." + table_name
    postgres_config_hard_delete_true = copy.deepcopy(postgres_config_no_ssl)
    postgres_config_hard_delete_true["hard_delete"] = False
    pg_soft_delete = TargetPostgres(config=postgres_config_hard_delete_true)
    engine = create_engine(pg_soft_delete)
    singer_file_to_target(file_name, pg_soft_delete)
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert result.rowcount == 7

    # Same file as above, but with South America (code=SA) record missing.
    file_name = f"{table_name}_with_delete.singer"
    south_america = {}

    singer_file_to_target(file_name, pg_soft_delete)
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert result.rowcount == 7
        result = connection.execute(
            sqlalchemy.text(f"SELECT * FROM {full_table_name} WHERE code='SA'")
        )
        south_america = result.first()._asdict()

    singer_file_to_target(file_name, pg_soft_delete)
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
                f"SELECT * FROM {full_table_name} where {METADATA_COLUMN_PREFIX}_deleted_at is NOT NULL"
            )
        )
        assert result.rowcount == 3  # 2 manual + 1 deleted (south america)

        result = connection.execute(
            sqlalchemy.text(f"SELECT * FROM {full_table_name} WHERE code='SA'")
        )
        # South America row should not have been modified, but it would have been prior
        # to the fix mentioned in #204 and implemented in #240.
        assert south_america == result.first()._asdict()


def test_activate_version_no_metadata(postgres_config_no_ssl):
    """Activate Version Test for if add_record_metadata is disabled"""
    postgres_config_modified = copy.deepcopy(postgres_config_no_ssl)
    postgres_config_modified["activate_version"] = True
    postgres_config_modified["add_record_metadata"] = False
    with pytest.raises(AssertionError):
        TargetPostgres(config=postgres_config_modified)


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
