"""Test custom types and the type hierarchy."""

import pytest
from sqlalchemy import types
from sqlalchemy.dialects.postgresql import BIGINT, SMALLINT

from target_postgres.connector import NOTYPE, JSONSchemaToPostgres, PostgresConnector


@pytest.fixture
def connector():
    """Create a PostgresConnector instance."""
    return PostgresConnector(
        config={
            "dialect+driver": "postgresql+psycopg",
            "host": "localhost",
            "port": "5432",
            "user": "postgres",
            "password": "postgres",
            "database": "postgres",
            "ssl_enable": False,
            "ssl_client_certificate_enable": False,
        },
    )


@pytest.mark.parametrize(
    ("types", "expected"),
    [
        pytest.param(
            [types.Integer(), types.String()],
            types.String,
            id="int+str=str",
        ),
        pytest.param(
            [types.Boolean(), types.String()],
            types.String,
            id="bool+str=str",
        ),
        pytest.param(
            [types.Integer(), types.DateTime()],
            types.Integer,
            id="int+datetime=int",
        ),
        pytest.param(
            [NOTYPE(), types.String()],
            types.String,
            id="none+str=str",
        ),
        pytest.param(
            [NOTYPE(), types.Integer()],
            NOTYPE,
            id="none+int=none",
        ),
    ],
)
def test_type_hierarchy(connector, types, expected):
    """Test that types are merged correctly."""
    assert type(connector.merge_sql_types(types)) is expected


class TestJSONSchemaToPostgres:
    """Test JSONSchemaToPostgres class."""

    @pytest.fixture
    def to_postgres(self, connector: PostgresConnector):
        """Create a JSONSchemaToPostgres instance."""
        return connector.jsonschema_to_sql

    def test_datetime_string(self, to_postgres: JSONSchemaToPostgres):
        """Test conversion of JSON schema string to Postgres datetime."""
        result = to_postgres.to_sql_type({"type": "string", "format": "date-time"})
        assert type(result) is types.TIMESTAMP

    @pytest.mark.parametrize(
        ("jsonschema", "expected"),
        [
            pytest.param({"type": "integer"}, BIGINT, id="default"),
            pytest.param({"type": ["integer", "null"]}, BIGINT, id="default-nullable"),
            pytest.param(
                {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 2**15 - 1,
                },
                SMALLINT,
                id="smallint",
            ),
            pytest.param(
                {
                    "type": "integer",
                    "minimum": -5,
                    "maximum": 5,
                },
                SMALLINT,
                id="negative-smallint",
            ),
            pytest.param(
                {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 2**31 - 1,
                },
                types.INTEGER,
                id="integer",
            ),
            pytest.param(
                {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 2**31 + 1,
                },
                BIGINT,
                id="bigint",
            ),
            pytest.param(
                {
                    "type": "integer",
                    "x-sql-datatype": "smallint",
                },
                SMALLINT,
                id="x-sql-datatype-smallint",
            ),
        ],
    )
    def test_integers(
        self,
        to_postgres: JSONSchemaToPostgres,
        jsonschema: dict,
        expected: type[types.TypeEngine],
    ):
        """Test conversion of JSON schema types to Postgres types."""
        result = to_postgres.to_sql_type(jsonschema)
        assert type(result) is expected
