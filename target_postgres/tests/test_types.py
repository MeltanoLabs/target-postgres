"""Test custom types and the type hierarchy."""

import pytest
import sqlalchemy as sa
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
        pytest.param([sa.Integer(), sa.String()], sa.String, id="int+str=str"),
        pytest.param([sa.Boolean(), sa.String()], sa.String, id="bool+str=str"),
        pytest.param([sa.Integer(), sa.DateTime()], sa.Integer, id="int+datetime=int"),
        pytest.param([NOTYPE(), sa.String()], sa.String, id="none+str=str"),
        pytest.param([NOTYPE(), sa.Integer()], NOTYPE, id="none+int=none"),
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
        assert type(result) is sa.TIMESTAMP

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
                sa.INTEGER,
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
        expected: type[sa.types.TypeEngine],
    ):
        """Test conversion of JSON schema types to Postgres types."""
        result = to_postgres.to_sql_type(jsonschema)
        assert type(result) is expected
