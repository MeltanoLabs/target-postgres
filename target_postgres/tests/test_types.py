"""Test custom types and the type hierarchy."""

import pytest
from pgvector.sqlalchemy import VECTOR
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

    @pytest.fixture
    def base_vector_schema(self):
        """Create a base vector schema."""
        return {
            "type": "array",
            "items": {
                "type": "number",
            },
            "x-sql-datatype": "pgvector",
        }

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
            pytest.param(
                {"type": ["integer", "null"], "minimum": 0, "maximum": 2**15 - 1},
                SMALLINT,
                id="nullable-smallint-preserves-constraints",
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

    @pytest.mark.parametrize(
        ("jsonschema", "expected"),
        [
            pytest.param({"x-sql-datatype": "smallint"}, SMALLINT, id="smallint"),
            pytest.param({"x-sql-datatype": "integer"}, types.INTEGER, id="integer"),
            pytest.param({"x-sql-datatype": "bigint"}, BIGINT, id="bigint"),
        ],
    )
    def test_x_sql_datatype_without_type(
        self,
        to_postgres: JSONSchemaToPostgres,
        jsonschema: dict,
        expected: type[types.TypeEngine],
    ):
        """Test x-sql-datatype resolves correctly even when no type field is present."""
        result = to_postgres.to_sql_type(jsonschema)
        assert type(result) is expected

    @pytest.mark.parametrize(
        ("jsonschema", "expected"),
        [
            pytest.param(
                {"type": ["string", "integer"]}, types.TEXT, id="str+int=text"
            ),
            pytest.param(
                {"type": ["boolean", "string"]}, types.TEXT, id="bool+str=text"
            ),
        ],
    )
    def test_union_types(
        self,
        to_postgres: JSONSchemaToPostgres,
        jsonschema: dict,
        expected: type[types.TypeEngine],
    ):
        """Test union types (multiple non-null) resolve via pick_best_sql_type."""
        result = to_postgres.to_sql_type(jsonschema)
        assert type(result) is expected

    @pytest.mark.parametrize(
        ("jsonschema", "expected"),
        [
            pytest.param(
                {
                    "anyOf": [
                        {"type": "string", "format": "date-time"},
                        {"type": "null"},
                    ]
                },
                types.TIMESTAMP,
                id="nullable-datetime",
            ),
            pytest.param(
                {"anyOf": [{"type": "string"}, {"type": "null"}]},
                types.TEXT,
                id="nullable-string",
            ),
        ],
    )
    def test_any_of(
        self,
        to_postgres: JSONSchemaToPostgres,
        jsonschema: dict,
        expected: type[types.TypeEngine],
    ):
        """Test anyOf nullable types resolve correctly."""
        result = to_postgres.to_sql_type(jsonschema)
        assert type(result) is expected

    def test_fallback_to_notype(self, to_postgres: JSONSchemaToPostgres):
        """Test that a schema with no type or anyOf falls back to NOTYPE."""
        result = to_postgres.to_sql_type({})
        assert type(result) is NOTYPE

    def test_nullable_string_preserves_format(self, to_postgres: JSONSchemaToPostgres):
        """Test that nullable string unions preserve the format context."""
        result = to_postgres.to_sql_type(
            {"type": ["string", "null"], "format": "date-time"}
        )
        assert type(result) is types.TIMESTAMP

    def test_vector_no_dimension(
        self,
        to_postgres: JSONSchemaToPostgres,
        base_vector_schema: dict,
        subtests: pytest.Subtests,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Test conversion of to pgvector without dimension specified."""
        result = to_postgres.to_sql_type({**base_vector_schema})
        assert isinstance(result, VECTOR)
        assert result.dim is None

    @pytest.mark.xfail(reason="Dimension is not supported yet.", strict=True)
    def test_vector_with_dimension(
        self,
        to_postgres: JSONSchemaToPostgres,
        base_vector_schema: dict,
        subtests: pytest.Subtests,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Test conversion of to pgvector with dimension specified."""
        dim = 10
        result = to_postgres.to_sql_type(
            {
                **base_vector_schema,
                "x-sql-datatype-properties": {"dim": dim},
            }
        )
        assert isinstance(result, VECTOR)
        assert result.dim == dim

    def test_vector_library_not_available(
        self,
        to_postgres: JSONSchemaToPostgres,
        base_vector_schema: dict,
        subtests: pytest.Subtests,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Test conversion of to pgvector with library not available."""
        monkeypatch.setattr("target_postgres.connector.PGVECTOR_AVAILABLE", False)
        result = to_postgres.to_sql_type({**base_vector_schema})
        assert isinstance(result, types.ARRAY)
        assert isinstance(result.item_type, types.INTEGER)
