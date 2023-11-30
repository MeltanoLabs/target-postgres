"""Test custom types and the type hierarchy."""

import pytest
from sqlalchemy import Boolean, DateTime, Integer, String

from target_postgres.connector import NOTYPE, PostgresConnector


@pytest.fixture
def connector():
    """Create a PostgresConnector instance."""
    return PostgresConnector(
        config={
            "dialect+driver": "postgresql+psycopg2",
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
        pytest.param([Integer(), String()], String, id="int+str=str"),
        pytest.param([Boolean(), String()], String, id="bool+str=str"),
        pytest.param([Integer(), DateTime()], Integer, id="int+datetime=int"),
        pytest.param([NOTYPE(), String()], String, id="none+str=str"),
        pytest.param([NOTYPE(), Integer()], NOTYPE, id="none+int=none"),
    ],
)
def test_type_hierarchy(connector, types, expected):
    """Test that types are merged correctly."""
    assert type(connector.merge_sql_types(types)) is expected
