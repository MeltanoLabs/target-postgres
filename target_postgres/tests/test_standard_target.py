""" Attempt at making some standard Target Tests. """
import pytest
from pathlib import Path
import os
import io
from contextlib import redirect_stderr, redirect_stdout
from target_postgres.target import TargetPostgres
from target_postgres.tests.samples.sample_tap_countries.countries_tap import SampleTapCountries
from target_postgres.tests.samples.aapl.aapl import Fundamentals
from singer_sdk import Stream, Tap

from singer_sdk.testing import (
    sync_end_to_end,
    )


@pytest.fixture
def postgres_config():
    return {"sqlalchemy_url": "postgresql://postgres:postgres@localhost:5432/postgres"}

#TODO should set schemas for each tap individually so we don't collide
def test_countries_to_postgres(postgres_config):
    tap = SampleTapCountries(config={}, state=None)
    target = TargetPostgres(config=postgres_config)
    sync_end_to_end(tap, target)

def test_aapl_to_postgres(postgres_config):
    tap = Fundamentals(config={}, state=None)
    target = TargetPostgres(config=postgres_config)
    sync_end_to_end(tap, target)

def test_datafiles_to_postgres(postgres_config):
    """Redirect a bunch of .stream files singer output to Postgres

    Reads everything from folder_path, prints to stdout, 
    then sends that to Target-Postgres
    """
    buf = io.StringIO()
    samples_path = Path(__file__).parent / Path("./samples")
    folder_path=f"{samples_path}/data_files"
    with redirect_stdout(buf):
        #Every file in directory
        for file_name in [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path,f))]:
            file_path = f"{folder_path}/{file_name}"
            os.path.isfile(file_path)
            with open(file_path, 'r') as f:
                for line in f:
                    print(line.rstrip("\r\n"))
    buf.seek(0)
    target = TargetPostgres(config=postgres_config)
    target.listen(buf)
