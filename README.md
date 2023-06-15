# `target-postgres`

Target for Postgres.

Built with the [Meltano SDK](https://sdk.meltano.com) for Singer Taps and Targets. This target is in **development**, it probably doesn't work yet, stick with https://hub.meltano.com/loaders/target-postgres . Generally the goal here is to create a generalized target enough so that the SDK can automate >80% of testing for new targets, and potentially so taps can test very easily with a real local target.

# Limitations
1. Target is not working with Empty key properties. See https://github.com/MeltanoLabs/target-postgres/issues/54

## Capabilities

* `about`
* `stream-maps`
* `schema-flattening`

## Settings
| Setting                      | Required | Default | Description |
|:-----------------------------|:--------:|:-------:|:------------|
| host                         | False    | None    | Hostname for postgres instance. Note if sqlalchemy_url is set this will be ignored. |
| port                         | False    |    5432 | The port on which postgres is awaiting connection. Note if sqlalchemy_url is set this will be ignored. |
| user                         | False    | None    | User name used to authenticate. Note if sqlalchemy_url is set this will be ignored. |
| password                     | False    | None    | Password used to authenticate. Note if sqlalchemy_url is set this will be ignored. |
| database                     | False    | None    | Database name. Note if sqlalchemy_url is set this will be ignored. |
| sqlalchemy_url               | False    | None    | SQLAlchemy connection string. This will override using host, user, password, port, dialect, and all ssl settings. Note that you must escape password special characters properly. See https://docs.sqlalchemy.org/en/20/core/engines.html#escaping-special-characters-such-as-signs-in-passwords |
| dialect+driver               | False    | postgresql+psycopg2 | Dialect+driver see https://docs.sqlalchemy.org/en/20/core/engines.html. Generally just leave this alone. Note if sqlalchemy_url is set this will be ignored. |
| default_target_schema        | False    | None    | Postgres schema to send data to, example: tap-clickup |
| hard_delete                  | False    |       0 | When activate version is sent from a tap this specefies if we should delete the records that don't match, or mark them with a date in the `_sdc_deleted_at` column. |
| add_record_metadata          | False    |       1 | Note that this must be enabled for activate_version to work!This adds _sdc_extracted_at, _sdc_batched_at, and more to every table. See https://sdk.meltano.com/en/latest/implementation/record_metadata.html for more information. |
| ssl_enable                   | False    |       0 | Whether or not to use ssl to verify the server's identity. Use ssl_certificate_authority and ssl_mode for further customization. To use a client certificate to authenticate yourself to the server, use ssl_client_certificate_enable instead. Note if sqlalchemy_url is set this will be ignored. |
| ssl_client_certificate_enable| False    |       0 | Whether or not to provide client-side certificates as a method of authentication to the server. Use ssl_certificate and ssl_private_key for further customization. To use SSL to verify the server's identity, use ssl_enable instead. Note if sqlalchemy_url is set this will be ignored. |
| ssl_mode                     | False    | verify-full | SSL Protection method, see [postgres documentation](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION) for more information. Must be one of disable, allow, prefer, require, verify-ca, or verify-full. Note if sqlalchemy_url is set this will be ignored. |
| ssl_certificate_authority    | False    | None    | The certificate authority that should be used to verify the server's identity. Can be provided either as the certificate itself (in .env) or as a filepath to the certificate. Note if sqlalchemy_url is set this will be ignored. |
| ssl_certificate              | False    | None    | The certificate that should be used to verify your identity to the server. Can be provided either as the certificate itself (in .env) or as a filepath to the certificate. Note if sqlalchemy_url is set this will be ignored. |
| ssl_private_key              | False    | None    | The private key for the certificate you provided. Can be provided either as the certificate itself (in .env) or as a filepath to the certificate. Note if sqlalchemy_url is set this will be ignored. |
| stream_maps                  | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config            | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled           | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth         | False    | None    | The max depth to flatten schemas. |
A full list of supported settings and capabilities is available by running: `target-postgres --about`

## Installation

- [ ] `Developer TODO:` Come back to this re [#5](https://github.com/MeltanoLabs/target-postgres/issues/5)

```bash
pipx install -e .
```

## Configuration

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

The database account provided must have access to:
1. Create schemas
1. Create tables (DDL)
1. Push Data to tables (DML)

## Usage

You can easily run `target-postgres` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-postgres --version
target-postgres --help
# Test using the "Carbon Intensity" sample:
pipx install git+https://gitlab.com/meltano/tap-carbon-intensity
tap-carbon-intensity | target-postgres --config /path/to/target-postgres-config.json
```

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
pipx install pre-commit
pre-commit install
```

### Create and Run Tests

Create tests within the `target_postgres/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-postgres` CLI interface directly using `poetry run`:

```bash
poetry run target-postgres --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-postgres --version
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano SDK to
develop your own Singer taps and targets.
