# `target-postgres`

![PyPI - Version](https://img.shields.io/pypi/v/meltanolabs-target-postgres)
![PyPI - Downloads](https://img.shields.io/pypi/dm/meltanolabs-target-postgres)
![PyPI - License](https://img.shields.io/pypi/l/meltanolabs-target-postgres)
![Test target-postgres](https://github.com/meltanolabs/target-postgres/actions/workflows/ci_workflow.yml/badge.svg)

Target for Postgres.

Built with the [Meltano SDK](https://sdk.meltano.com) for Singer Taps and Targets.

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
| ssh_tunnel                   | False    | None    | SSH Tunnel Configuration, this is a json object |
| ssh_tunnel.enable   | True (if ssh_tunnel set) | False   | Enable an ssh tunnel (also known as bastion host), see the other ssh_tunnel.* properties for more details.
| ssh_tunnel.host | True (if ssh_tunnel set) | False   | Host of the bastion host, this is the host we'll connect to via ssh
| ssh_tunnel.username | True (if ssh_tunnel set) | False   |Username to connect to bastion host
| ssh_tunnel.port | True (if ssh_tunnel set) | 22 | Port to connect to bastion host
| ssh_tunnel.private_key | True (if ssh_tunnel set) | None | Private Key for authentication to the bastion host
| ssh_tunnel.private_key_password | False | None | Private Key Password, leave None if no password is set
| ssl_enable                   | False    |       0 | Whether or not to use ssl to verify the server's identity. Use ssl_certificate_authority and ssl_mode for further customization. To use a client certificate to authenticate yourself to the server, use ssl_client_certificate_enable instead. Note if sqlalchemy_url is set this will be ignored. |
| ssl_client_certificate_enable| False    |       0 | Whether or not to provide client-side certificates as a method of authentication to the server. Use ssl_client_certificate and ssl_client_private_key for further customization. To use SSL to verify the server's identity, use ssl_enable instead. Note if sqlalchemy_url is set this will be ignored. |
| ssl_mode                     | False    | verify-full | SSL Protection method, see [postgres documentation](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION) for more information. Must be one of disable, allow, prefer, require, verify-ca, or verify-full. Note if sqlalchemy_url is set this will be ignored. |
| ssl_certificate_authority    | False    | ~/.postgresql/root.crl | The certificate authority that should be used to verify the server's identity. Can be provided either as the certificate itself (in .env) or as a filepath to the certificate. Note if sqlalchemy_url is set this will be ignored. |
| ssl_client_certificate       | False    | ~/.postgresql/postgresql.crt | The certificate that should be used to verify your identity to the server. Can be provided either as the certificate itself (in .env) or as a filepath to the certificate. Note if sqlalchemy_url is set this will be ignored. |
| ssl_client_private_key       | False    | ~/.postgresql/postgresql.key | The private key for the certificate you provided. Can be provided either as the certificate itself (in .env) or as a filepath to the certificate. Note if sqlalchemy_url is set this will be ignored. |
| ssl_storage_directory        | False    | .secrets | The folder in which to store SSL certificates provided as raw values. When a certificate/key is provided as a raw value instead of as a filepath, it must be written to a file before it can be used. This configuration option determines where that file is created. |
| stream_maps                  | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config            | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled           | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth         | False    | None    | The max depth to flatten schemas. |

A full list of supported settings and capabilities is available by running: `target-postgres --about`

## Installation

```bash
pipx install meltanolabs-target-postgres
```

## Configuration

### An Explanation of Various SSL Configuration Options

There are two distinct processes which both fall under the banner of SSL. One process occurs when the client wishes to ensure the identity of the server, and is the more common reason that SSL is used. Another is when the server wishes to ensure the identity of the client, for authentication/authorization purposes.

If your server is set up with a certificate and private key, and you wish to check their certificate against a root certificate which you posess, use `ssl_enable`. You may then further customize this process using the `ssl_certificate_authority` and `ssl_mode` settings. See the [documentation](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBQ-SSL-CERTIFICATES) for further details.

If your server is set up with a root certificate, and you wish to provide a certificate to the server to verify your identity, use `ssl_client_certificate_enable`. You may then further customize this process using the `ssl_client_certificate` and `ssl_client_private_key` settings. See the [documentation](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-CLIENTCERT) for further details.

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

### Using Docker Compose

`docker-compose.yml` provides the commands to create two empty sample databases using [Docker](https://docs.docker.com/engine/install/). These can be a starting point to create your own database running in Docker, or can be used to run the tap's [built-in tests](#create-and-run-tests).

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
