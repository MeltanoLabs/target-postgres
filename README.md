# target-postgres

![PyPI - Version](https://img.shields.io/pypi/v/meltanolabs-target-postgres)
![PyPI - Downloads](https://img.shields.io/pypi/dm/meltanolabs-target-postgres)
![PyPI - License](https://img.shields.io/pypi/l/meltanolabs-target-postgres)
![Test target-postgres](https://github.com/meltanolabs/target-postgres/actions/workflows/ci_workflow.yml/badge.svg)

Singer Target for PostgreSQL databases.

Built with the [Meltano SDK](https://sdk.meltano.com) for Singer Taps and Targets.

## Capabilities

* `about`
* `stream-maps`
* `schema-flattening`
* `validate-records`
* `target-schema`
* `hard-delete`

## Supported Python and PostgreSQL Versions

This target is tested with all actively supported [Python](https://devguide.python.org/versions/#supported-versions) and [PostgreSQL](https://www.postgresql.org/support/versioning/) versions. At the time of writing, this includes Python 3.9 through 3.13 and PostgreSQL 12 through 17.

## Settings

| Setting                         | Required | Default                       | Description                                                                                                                                                                                                                                                                                              |
| :------------------------------ | :------- | :---------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| host                            | False    | None                          | Hostname for postgres instance. Note if sqlalchemy_url is set this will be ignored.                                                                                                                                                                                                                      |
| port                            | False    | 5432                          | The port on which postgres is awaiting connection. Note if sqlalchemy_url is set this will be ignored.                                                                                                                                                                                                   |
| user                            | False    | None                          | User name used to authenticate. Note if sqlalchemy_url is set this will be ignored.                                                                                                                                                                                                                      |
| password                        | False    | None                          | Password used to authenticate. Note if sqlalchemy_url is set this will be ignored.                                                                                                                                                                                                                       |
| database                        | False    | None                          | Database name. Note if sqlalchemy_url is set this will be ignored.                                                                                                                                                                                                                                       |
| sqlalchemy_url                  | False    | None                          | SQLAlchemy connection string. This will override using host, user, password, port, dialect, and all ssl settings. Note that you must escape password special characters properly. See https://docs.sqlalchemy.org/en/20/core/engines.html#escaping-special-characters-such-as-signs-in-passwords         |
| dialect+driver                  | False    | postgresql+psycopg2           | Dialect+driver see https://docs.sqlalchemy.org/en/20/core/engines.html. Generally just leave this alone. Note if sqlalchemy_url is set this will be ignored.                                                                                                                                             |
| default_target_schema           | False    | melty                         | Postgres schema to send data to, example: tap-clickup                                                                                                                                                                                                                                                    |
| activate_version                | False    | 1                             | If set to false, the tap will ignore activate version messages. If set to true, add_record_metadata must be set to true as well.                                                                                                                                                                         |
| hard_delete                     | False    | 0                             | When activate version is sent from a tap this specefies if we should delete the records that don't match, or mark them with a date in the `_sdc_deleted_at` column. This config option is ignored if `activate_version` is set to false.                                                                 |
| add_record_metadata             | False    | 1                             | Note that this must be enabled for activate_version to work!This adds _sdc_extracted_at, _sdc_batched_at, and more to every table. See https://sdk.meltano.com/en/latest/implementation/record_metadata.html for more information.                                                                       |
| interpret_content_encoding      | False    | 0                             | If set to true, the target will interpret the content encoding of the schema to determine how to store the data. Using this option may result in a more efficient storage of the data but may also result in an error if the data is not encoded as expected.                                            |
| ssl_enable                      | False    | 0                             | Whether or not to use ssl to verify the server's identity. Use ssl_certificate_authority and ssl_mode for further customization. To use a client certificate to authenticate yourself to the server, use ssl_client_certificate_enable instead. Note if sqlalchemy_url is set this will be ignored.      |
| ssl_client_certificate_enable   | False    | 0                             | Whether or not to provide client-side certificates as a method of authentication to the server. Use ssl_client_certificate and ssl_client_private_key for further customization. To use SSL to verify the server's identity, use ssl_enable instead. Note if sqlalchemy_url is set this will be ignored. |
| ssl_mode                        | False    | verify-full                   | SSL Protection method, see [postgres documentation](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION) for more information. Must be one of disable, allow, prefer, require, verify-ca, or verify-full. Note if sqlalchemy_url is set this will be ignored.                    |
| ssl_certificate_authority       | False    | ~/.postgresql/root.crl        | The certificate authority that should be used to verify the server's identity. Can be provided either as the certificate itself (in .env) or as a filepath to the certificate. Note if sqlalchemy_url is set this will be ignored.                                                                       |
| ssl_client_certificate          | False    | ~/.postgresql/postgresql.crt  | The certificate that should be used to verify your identity to the server. Can be provided either as the certificate itself (in .env) or as a filepath to the certificate. Note if sqlalchemy_url is set this will be ignored.                                                                           |
| ssl_client_private_key          | False    | ~/.postgresql/postgresql.key  | The private key for the certificate you provided. Can be provided either as the certificate itself (in .env) or as a filepath to the certificate. Note if sqlalchemy_url is set this will be ignored.                                                                                                    |
| ssl_storage_directory           | False    | .secrets                      | The folder in which to store SSL certificates provided as raw values. When a certificate/key is provided as a raw value instead of as a filepath, it must be written to a file before it can be used. This configuration option determines where that file is created.                                   |
| ssh_tunnel                      | False    | None                          | SSH Tunnel Configuration, this is a json object                                                                                                                                                                                                                                                          |
| ssh_tunnel.enable               | False    | 0                             | Enable an ssh tunnel (also known as bastion host), see the other ssh_tunnel.* properties for more details                                                                                                                                                                                                |
| ssh_tunnel.host                 | False    | None                          | Host of the bastion host, this is the host we'll connect to via ssh                                                                                                                                                                                                                                      |
| ssh_tunnel.username             | False    | None                          | Username to connect to bastion host                                                                                                                                                                                                                                                                      |
| ssh_tunnel.port                 | False    | 22                            | Port to connect to bastion host                                                                                                                                                                                                                                                                          |
| ssh_tunnel.private_key          | False    | None                          | Private Key for authentication to the bastion host                                                                                                                                                                                                                                                       |
| ssh_tunnel.private_key_password | False    | None                          | Private Key Password, leave None if no password is set                                                                                                                                                                                                                                                   |

A full list of supported settings and capabilities is available by running: `target-postgres --about`

### Built-in Settings

The following settings are automatically supported by the Meltano SDK and inherited by this target.

| Setting                         | Required | Default                       | Description                                                                                                                                                                                                                                                                                              |
| :------------------------------ | :------- | :---------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| load_method                     | False    | TargetLoadMethods.APPEND_ONLY | The method to use when loading data into the destination. `append-only` will always write all input records whether that records already exists or not. `upsert` will update existing records and insert new records. `overwrite` will delete all existing records and insert all input records.         |
| batch_size_rows                 | False    | None                          | Maximum number of rows in each batch.                                                                                                                                                                                                                                                                    |
| validate_records                | False    | 1                             | Whether to validate the schema of the incoming streams.                                                                                                                                                                                                                                                  |
| stream_maps                     | False    | None                          | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html).                                                                                                                                                              |
| stream_map_config               | False    | None                          | User-defined config values to be used within map expressions.                                                                                                                                                                                                                                            |
| faker_config                    | False    | None                          | Config for the [`Faker`](https://faker.readthedocs.io/en/master/) instance variable `fake` used within map expressions. Only applicable if the plugin specifies `faker` as an addtional dependency (through the `singer-sdk` `faker` extra or directly).                                                 |
| faker_config.seed               | False    | None                          | Value to seed the Faker generator for deterministic output: https://faker.readthedocs.io/en/master/#seeding-the-generator                                                                                                                                                                                |
| faker_config.locale             | False    | None                          | One or more LCID locale strings to produce localized output for: https://faker.readthedocs.io/en/master/#localization                                                                                                                                                                                    |
| flattening_enabled              | False    | None                          | 'True' to enable schema flattening and automatically expand nested properties.                                                                                                                                                                                                                           |
| flattening_max_depth            | False    | None                          | The max depth to flatten schemas.                                                                                                                                                                                                                                                                        |

#### Note on generating fake data

If you need access to the `faker` instance in your stream map expressions, you will need to install it as an additional dependency in your plugin.

If you're using [Meltano](https://docs.meltano.com/), you can add the `faker` extra to your `meltano.yml` as follows:

1. If you're installing the plugin from PyPI:

  ```yaml
  pip_url: "meltanolabs-target-postgres[faker]==<version>"
  ```

2. If you're installing the plugin from the Git repository:

  ```yaml
  # Note the nested quotes
  pip_url: "'meltanolabs-target-postgres[faker] @ git+https://github.com/MeltanoLabs/target-postgres.git@<ref>'"
  ```

## Installation

### Using [`pipx`](https://github.com/pypa/pipx/)

```bash
pipx install meltanolabs-target-postgres
```

### Using [`uv`](https://docs.astral.sh/uv/)

```bash
uv tool install meltanolabs-target-postgres
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

### Setting Up SSL Files

We have set the provided keys in the .ssl directory to be valid for multiple centuries. However, we have also provided configuration instructions below to create all of the necessary files for testing SSL.

A list of each file and its purpose:
1. `ca.crt`: CA for client's certificate (stored on the server)
1. `cert.crt`: Client's certificate (stored on the client)
1. `pkey.key`: Client's private key (stored on the client)
1. `public_pkey.key`: Client's private key with incorrect file permissions (stored on the client)
1. `root.crt`: CA for server's certificate (stored on the client)
1. `server.crt`: Server's certificate (stored on the server)
1. `server.key`: Server's private key (stored on the server)

Run the following command to generate all relevant SSL files, with certificates valid for two centuries (73048 days).

```bash
openssl req -new -x509 -days 73048 -nodes -out ssl/server.crt -keyout ssl/server.key -subj "/CN=localhost" &&
openssl req -new -x509 -days 73048 -nodes -out ssl/cert.crt -keyout ssl/pkey.key -subj "/CN=postgres" &&
cp ssl/server.crt ssl/root.crt &&
cp ssl/cert.crt ssl/ca.crt &&
cp ssl/pkey.key ssl/public_pkey.key &&
chown 999:999 ssl/server.key &&
chmod 600 ssl/server.key &&
chmod 600 ssl/pkey.key &&
chmod 644 ssl/public_pkey.key
```

Now that all of the SSL files have been set up, you're ready to set up tests with pytest.

### Create and Run Tests

Start the test databases using Docker Compose:
```bash
docker-compose up -d
```

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

## Data Types

The below table shows how this tap will map between jsonschema datatypes and Postgres datatypes.

| jsonschema                     | Postgres                                |
|--------------------------------|-----------------------------------------|
| integer                        | bigint                                  |
| UNSUPPORTED                    | bigserial                               |
| UNSUPPORTED                    | bit [ (n) ]                             |
| UNSUPPORTED                    | bit varying [ (n) ]                     |
| boolean                        | boolean                                 |
| UNSUPPORTED                    | box                                     |
| string with contentEncoding="base16" ([opt-in feature](#content-encoding-support)) | bytea                                   |
| UNSUPPORTED                    | character [ (n) ]                       |
| UNSUPPORTED                    | character varying [ (n) ]               |
| UNSUPPORTED                    | cidr                                    |
| UNSUPPORTED                    | circle                                  |
| string with format="date"      | date                                    |
| UNSUPPORTED                    | double precision                        |
| UNSUPPORTED                    | inet                                    |
| UNSUPPORTED                    | integer                                 |
| UNSUPPORTED                    | interval [ fields ] [ (p) ]             |
| UNSUPPORTED                    | json                                    |
| array; object                  | jsonb                                   |
| UNSUPPORTED                    | line                                    |
| UNSUPPORTED                    | lseg                                    |
| UNSUPPORTED                    | macaddr                                 |
| UNSUPPORTED                    | macaddr8                                |
| UNSUPPORTED                    | money                                   |
| number                         | numeric [ (p, s) ]                      |
| UNSUPPORTED                    | path                                    |
| UNSUPPORTED                    | pg_lsn                                  |
| UNSUPPORTED                    | pg_snapshot                             |
| UNSUPPORTED                    | point                                   |
| UNSUPPORTED                    | polygon                                 |
| UNSUPPORTED                    | real                                    |
| UNSUPPORTED                    | smallint                                |
| UNSUPPORTED                    | smallserial                             |
| UNSUPPORTED                    | serial                                  |
| string without format; untyped | text                                    |
| string with format="time"      | time [ (p) ] [ without time zone ]      |
| UNSUPPORTED                    | time [ (p) ] with time zone             |
| string with format="date-time" | timestamp [ (p) ] [ without time zone ] |
| UNSUPPORTED                    | timestamp [ (p) ] with time zone        |
| UNSUPPORTED                    | tsquery                                 |
| UNSUPPORTED                    | tsvector                                |
| UNSUPPORTED                    | txid_snapshot                           |
| string with format="uuid"      | uuid                                    |
| UNSUPPORTED                    | xml                                     |

Note that while object types are mapped directly to jsonb, array types are mapped to a jsonb array.

If a column has multiple jsonschema types, the following order is using to order Postgres types, from highest priority to lowest priority.
- BYTEA
- ARRAY(JSONB)
- JSONB
- TEXT
- TIMESTAMP
- DATETIME
- DATE
- TIME
- DECIMAL
- BIGINT
- INTEGER
- BOOLEAN
- NOTYPE

## Content Encoding Support

Json Schema supports the [`contentEncoding` keyword](https://datatracker.ietf.org/doc/html/rfc4648#section-8), which can be used to specify the encoding of input string types.

This target can detect content encoding clues in the schema to determine how to store the data in the postgres in a more efficient way.

Content encoding interpretation is disabled by default. This is because the default config is meant to be as permissive as possible, and do not make any assumptions about the data that could lead to data loss.

However if you know your data respects the advertised content encoding way, you can enable this feature to get better performance and storage efficiency.

To enable it, set the `interpret_content_encoding` option to `True`.

### base16

The string is encoded using the base16 encoding, as defined in [RFC 4648](https://json-schema.org/draft/2020-12/draft-bhutton-json-schema-validation-00#rfc.section.8.3
).

Example schema:
```json
{
  "type": "object",
  "properties": {
    "my_hex": {
      "type": "string",
      "contentEncoding": "base16"
    }
  }
}
```

Data will be stored as a `bytea` in the database.

Example data:
```json
# valid data
{ "my_hex": "01AF" }
{ "my_hex": "01af" }
{ "my_hex": "1af" }
{ "my_hex": "0x1234" }

# invalid data
{ "my_hex": " 0x1234 " }
{ "my_hex": "House" }
```

For convenience, data prefixed with `0x` or containing an odd number of characters is supported although it's not part of the standard.
