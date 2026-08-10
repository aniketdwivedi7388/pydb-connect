# Configuration

One file describes the *shape* of every connection your code uses. The
environment supplies whatever differs per machine. Secrets live in neither -
they live in a secret store, referenced by name.

## Precedence

Four layers. Higher wins.

| # | Layer | Set by | Use it for |
|---|-------|--------|------------|
| 1 | Explicit keyword arguments | `connect("wh", database="/tmp/x.db")` | Tests, one-off overrides, debugging |
| 2 | Environment variables | `PYDB_WH_HOST=...` | Per-environment values, containers, CI |
| 3 | YAML file | `connections.yaml` | Shared, committed, secret-free description |
| 4 | Defaults | `defaults:` block, then library defaults | Pool and retry settings you want everywhere |

Worked example. Given this file:

```yaml
defaults:
  pool: {max_size: 5}
connections:
  warehouse:
    backend: postgres
    host: db.internal
    port: 5432
    database: analytics
```

and this environment:

```bash
export PYDB_WAREHOUSE_HOST=db-replica.internal
export PYDB_WAREHOUSE_POOL_MAX_SIZE=12
```

and this call:

```python
connect("warehouse", port=6432)
```

you get `host=db-replica.internal` (env beat the file), `port=6432` (code beat
the file), `database=analytics` (only the file set it) and `pool.max_size=12`
(env beat the defaults block).

A `None` keyword argument does **not** override anything. `connect("wh",
host=None)` leaves the file's host alone, so you can pass optional arguments
straight through from your own function signatures without erasing config.

## Where the file is found

In order, first hit wins:

1. The `config_path` argument, or `pydb --config PATH`.
2. `$PYDB_CONFIG_FILE` (or `$PYDB_CONFIG`).
3. `./connections.yaml`, `./connections.yml`
4. `./config/connections.yaml`, `./config/connections.yml`
5. `~/.config/pydbconnect/connections.yaml`
6. `/etc/pydbconnect/connections.yaml`

**No file at all is a valid state.** If nothing is found, configuration comes
entirely from the environment. That is the right setup for a container image
that should carry no environment-specific data.

## File reference

```yaml
version: 1                       # informational; reserved for future changes
default_profile: dev             # used when neither --profile nor $PYDB_PROFILE is set

defaults:                        # merged under every connection
  pool:
    max_size: 5
  retry:
    max_attempts: 3

connections:                     # visible in every profile
  local:
    backend: sqlite
    database: ./local.db

profiles:
  prod:
    defaults:                    # profile-level defaults, merged over the global ones
      pool: {max_size: 20}
    connections:
      warehouse:
        backend: postgres
        host: ${env:PGHOST}
        database: analytics
        user: etl_writer
        secret: env:PGPASSWORD
```

### Connection keys

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `backend` | string | **required** | Registered backend: `sqlite`, `postgres`, `mysql`, `oracle`, `snowflake`, `adls` |
| `host` | string | `null` | Server hostname. Unused by `sqlite`; the storage account for `adls` |
| `port` | int | backend default | 1-65535 |
| `database` | string | `null` | Database name; file path for `sqlite`; container for `adls` |
| `schema` | string | `null` | Default schema, applied at connect time where supported |
| `user` | string | `null` | Login name |
| `secret` | string | `null` | A *reference* to the password, e.g. `env:PGPASSWORD`. See [secrets.md](secrets.md) |
| `options` | mapping | `{}` | Passed through to the driver |
| `sql_guard` | `warn`/`error`/`off` | `warn` | What to do about string-interpolated SQL |
| `pool` | mapping | see below | Pool settings |
| `retry` | mapping | see below | Retry settings |

**`password` is not a valid key.** Using it raises a `ConfigurationError` that
tells you to use `secret` instead. That is deliberate: a config file containing
a password is the exact failure this library exists to prevent.

### `pool`

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `min_size` | int | `0` | Connections opened eagerly at pool creation |
| `max_size` | int | `5` | Hard ceiling. Must be >= `min_size` |
| `timeout` | float | `30.0` | Seconds to wait for a free connection before `PoolTimeout` |
| `recycle` | float | `0.0` | Close connections older than this. `0` disables |
| `pre_ping` | bool | `true` | Liveness check on checkout: one round trip, no mystery failures |

Set `recycle` *below* any idle timeout between you and the database - a load
balancer, a connection proxy, a NAT gateway, a firewall. Silently dropped idle
connections are the single most common source of "it works, then it doesn't
until I restart".

### `retry`

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `max_attempts` | int | `3` | Total attempts including the first. `1` disables retry |
| `initial_backoff` | float | `0.2` | Seconds before the second attempt |
| `max_backoff` | float | `10.0` | Ceiling on any single wait |
| `multiplier` | float | `2.0` | Growth factor per attempt |
| `max_elapsed` | float | `60.0` | Give up after this long regardless of attempts. `0` disables |
| `jitter` | `full`/`none` | `full` | Full jitter spreads a retry storm; use it |

`max_elapsed` is the knob most people forget. Without it, `max_attempts: 10`
with `max_backoff: 60` can block a job for eight minutes on a connection that
was never coming back.

### `options`

Passed straight to the driver, so anything the driver accepts works without this
library needing to know about it.

```yaml
options:
  sslmode: require                 # postgres
  application_name: nightly-load   # postgres, shows in pg_stat_activity
  charset: utf8mb4                 # mysql
  warehouse: LOAD_WH               # snowflake
  service_name: ORCLPDB1           # oracle
  journal_mode: WAL                # sqlite
```

## Environment variables

The connection name is upper-cased with non-alphanumeric characters replaced by
`_`: `order-db` becomes `PYDB_ORDER_DB_*`.

| Variable | Maps to |
|----------|---------|
| `PYDB_<CONN>_BACKEND` | `backend` |
| `PYDB_<CONN>_HOST` | `host` |
| `PYDB_<CONN>_PORT` | `port` |
| `PYDB_<CONN>_DATABASE` | `database` |
| `PYDB_<CONN>_SCHEMA` | `schema` |
| `PYDB_<CONN>_USER` | `user` |
| `PYDB_<CONN>_SECRET` | `secret` (a reference) |
| `PYDB_<CONN>_PASSWORD` | sets `secret` to `env:PYDB_<CONN>_PASSWORD` |
| `PYDB_<CONN>_SQL_GUARD` | `sql_guard` |
| `PYDB_<CONN>_POOL_MAX_SIZE` | `pool.max_size` (same for `MIN_SIZE`, `TIMEOUT`, `RECYCLE`, `PRE_PING`) |
| `PYDB_<CONN>_RETRY_MAX_ATTEMPTS` | `retry.max_attempts` (same for `INITIAL_BACKOFF`, `MAX_BACKOFF`, `MULTIPLIER`, `MAX_ELAPSED`, `JITTER`) |
| `PYDB_<CONN>_OPTIONS` | `options`, as a JSON object |
| `PYDB_<CONN>_OPTION_<KEY>` | one entry in `options`, key lower-cased |
| `PYDB_CONFIG_FILE` | path to the configuration file |
| `PYDB_PROFILE` | active profile |

`PYDB_<CONN>_PASSWORD` is a convenience that keeps the value out of the config
object: it sets `secret` to a *reference* to itself, so the password is read
through the normal resolver and never appears in a `repr` or a `to_dict`.

A whole connection can be defined with nothing but environment variables:

```bash
export PYDB_EVENTS_BACKEND=postgres
export PYDB_EVENTS_HOST=db.internal
export PYDB_EVENTS_DATABASE=events
export PYDB_EVENTS_USER=reader
export PYDB_EVENTS_PASSWORD="$(cat /run/secrets/db)"
python -c "from pydbconnect import connect; print(connect('events').query('select 1'))"
```

## Placeholders

Any string value may contain a placeholder, resolved when the file is loaded.

| Syntax | Resolves to |
|--------|-------------|
| `${env:VAR}` | The environment variable. Errors if unset |
| `${env:VAR:-fallback}` | The variable, or `fallback` if unset |
| `${file:/path}` | Contents of a file, trailing newline stripped |
| `${vault:myvault/secret-name}` | Azure Key Vault secret |
| `${keyvault:myvault/secret-name}` | Same as `vault:` |

```yaml
connections:
  warehouse:
    backend: postgres
    host: ${env:PGHOST:-localhost}
    database: analytics_${env:DEPLOY_ENV:-dev}
    options:
      sslrootcert: ${env:HOME}/.postgresql/root.crt
```

Values from `file:` and `vault:` are registered with the redaction table, so
they are scrubbed from logs and error messages. `env:` values are not, because
`${env:AWS_REGION}` is not a secret.

Use `strict_placeholders=False` to leave unresolvable placeholders in place -
this is what lets `pydb config list` run on a laptop without production
credentials.

## Profiles

One file, many environments. A connection defined under `profiles:` overrides
the same name under the top-level `connections:`, merged key by key.

```yaml
connections:
  warehouse:                 # shared shape
    backend: postgres
    database: analytics
    user: etl_writer
    secret: env:PGPASSWORD
    pool: {max_size: 5}

profiles:
  dev:
    connections:
      warehouse:
        backend: sqlite      # dev overrides the backend entirely
        database: ./dev.db
  prod:
    connections:
      warehouse:
        host: db-prod.internal
        pool: {max_size: 25} # merged, so user/secret/database still apply
```

Select a profile with `--profile prod`, `$PYDB_PROFILE`, `default_profile:` in
the file, or `connect("warehouse", profile="prod")` - in that order of
precedence.

## Validation

```bash
pydb config validate            # every connection
pydb config validate warehouse  # one connection
pydb config list                # what is defined, redacted
pydb config list --backends     # which drivers are installed here
```

`pydb config validate` exits `2` if anything is invalid, so it can gate a
deployment:

```yaml
- name: Validate database configuration
  run: pydb config validate --profile prod
```

Validation checks types and ranges, that the backend is registered, that its
required fields are set, and that the secret reference uses a known scheme. It
does **not** connect or resolve secrets, so it is safe to run anywhere.

## Troubleshooting

| Message | Cause | Fix |
|---------|-------|-----|
| `no connection named 'x' in ...; defined connections: a, b` | Typo, or wrong profile | Check `pydb config list`; check `--profile` |
| `unknown backend 'postgrez'; registered backends: ...` | Typo in `backend:` | Use a name from the list in the message |
| `'password' is not a valid key` | Password in the config file | Move it to a secret store, put `secret: env:VAR` in the file |
| `backend 'postgres' requires 'host' to be set` | Missing required field | Set it in the file or via `PYDB_<CONN>_HOST` |
| `unknown configuration key(s): hostname` | Misspelled key | The message lists every valid key |
| `unknown key(s) under 'pool': maxsize` | Misspelled nested key | It is `max_size` |
| `placeholder ${env:X} could not be resolved` | Variable not exported | Export it, or use `${env:X:-default}` |
| `profile 'staging' is not defined; available profiles: dev, prod` | Typo in `--profile` | Use a listed profile, or add it to the file |
| `environment variable 'PGPASSWORD' is not set` | Secret reference points nowhere | Export it, or point `secret:` at `file:` / `keyvault:` |
| `configuration file not found: ...` | Wrong `--config` path | Check the path; remember the search order above |
| `invalid YAML in ...` | Syntax error | The message includes the line and column from PyYAML |
| `... must contain a mapping at the top level` | File starts with a list | The top level is a mapping of `connections:`, `profiles:`, `defaults:` |
| `no connection available from pool 'x' within 30s` | More concurrency than pool slots | Raise `pool.max_size`, or run fewer workers |
| `PyYAML is not installed and the file is not valid JSON` | Optional dependency missing | `pip install pyyaml`, or write the file as JSON |

## Programmatic use

```python
from pydbconnect import load_config, ConnectionConfig, connect

# Inspect without connecting
config_file = load_config("config/connections.yaml", profile="prod")
print(config_file.names())                  # ['lake', 'warehouse']
config = config_file.get("warehouse")       # validated ConnectionConfig
print(config)                               # repr is redacted

# Build one by hand - no file involved
config = ConnectionConfig(
    name="scratch", backend="sqlite", database=":memory:",
).validate()
with connect(config) as conn:
    conn.query("SELECT 1")
```
