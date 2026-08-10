# config/

Connection definitions live here. Credentials do not.

## Getting started

```bash
cp connections.example.yaml connections.yaml
pydb config validate
pydb ping local
```

`connections.yaml` is in `.gitignore` because it will accumulate
machine-specific paths and hostnames. `connections.example.yaml` is committed
and is the file to keep current.

## What belongs in this directory

| Belongs here | Does not |
|--------------|----------|
| Hostnames, ports, database and schema names | Passwords, tokens, account keys |
| Usernames | Anything from a `.pem`, `.p8` or `.key` file |
| Pool and retry tuning | SAS tokens, connection strings containing `AccountKey=` |
| Secret *references* (`secret: env:PGPASSWORD`) | Secret *values* (`secret: literal:hunter2`) |
| Driver options (`sslmode`, `charset`, `warehouse`) | `.env` files |

The `password:` key does not exist. Using it raises a `ConfigurationError`
pointing at `secret:` instead. That is deliberate.

## Where the file is found

In order, first hit wins:

1. `pydb --config PATH`, or `connect(..., config_path=...)`
2. `$PYDB_CONFIG_FILE`
3. `./connections.yaml`, `./connections.yml`
4. `./config/connections.yaml`, `./config/connections.yml`
5. `~/.config/pydbconnect/connections.yaml`
6. `/etc/pydbconnect/connections.yaml`

No file at all is valid - everything can come from `PYDB_*` environment
variables instead, which is the right setup for a container image that should
carry no environment-specific data.

## Profiles

`dev`, `test` and `prod` in one file. Select with `--profile prod` or
`$PYDB_PROFILE`; the file's `default_profile:` applies when neither is set.

```bash
pydb config list                    # default profile
pydb config list --profile prod     # what prod would resolve to, redacted
pydb config validate --profile prod # fails with exit code 2 if anything is wrong
```

`pydb config validate` connects to nothing and resolves no secrets, so it is
safe to run in CI as a deployment gate.

## Checking your work

```bash
pydb config list             # redacted view of every connection
pydb config list --backends  # which drivers are installed on this machine
pydb ping local              # actually open a connection
```

See [../docs/configuration.md](../docs/configuration.md) for the full key
reference and [../docs/secrets.md](../docs/secrets.md) for credential handling.
