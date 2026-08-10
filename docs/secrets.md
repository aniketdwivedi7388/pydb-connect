# Secrets

## The threat model

Start with what a committed password actually costs, because the answer is
usually worse than people assume.

**Git does not forget.** Deleting the line and pushing a fix does nothing. The
credential lives in the object store, in every clone, in every fork, in every
CI cache, in every laptop backup, and in whatever mirrors your organisation runs.
Removing it means rewriting history and force-pushing across every fork - and
then rotating anyway, because you cannot prove nobody fetched it.

**The blast radius is not the repo.** A service account password is rarely
scoped to the one table the job needed. It usually reads every schema in the
instance, and the same value is usually reused in three other places.

**Detection lags badly.** Credential misuse looks like normal traffic from a
service account that is *supposed* to run large queries at odd hours. Discovery
typically comes from an audit or from someone else's incident, months later.

**Rotation is the expensive part.** Every consumer of that credential has to
change at once, and nobody has a complete list, because the whole reason it was
in the file was that no one was tracking where it went.

Three practical rules follow:

1. **A secret is never a value in your repo.** It is a *reference* your code
   resolves at runtime.
2. **A secret is never printed.** Not in a log, not in a `repr`, not in an
   exception, not in a crash dump.
3. **A secret is rotatable without a code change.** If rotating means editing a
   file and redeploying, rotation will not happen on schedule.

## How this library handles it

Configuration holds a reference:

```yaml
connections:
  warehouse:
    backend: postgres
    host: db.internal
    user: etl_writer
    secret: env:PGPASSWORD      # a reference, not a password
```

The reference is resolved on demand into a `SecretStr`, which refuses to render
itself:

```python
>>> from pydbconnect import SecretStr
>>> pw = SecretStr("hunter2")
>>> print(pw)
***
>>> f"connecting with {pw}"
'connecting with ***'
>>> repr(pw)
'SecretStr(***)'
>>> pw.reveal()
'hunter2'
```

`.reveal()` is the only way out. It is deliberately ugly to type and trivial to
grep for in code review: `git grep '\.reveal()'` should return a handful of
lines in backend modules and nowhere else.

`SecretStr` also refuses to pickle, and hashes to a constant so the value cannot
be recovered from a hash bucket in a heap dump.

## Resolvers

| Reference | Reads from | Use when |
|-----------|------------|----------|
| `env:PGPASSWORD` | Environment variable | Default. Containers, CI, local development |
| `file:/run/secrets/db` | A file on disk | Kubernetes secrets, Docker secrets, mounted volumes |
| `keyvault:myvault/db-password` | Azure Key Vault | Azure workloads with managed identity |
| `vault:myvault/db-password` | Alias for `keyvault:` | Same |
| `literal:hunter2` | The string itself | Never, in anything shared. Warns every time |

A reference with no recognised scheme is treated as `env:`, so `secret:
PGPASSWORD` works. That default is deliberate - a typo becoming a literal
password would be a bad failure mode.

### `env:`

```yaml
secret: env:PGPASSWORD
```

The default and usually the right answer. The environment is process-scoped,
never written to disk by your code, and set by whatever already manages
deployment.

Two real weaknesses, worth knowing: environment variables are visible to
anything that can read `/proc/<pid>/environ` as the same user or root, and they
are inherited by child processes - so a subprocess you shell out to gets your
database password whether it needs it or not.

### `file:`

```yaml
secret: file:/run/secrets/warehouse-password
```

How Kubernetes and Docker Swarm deliver secrets. Better than the environment in
two ways: the file is not inherited by child processes, and the orchestrator can
update it in place, so **rotation happens without a restart** as long as you
open connections rather than caching the resolved value forever.

```yaml
# Kubernetes
apiVersion: v1
kind: Pod
spec:
  containers:
    - name: etl
      env:
        - name: PYDB_PROFILE
          value: prod
      volumeMounts:
        - name: db-credentials
          mountPath: /run/secrets
          readOnly: true
  volumes:
    - name: db-credentials
      secret:
        secretName: warehouse-password
```

Trailing newlines are stripped, because every tool that writes a secret file
adds one and no password ends in `\n`.

### `keyvault:`

```yaml
secret: keyvault:my-vault/warehouse-password
```

Authenticates with `DefaultAzureCredential`, which tries managed identity,
workload identity, environment credentials, the Azure CLI and Visual Studio Code
in turn. On AKS or App Service with a managed identity assigned, **there is no
credential to store anywhere** - the platform vouches for the workload.

The vault name may be bare (`my-vault`) or a full URL
(`https://my-vault.vault.azure.net`). `azure-identity` and
`azure-keyvault-secrets` are imported inside the resolver, so they are only
needed if you use it: `pip install "pydb-connect[azure]"`.

Two operational notes. Every call is a network round trip and Key Vault
throttles at a few thousand requests per vault per interval, so resolve once per
process and not once per query. And the identity needs the **Key Vault Secrets
User** role, not Contributor - Contributor grants management-plane access
without data-plane read, which produces a confusing 403.

### `literal:`

```yaml
secret: literal:hunter2      # emits a UserWarning, every time
```

Exists so a five-minute experiment does not require standing up a secret store.
It warns on every resolution and the value is masked even in `repr(config)`.
If you see this warning in CI output, treat it as a build failure.

### Custom resolvers

Anything else - AWS Secrets Manager, HashiCorp Vault, GCP Secret Manager -
plugs in:

```python
from pydbconnect.secrets import register_resolver, SecretStr

def _from_aws(ref: str) -> SecretStr:
    import boto3
    value = boto3.client("secretsmanager").get_secret_value(SecretId=ref)["SecretString"]
    return SecretStr(value, source=f"aws:{ref}")

register_resolver("aws", _from_aws)
# now: secret: aws:prod/warehouse/password
```

Import the SDK *inside* the resolver, as the built-ins do, so users who do not
need it are not forced to install it.

## Redaction

Redaction is defence in depth. It does not replace handling secrets properly; it
catches the case where a driver puts the DSN in an error message and something
logs it.

Every `SecretStr` registers its value in a process-wide table. `redact()`
scrubs those values out of arbitrary text, and it runs automatically in three
places:

- `PyDBError.__str__` - so no library exception can carry a credential;
- `ConnectionConfig.__repr__` and `.to_dict()` - so nothing the CLI prints does;
- `RedactingFilter` - for the `logging` module.

Wire the filter into your handlers at startup:

```python
import logging
from pydbconnect.secrets import RedactingFilter

handler = logging.StreamHandler()
handler.addFilter(RedactingFilter())          # on the handler, not the logger
logging.getLogger().addHandler(handler)
```

Attach it to the **handler**, not to your own logger. A filter on your logger
only sees your records; a filter on the handler sees everything that reaches
it, including records from `psycopg`, `snowflake.connector` and `azure.core` -
which are the libraries most likely to log a connection string.

The filter handles both styles:

```python
log.info("connecting to %s", dsn)              # lazy args - scrubbed
log.info(f"connecting to {dsn}")               # eager f-string - scrubbed
```

Values shorter than four characters are never registered. Scrubbing the string
`"ab"` out of every log line would destroy the logs to protect a password that
was never safe anyway.

**What redaction cannot do:** scrub a value it has never seen, un-send a log
line, or help with a secret that reached a third-party service directly. Set
`sql_guard` and handle secrets correctly; treat redaction as the airbag, not
the brakes.

## Rotation

Rotation only works if it needs no code change and no coordinated deploy.

**Overlapping credentials.** Create the new credential, let both work, roll
consumers over, revoke the old one. A database that supports two passwords for
one user is rare; two *users* with the same grants is the usual workaround, and
`user` is an ordinary config field you can change with an environment variable.

**Resolve late, not once at import.** This library resolves the secret inside
`backend.connect()`, on every connection open. A process that opens connections
through a pool with `recycle` set picks up a rotated `file:` secret without a
restart. A process that resolves at import and caches for a week does not.

**Rotation checklist**

- [ ] The credential is in a store, not in a file, not in an image, not in a CI variable that is echoed anywhere.
- [ ] The store logs reads, so you can tell who used it and when.
- [ ] Rotation is scheduled, and the schedule has been exercised at least once - untested rotation is not rotation.
- [ ] The old credential is actually revoked afterwards, not just superseded.
- [ ] Nothing in the pipeline echoes the value: check `set -x`, `docker inspect`, `kubectl describe`, and CI job logs.
- [ ] `pool.recycle` is short enough that a rotated `file:` secret is picked up without a restart.

## Local development without leaking

The usual `.env` file has two problems: it is a plaintext credential on a laptop
disk, and it gets committed roughly once per team per year.

Better options, in order:

1. **Do not use production credentials locally.** Point `profile: dev` at
   SQLite, or at a container. This library is designed so the *only* difference
   is two lines of config:

   ```yaml
   profiles:
     dev:
       connections:
         warehouse: {backend: sqlite, database: ./dev.db}
   ```

2. **Use your own identity.** With `az login` on the machine,
   `DefaultAzureCredential` works for `keyvault:` references - your access, your
   audit trail, revocable with your account.

3. **Use the OS keychain**, via a tiny custom resolver:

   ```python
   register_resolver("keychain", lambda ref: SecretStr(
       __import__("keyring").get_password(*ref.split("/", 1)), source="keychain"))
   ```

4. **If you must use a file**, keep it outside the repository entirely -
   `~/.config/pydbconnect/secrets/warehouse` with mode `600` - and reference it
   with `file:`. A file inside the working tree will eventually be committed,
   `.gitignore` or not, because `git add -A` runs before `.gitignore` is read
   by a human.

Whatever you choose, add these to `.gitignore` on day one and verify with
`git check-ignore -v .env`:

```gitignore
.env
.env.*
*.pem
*.p8
*.key
secrets/
connections.local.yaml
```

## CI patterns

**GitHub Actions**

```yaml
jobs:
  load:
    runs-on: ubuntu-latest
    env:
      PYDB_PROFILE: prod
      PYDB_WAREHOUSE_PASSWORD: ${{ secrets.WAREHOUSE_PASSWORD }}
    steps:
      - uses: actions/checkout@v4
      - run: pip install "pydb-connect[postgres]"
      - run: pydb config validate          # fails fast, exit code 2
      - run: pydb load warehouse --table staging --file data.csv --chunk 5000
```

`PYDB_WAREHOUSE_PASSWORD` sets `secret` to a *reference* to itself, so the value
never enters the config object. GitHub masks known secret values in log output,
but only exact matches - it will not catch a value that gets base64-encoded or
truncated in transit, which is another reason the library redacts on its own.

**OIDC beats stored secrets.** GitHub Actions, GitLab CI and Azure Pipelines can
all federate to a cloud identity with no stored credential at all. Combined with
`keyvault:` references and managed identity, the credential count in CI drops to
zero:

```yaml
permissions:
  id-token: write
steps:
  - uses: azure/login@v2
    with:
      client-id: ${{ vars.AZURE_CLIENT_ID }}
      tenant-id: ${{ vars.AZURE_TENANT_ID }}
      subscription-id: ${{ vars.AZURE_SUBSCRIPTION_ID }}
  - run: pydb ping warehouse       # secret: keyvault:my-vault/warehouse-password
```

**Never echo configuration in CI.** `pydb config list` is safe - it redacts -
but `env | sort` and `set -x` are not.

## Checklist

Before merging anything that touches credentials:

- [ ] No password, token, key or connection string appears anywhere in the repository. Verify with a scanner, not by eye: `gitleaks detect`, `trufflehog`, or GitHub secret scanning.
- [ ] Every `secret:` in configuration is a reference with a scheme.
- [ ] No `literal:` references outside a throwaway experiment.
- [ ] `.gitignore` covers `.env`, `*.pem`, `*.key`, `*.p8`, `secrets/`.
- [ ] `RedactingFilter` is attached to the logging handlers at startup.
- [ ] `.reveal()` appears only where a value is handed to a driver.
- [ ] No secret is passed as a command-line argument - `ps` shows the full command line to every user on the box.
- [ ] No secret is baked into a container image, including in an intermediate layer that a later `RUN rm` "removed".
- [ ] Service accounts have the narrowest grants the job needs, so a leak is bounded.
- [ ] Rotation is scheduled and has been rehearsed.
- [ ] CI logs have been read once, deliberately, looking for leakage.

## If a credential leaks

In order, and start the first two in parallel:

1. **Rotate it.** Immediately. Before the investigation, before the post-mortem, before working out how it got there.
2. **Revoke the old one.** Superseding is not revoking.
3. **Read the access logs** for the window between exposure and revocation. Database audit logs, Key Vault access logs, cloud sign-in logs.
4. **Assume the whole repository history is exposed**, not only that one line. Scan for others.
5. **Then** fix the process that allowed it, and add a pre-commit scanner so the next one is caught before it is pushed.
