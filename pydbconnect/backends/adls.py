"""Azure Data Lake Storage Gen2 / Blob Storage backend.

Install with ``pip install "pydb-connect[azure]"``.

**This is object storage, not a database.** Only part of the
:class:`~pydbconnect.backends.base.Backend` contract applies, and the parts that
do not are explicit about it rather than quietly returning nothing:

============================  =====================================================
Works                         Does not
============================  =====================================================
config resolution             ``execute`` / ``query`` / ``executemany``
secret resolution             transactions - object writes are atomic per blob
pooling and liveness checks   ``upsert`` - there are no rows to merge
retry classification          ``COPY`` - use :meth:`ADLSClient.upload_file`
============================  =====================================================

Calling a SQL method raises :class:`~pydbconnect.exceptions.NotSupportedError`
with a message pointing at the right one. The value of having it here at all is
that a pipeline reading Parquet from a lake and writing rows to a warehouse
configures both halves the same way, through the same file, with the same
secret handling.

Configuration::

    connections:
      lake:
        backend: adls
        host: mystorageacct          # or a full https:// URL
        database: raw                # container / filesystem name
        secret: env:AZURE_STORAGE_CONNECTION_STRING   # optional
        options:
          auth: default              # default | connection_string | key | sas
          endpoint_suffix: core.windows.net
          max_concurrency: 4

Authentication, in order of preference:

1. **Managed identity or workload identity** via ``DefaultAzureCredential`` -
   set no ``secret`` at all. Nothing to rotate, nothing to leak. This is the
   right answer in AKS, App Service, Functions and on a developer machine
   signed in with ``az login``.
2. **Connection string or account key** in ``secret``. Works everywhere,
   rotates badly, grants full account access. Use only when identity-based auth
   is genuinely unavailable.
3. **SAS token** in ``secret`` - narrower than an account key and time-bounded,
   but still bearer credentials in an environment variable.

When ``auth`` is left at ``default`` and a ``secret`` is present, its shape is
detected: ``AccountKey=`` means connection string, a leading ``?`` or an
embedded ``sig=`` means SAS, anything else is treated as an account key.

Usage::

    with connect("lake") as lake:
        client = lake.client
        for path in client.list_paths("events/2026/"):
            print(path)
        data = client.read_bytes("events/2026/01/part-0.parquet")
        client.write_text("_SUCCESS", "")
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterator, Optional, Sequence

from ..exceptions import ConnectionFailure, NotSupportedError
from .base import Backend

if TYPE_CHECKING:  # pragma: no cover
    from ..config import ConnectionConfig

__all__ = ["ADLSBackend", "ADLSClient"]

log = logging.getLogger("pydbconnect.backends.adls")

#: HTTP status codes worth retrying. 408 request timeout, 429 throttled,
#: 500/502/503/504 server-side. Azure throttles aggressively under load and
#: expects clients to back off - the 429 case is the one that matters.
_RETRYABLE_STATUS = frozenset({408, 429, 500, 502, 503, 504})

#: Status codes that mean "stop": bad request, unauthenticated, forbidden,
#: not found, conflict, precondition failed.
_PERMANENT_STATUS = frozenset({400, 401, 403, 404, 409, 412})


class ADLSClient:
    """A thin, testable wrapper over an Azure container client.

    Obtained from ``connection.client`` (or ``connection.raw``). Paths are
    relative to the configured container, so ``"events/2026/part-0.parquet"``
    rather than a full URL.

    Attributes:
        container: The container / filesystem name.
        account_url: The storage endpoint in use.
    """

    def __init__(self, service_client: Any, container: str, account_url: str = "") -> None:
        self._service = service_client
        self._container_client = service_client.get_container_client(container)
        self._datalake: Any = None
        self.container = container
        self.account_url = account_url

    # -- reads -------------------------------------------------------------- #

    def list_paths(self, prefix: str = "", *, limit: Optional[int] = None) -> Iterator[str]:
        """Yield blob paths under ``prefix``.

        Lazily paginated: listing a container with ten million blobs does not
        build a ten-million-element list.

        Args:
            prefix: Path prefix, e.g. ``"events/2026/01/"``.
            limit: Stop after this many names.
        """
        listing = self._container_client.list_blobs(name_starts_with=prefix or None)
        for count, blob in enumerate(listing, start=1):
            yield blob.name
            if limit is not None and count >= limit:
                return

    def exists(self, path: str) -> bool:
        """Return whether a blob exists at ``path``."""
        return bool(self._container_client.get_blob_client(path).exists())

    def stat(self, path: str) -> Dict[str, Any]:
        """Return size, last-modified time, content type and ETag for ``path``."""
        props = self._container_client.get_blob_client(path).get_blob_properties()
        return {
            "path": path,
            "size": props.size,
            "last_modified": props.last_modified,
            "content_type": getattr(props.content_settings, "content_type", None),
            "etag": props.etag,
        }

    def read_bytes(self, path: str) -> bytes:
        """Download ``path`` and return its bytes.

        The whole object lands in memory. For anything large use
        :meth:`download_file`, which streams to disk.
        """
        return self._container_client.get_blob_client(path).download_blob().readall()

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        """Download ``path`` and decode it as text."""
        return self.read_bytes(path).decode(encoding)

    def download_file(self, path: str, local_path: str) -> int:
        """Stream ``path`` to a local file and return the bytes written."""
        written = 0
        with open(local_path, "wb") as handle:
            stream = self._container_client.get_blob_client(path).download_blob()
            written = stream.readinto(handle)
        return int(written or 0)

    # -- writes ------------------------------------------------------------- #

    def write_bytes(self, path: str, data: bytes, *, overwrite: bool = True, **kwargs: Any) -> str:
        """Upload ``data`` to ``path`` and return the path.

        Args:
            path: Destination path within the container.
            data: The bytes to write.
            overwrite: Replace an existing blob. Left at True because a
                pipeline that cannot re-run is not a pipeline.
            **kwargs: Passed to the SDK, e.g. ``content_settings``.
        """
        self._container_client.get_blob_client(path).upload_blob(
            data, overwrite=overwrite, **kwargs
        )
        return path

    def write_text(self, path: str, text: str, *, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Encode ``text`` and upload it to ``path``."""
        return self.write_bytes(path, text.encode(encoding), **kwargs)

    def upload_file(
        self, local_path: str, path: str, *, overwrite: bool = True, max_concurrency: int = 4
    ) -> str:
        """Stream a local file to ``path`` without reading it all into memory."""
        with open(local_path, "rb") as handle:
            self._container_client.get_blob_client(path).upload_blob(
                handle, overwrite=overwrite, max_concurrency=max_concurrency
            )
        return path

    def delete(self, path: str, *, missing_ok: bool = True) -> bool:
        """Delete ``path``. Returns whether anything was deleted.

        Args:
            path: Blob to delete.
            missing_ok: Swallow "not found". A delete that fails because the
                thing is already gone has achieved its purpose.
        """
        try:
            self._container_client.get_blob_client(path).delete_blob()
        except Exception as exc:
            if missing_ok and getattr(exc, "status_code", None) == 404:
                return False
            raise
        return True

    # -- Gen2 hierarchical namespace ---------------------------------------- #

    def _datalake_client(self) -> Any:
        """Return a Data Lake filesystem client, created on first use.

        Directory operations - create, rename, recursive delete - are Gen2
        features that the Blob API cannot express. They need a hierarchical
        namespace enabled on the storage account.
        """
        if self._datalake is None:
            try:
                from azure.storage.filedatalake import DataLakeServiceClient
            except ImportError as exc:
                raise NotSupportedError(
                    "directory operations need azure-storage-file-datalake. "
                    'Install it with: pip install "pydb-connect[azure]"'
                ) from exc
            credential = getattr(self._service, "credential", None)
            service = DataLakeServiceClient(
                account_url=self.account_url.replace(".blob.", ".dfs."),
                credential=credential,
            )
            self._datalake = service.get_file_system_client(self.container)
        return self._datalake

    def create_directory(self, path: str) -> str:
        """Create a Gen2 directory. Requires a hierarchical namespace."""
        self._datalake_client().create_directory(path)
        return path

    def delete_directory(self, path: str) -> str:
        """Delete a Gen2 directory and everything under it."""
        self._datalake_client().get_directory_client(path).delete_directory()
        return path

    def rename(self, path: str, new_path: str) -> str:
        """Rename a Gen2 file or directory - a metadata operation, not a copy."""
        client = self._datalake_client().get_file_client(path)
        client.rename_file(f"{self.container}/{new_path}")
        return new_path

    # -- lifecycle ---------------------------------------------------------- #

    def ping(self) -> bool:
        """Return whether the container is reachable. Never raises."""
        try:
            self._container_client.get_container_properties()
        except Exception:  # noqa: BLE001 - the Azure SDK exception tree is wide
            return False
        return True

    def close(self) -> None:
        """Close the underlying HTTP transports."""
        for client in (self._datalake, self._container_client, self._service):
            if client is None:
                continue
            try:
                client.close()
            except Exception:
                log.debug("error closing azure client", exc_info=True)

    # DB-API-shaped members so the pool and Connection can treat this uniformly.

    def cursor(self) -> Any:
        """Always raises: object storage has no cursors."""
        raise NotSupportedError(
            "the adls backend is object storage and has no SQL interface. "
            "Use connection.client for list_paths/read_bytes/write_bytes/upload_file"
        )

    def commit(self) -> None:
        """No-op: each blob write is atomic on its own."""

    def rollback(self) -> None:
        """No-op: there is nothing to roll back."""

    def __repr__(self) -> str:
        return f"<ADLSClient container={self.container!r} account={self.account_url!r}>"


class ADLSBackend(Backend):
    """Azure Data Lake Gen2 / Blob Storage."""

    name = "adls"
    driver_module = "azure.storage.blob"
    install_extra = "azure"
    default_port = None
    required_fields = ("database",)     # the container name

    placeholder_style = "qmark"         # unused; no SQL
    supports_copy = False
    supports_upsert = False
    supports_streaming = False
    supports_transactions = False

    def connect(self, config: "ConnectionConfig") -> ADLSClient:
        """Build an :class:`ADLSClient` for the configured container.

        Raises:
            ConnectionFailure: No account was configured, or the SDK refused
                the credentials.
            DriverNotInstalledError: The Azure SDK is not installed.
        """
        blob = self.import_driver()
        options = dict(config.options or {})
        container = config.database
        if not container:
            raise ConnectionFailure(
                "adls needs a container name in 'database'", connection=config.name
            )

        account = options.pop("account", None) or config.host
        if not account:
            raise ConnectionFailure(
                "adls needs a storage account: set 'host' or 'options.account'",
                connection=config.name,
            )
        suffix = options.pop("endpoint_suffix", "core.windows.net")
        account_url = (
            account if str(account).startswith("http")
            else f"https://{account}.blob.{suffix}"
        )

        secret = config.resolve_password()
        auth = str(options.pop("auth", "default")).lower()
        value = secret.reveal() if secret is not None else None

        if value and auth == "default":
            auth = self._detect_auth(value)

        try:
            if auth == "connection_string" and value:
                service = blob.BlobServiceClient.from_connection_string(value, **options)
                account_url = getattr(service, "url", account_url)
            elif auth in ("key", "sas") and value:
                service = blob.BlobServiceClient(
                    account_url=account_url, credential=value, **options
                )
            else:
                credential = self._default_credential()
                service = blob.BlobServiceClient(
                    account_url=account_url, credential=credential, **options
                )
        except Exception as exc:
            raise ConnectionFailure(
                f"cannot create an Azure storage client for {account_url} "
                f"container {container!r}: {type(exc).__name__}: {exc}",
                connection=config.name,
            ) from exc

        log.debug("adls client for %s/%s using %s auth", account_url, container, auth)
        return ADLSClient(service, container, account_url)

    @staticmethod
    def _detect_auth(value: str) -> str:
        """Infer the credential type from its shape."""
        lowered = value.lower()
        if "accountkey=" in lowered or "defaultendpointsprotocol=" in lowered:
            return "connection_string"
        if value.startswith("?") or "sig=" in lowered:
            return "sas"
        return "key"

    def _default_credential(self) -> Any:
        """Build a ``DefaultAzureCredential``, imported lazily."""
        try:
            from azure.identity import DefaultAzureCredential
        except ImportError as exc:
            raise NotSupportedError(
                "identity-based auth needs azure-identity. Install it with: "
                'pip install "pydb-connect[azure]" - or set a secret to use a '
                "connection string, account key or SAS token instead"
            ) from exc
        return DefaultAzureCredential()

    def cursor(self, conn: Any, *, server_side: bool = False, name: str = "") -> Any:
        """Always raises - see :meth:`ADLSClient.cursor`."""
        return conn.cursor()

    def ping(self, conn: Any) -> bool:
        """Return whether the container responds to a metadata request."""
        try:
            return bool(conn.ping())
        except Exception:  # noqa: BLE001 - ping must never raise
            return False

    def close(self, conn: Any) -> None:
        """Close the SDK clients."""
        conn.close()

    def on_connect(self, conn: Any, config: "ConnectionConfig") -> None:
        """No session setup: there is no session."""

    def upsert_sql(
        self,
        table: str,
        columns: Sequence[str],
        key_columns: Sequence[str],
        update_columns: Optional[Sequence[str]] = None,
    ) -> str:
        """Always raises: there is no SQL here."""
        raise NotSupportedError(
            "the adls backend has no upsert; write objects with "
            "client.write_bytes or client.upload_file"
        )

    def classify_error(self, exc: BaseException) -> Optional[bool]:
        """Classify by HTTP status code.

        ``429 Too Many Requests`` and ``503 Server Busy`` are Azure telling you
        to back off, and backing off is exactly what the retry layer does.
        ``403`` means the credential is wrong, and no amount of retrying will
        change that.
        """
        status = getattr(exc, "status_code", None)
        if status is None:
            response = getattr(exc, "response", None)
            status = getattr(response, "status_code", None) if response is not None else None
        if status is not None:
            try:
                status = int(status)
            except (TypeError, ValueError):  # pragma: no cover
                return None
            if status in _RETRYABLE_STATUS:
                return True
            if status in _PERMANENT_STATUS:
                return False
        name = type(exc).__name__
        if name in ("ServiceRequestError", "ServiceResponseError", "IncompleteReadError"):
            return True
        if name in ("ClientAuthenticationError", "ResourceNotFoundError", "ResourceExistsError"):
            return False
        return None
