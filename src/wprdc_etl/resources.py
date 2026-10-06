"""Shared resources for wprdc-etl.

These are defined ONCE here and wired into the top-level Definitions
(see definitions.py). Component instances reference them *by key* on their
asset parameters — they never instantiate their own copies. That keeps the
object-store client, CKAN client, and geocoder as single shared objects
instead of one-per-publisher.

Landing zone backend: AWS S3 (WPRDC runs on AWS). The resource speaks plain
boto3, so the backend stays a deployment detail — pointing it at LocalStack
for offline dev, or any S3-compatible store, is an endpoint/env change, not a
code change.
"""

from __future__ import annotations

import fnmatch
import hashlib
import io
import json
import os
import posixpath
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import dagster as dg

from wprdc_etl.runtime import (
    guard_ckan_write,
    guard_real_s3_write,
    guard_spatial_write,
    is_production,
)

if TYPE_CHECKING:
    import geopandas as gpd
    import pandas as pd
    import paramiko
    import psycopg

    from wprdc_etl.components.models import GeometryModel

# What one point resolved to, per layer: {(lng, lat): {layer: (id, name)}}.
RegionHits = dict[tuple[float, float], dict[str, tuple[str, str | None]]]
# The resource field each publish records its fingerprint in. CKAN keeps an
# unknown resource field as an extra, through uploads and later patches.
FINGERPRINT_FIELD = "etl_sha256"


# Our field types -> the data-dictionary type_override DataPusher+ honours.
# No `bool`: production's DataPusher+ turns it into text regardless, so the
# replace loader converts booleans first (ckan.bool_format).
_OVERRIDE_TYPES = {"text": "text", "numeric": "numeric", "timestamp": "timestamp"}


def _with_overrides(
    fields: list[dict[str, str]], live: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    """`fields` with a type_override in each one's info, merged into the
    curator info `live` holds for that column (used across a rebuild's drop)."""
    out = []
    for f in fields:
        info = dict((live.get(f["id"]) or {}).get("info") or {})
        if f["type"] in _OVERRIDE_TYPES:
            info["type_override"] = _OVERRIDE_TYPES[f["type"]]
        out.append({**f, **({"info": info} if info else {})})
    return out


def force_publish() -> bool:
    """WPRDC_FORCE_PUBLISH=1 publishes even when CKAN already holds the same
    content — for a resource someone changed by hand behind the fingerprint."""
    return os.getenv("WPRDC_FORCE_PUBLISH", "").strip().lower() in ("1", "true", "yes")


def sha256_file(path: str) -> str:
    """Hex sha256 of a file, read in chunks."""
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def frame_fingerprint(dataframe: pd.DataFrame, *salt: str) -> str:
    """sha256 of a frame's CSV rendering (geometry as WKT), plus `salt`.

    For outputs whose BYTES are not a stable fingerprint: a shapefile's DBF
    header carries the date it was written, so the same frame zips to
    different bytes every day.
    """
    digest = hashlib.sha256("|".join(salt).encode())
    digest.update(dataframe.to_csv(index=False).encode())
    return digest.hexdigest()


@dataclass(frozen=True)
class LayerLoad:
    """What a PostGIS layer refresh did. `changed` is False when the store
    already held exactly these rows, and nothing was written."""

    rows: int
    changed: bool
    duplicates: int = 0


def _rows_fingerprint(config: tuple[Any, ...], rows: Iterable[tuple[Any, ...]]) -> str:
    """sha256 over a layer's config and the exact rows it would write. Each
    value is length-prefixed, so ("ab", "c") and ("a", "bc") can't collide."""
    digest = hashlib.sha256()
    for value in (*config, *(v for row in rows for v in row)):
        raw = value if isinstance(value, bytes) else str(value).encode()
        digest.update(len(raw).to_bytes(8, "big"))
        digest.update(raw)
    return digest.hexdigest()


# key -> (lng, lat) of the keyed geometry's representative point
KeyHits = dict[str, tuple[float, float]]


# --------------------------------------------------------------------------
# Landing zone (AWS S3)
# --------------------------------------------------------------------------
class LandingZoneResource(dg.ConfigurableResource):
    """Immutable landing zone on AWS S3.

    Layout: s3://{bucket}/{publisher}/{dataset}/{yyyy-mm-dd}/data.csv
            plus a manifest.json sidecar in the same prefix.
    """

    bucket: str = "wprdc-etl-landing"
    region: str = "us-east-1"  # CONFIRM WPRDC's actual region
    # Leave endpoint_url unset for real AWS S3. Set it only for a local S3
    # emulator (LocalStack) during offline dev.
    endpoint_url: str | None = None
    # Leave keys unset in production: boto3's default credential chain picks up
    # the EC2 instance profile / ECS task role / EKS IRSA automatically. Static
    # keys are for local dev against LocalStack only.
    access_key: str | None = None
    secret_key: str | None = None
    # Real S3 uses virtual-host addressing (the default). LocalStack needs
    # path-style, so flip this on only in local dev.
    force_path_style: bool = False

    def _client(self) -> Any:
        """A boto3 S3 client. boto3 builds its clients dynamically, so there's
        no real static type to hang on this."""
        import boto3
        from botocore.config import Config

        kwargs: dict[str, Any] = {"region_name": self.region}
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url
        # Only pass explicit keys if given; otherwise the default chain (IAM
        # role) is used — the right thing in production on AWS.
        if self.access_key and self.secret_key:
            kwargs["aws_access_key_id"] = self.access_key
            kwargs["aws_secret_access_key"] = self.secret_key
        if self.force_path_style:
            kwargs["config"] = Config(
                s3={"addressing_style": "path"}, signature_version="s3v4"
            )
        return boto3.client("s3", **kwargs)

    def prefix(
        self,
        publisher: str,
        dataset: str,
        partition: str,
        department: str | None = None,
    ) -> str:
        parts = [publisher, *([department] if department else []), dataset, partition]
        return "/".join(parts)

    def object_exists(self, key: str) -> bool:
        import botocore

        try:
            self._client().head_object(Bucket=self.bucket, Key=key)
            return True
        except botocore.exceptions.ClientError:
            return False

    def read_manifest(
        self,
        publisher: str,
        dataset: str,
        partition: str,
        department: str | None = None,
    ) -> dict[str, Any] | None:
        key = f"{self.prefix(publisher, dataset, partition, department)}/manifest.json"
        try:
            obj = self._client().get_object(Bucket=self.bucket, Key=key)
            return json.loads(obj["Body"].read())
        except Exception:
            return None

    def land(
        self,
        publisher: str,
        dataset: str,
        partition: str,
        data: bytes,
        source_meta: dict[str, Any],
        department: str | None = None,
    ) -> dict[str, Any]:
        """Write raw bytes + manifest sidecar. Returns the manifest.

        Idempotent by checksum: if a manifest already exists for this
        partition with a matching sha256, the write is skipped.
        """
        guard_real_s3_write(self.endpoint_url)
        sha = hashlib.sha256(data).hexdigest()
        existing = self.read_manifest(publisher, dataset, partition, department)
        if existing and existing.get("sha256") == sha:
            return {**existing, "skipped": True}

        prefix = self.prefix(publisher, dataset, partition, department)
        data_key = f"{prefix}/data.csv"
        manifest = {
            "publisher": publisher,
            "department": department,
            "dataset": dataset,
            "partition": partition,
            "sha256": sha,
            "size": len(data),
            "source": source_meta,  # e.g. remote path, mtime, size from listdir_attr
            "skipped": False,
        }
        client = self._client()
        client.put_object(Bucket=self.bucket, Key=data_key, Body=data)
        client.put_object(
            Bucket=self.bucket,
            Key=f"{prefix}/manifest.json",
            Body=json.dumps(manifest).encode(),
        )
        return manifest

    @staticmethod
    def _sha256_file(path: str, chunk: int = 1 << 20) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for block in iter(lambda: f.read(chunk), b""):
                h.update(block)
        return h.hexdigest()

    def land_file(
        self,
        publisher: str,
        dataset: str,
        partition: str,
        local_path: str,
        source_meta: dict[str, Any],
        department: str | None = None,
        filename: str = "data.csv",
    ) -> dict[str, Any]:
        """Streaming variant of land() for large files. Hashes the file in
        chunks and uploads it straight from disk via boto3's transfer manager
        (auto multipart), so the file is never held fully in memory. Same
        checksum-idempotency as land(). The object is stored under `filename`
        so blobs keep their extension; the name is recorded in the manifest."""
        guard_real_s3_write(self.endpoint_url)
        sha = self._sha256_file(local_path)
        existing = self.read_manifest(publisher, dataset, partition, department)
        if existing and existing.get("sha256") == sha:
            return {**existing, "skipped": True}

        prefix = self.prefix(publisher, dataset, partition, department)
        manifest = {
            "publisher": publisher,
            "department": department,
            "dataset": dataset,
            "partition": partition,
            "filename": filename,
            "sha256": sha,
            "size": os.path.getsize(local_path),
            "source": source_meta,
            "skipped": False,
        }
        client = self._client()
        client.upload_file(local_path, self.bucket, f"{prefix}/{filename}")
        client.put_object(
            Bucket=self.bucket,
            Key=f"{prefix}/manifest.json",
            Body=json.dumps(manifest).encode(),
        )
        return manifest

    def download(self, key: str, local_path: str) -> None:
        """Stream an object from the landing bucket to a local path (multipart
        via the transfer manager)."""
        self._client().download_file(self.bucket, key, local_path)

    # -- per-dataset state (watermark, last-seen columns) -------------------
    # Dataset-scoped, deliberately outside the dataset's landing prefix: these
    # are mutable running state, and the landing zone proper is immutable.
    def _state_key(
        self,
        publisher: str,
        dataset: str,
        department: str | None = None,
        *,
        filename: str = "watermark.json",
    ) -> str:
        parts = [
            "_state",
            publisher,
            *([department] if department else []),
            dataset,
            filename,
        ]
        return "/".join(parts)

    def read_watermark(
        self, publisher: str, dataset: str, department: str | None = None
    ) -> Any:
        """Return the stored high-water mark (opaque value) or None if unset."""
        key = self._state_key(publisher, dataset, department)
        try:
            obj = self._client().get_object(Bucket=self.bucket, Key=key)
            return json.loads(obj["Body"].read()).get("watermark")
        except Exception:
            return None

    def write_watermark(
        self,
        publisher: str,
        dataset: str,
        value: Any,
        department: str | None = None,
    ) -> None:
        """Persist the new high-water mark (a datetime string, id, token, ...)."""
        guard_real_s3_write(self.endpoint_url)
        key = self._state_key(publisher, dataset, department)
        body = json.dumps(
            {"watermark": value, "updated_at": datetime.now(timezone.utc).isoformat()}
        )
        self._client().put_object(Bucket=self.bucket, Key=key, Body=body.encode())

    def read_schema_state(
        self, publisher: str, dataset: str, department: str | None = None
    ) -> dict[str, Any] | None:
        """The column set this dataset last presented, or None if never seen.

        Backs the schema_ok drift signal. It can't live in a landing manifest:
        those are per-partition and immutable, and they're written by `landed`,
        which never parses the file and so never sees a column list. Drift is a
        question about the dataset over time, not about one partition.
        """
        key = self._state_key(publisher, dataset, department, filename="schema.json")
        try:
            obj = self._client().get_object(Bucket=self.bucket, Key=key)
            return json.loads(obj["Body"].read())
        except Exception:
            return None

    def write_schema_state(
        self,
        publisher: str,
        dataset: str,
        columns: list[str],
        column_hash: str,
        department: str | None = None,
        partition: str | None = None,
    ) -> None:
        """Record the columns this run saw, as the baseline for the next one."""
        guard_real_s3_write(self.endpoint_url)
        key = self._state_key(publisher, dataset, department, filename="schema.json")
        body = json.dumps(
            {
                "columns": columns,
                "schema_hash": column_hash,
                "partition": partition,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        self._client().put_object(Bucket=self.bucket, Key=key, Body=body.encode())

    # -- canonical accumulated table ----------------------------------------
    # The full current state of an accumulating dataset, kept as parquet so
    # dtypes survive the round trip. A CSV here would be self-defeating: reading
    # one back turns nullable ints into floats and eats leading zeros on the
    # very identifier columns the merge keys on.
    def read_table_state(
        self, publisher: str, dataset: str, department: str | None = None
    ) -> pd.DataFrame | None:
        """The stored canonical table, or None on the dataset's first run."""
        key = self._state_key(
            publisher, dataset, department, filename="current.parquet"
        )
        try:
            obj = self._client().get_object(Bucket=self.bucket, Key=key)
            raw = io.BytesIO(obj["Body"].read())
        except Exception:
            return None
        return _read_parquet(raw)

    def read_table_meta(
        self, publisher: str, dataset: str, department: str | None = None
    ) -> dict[str, Any] | None:
        """What we last wrote to the canonical table, or None if never.

        A sidecar rather than a property of the parquet, so the expected row
        count can be checked WITHOUT paying to read the table back. It's what
        catches a lost or truncated state object: publishing a short table would
        have DataPusher+ reload CKAN down to it.
        """
        key = self._state_key(publisher, dataset, department, filename="current.json")
        try:
            obj = self._client().get_object(Bucket=self.bucket, Key=key)
            return json.loads(obj["Body"].read())
        except Exception:
            return None

    def write_table_state(
        self,
        publisher: str,
        dataset: str,
        dataframe: pd.DataFrame,
        department: str | None = None,
    ) -> None:
        """Persist the canonical table as the next run's starting point.

        Writes the parquet first, then the sidecar: a crash between the two
        leaves the sidecar describing the OLDER, smaller table, which reads as
        "no shrinkage" next run. The reverse order would report phantom loss.
        """
        guard_real_s3_write(self.endpoint_url)
        client = self._client()
        buf = io.BytesIO()
        dataframe.to_parquet(buf, index=False)
        client.put_object(
            Bucket=self.bucket,
            Key=self._state_key(
                publisher, dataset, department, filename="current.parquet"
            ),
            Body=buf.getvalue(),
        )
        client.put_object(
            Bucket=self.bucket,
            Key=self._state_key(
                publisher, dataset, department, filename="current.json"
            ),
            Body=json.dumps(
                {
                    "rows": int(len(dataframe)),
                    "columns": [str(c) for c in dataframe.columns],
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            ).encode(),
        )


def _read_parquet(raw: Any) -> pd.DataFrame:
    """Read a parquet buffer, preserving geometry when it has geo metadata.

    pandas.read_parquet on a geo file hands back WKB bytes in an object column
    rather than a geometry dtype, so a GeoDataFrame has to go back through
    geopandas to stay one. Sniff the metadata rather than guessing from config.
    """
    import pandas as pd
    import pyarrow.parquet as pq

    meta = pq.read_schema(raw).metadata or {}
    raw.seek(0)
    if b"geo" in meta:
        import geopandas as gpd

        return gpd.read_parquet(raw)
    return pd.read_parquet(raw)


# --------------------------------------------------------------------------
# SFTP (paramiko) — thin, only what a landing pull needs
# --------------------------------------------------------------------------
def _reject_unknown_host_policy():
    """paramiko's RejectPolicy, but raising a non-retryable dg.Failure: an
    unknown host key is a config gap, and retrying it only delays the alert."""
    import paramiko

    class _Policy(paramiko.MissingHostKeyPolicy):
        def missing_host_key(self, client, hostname, key):
            raise dg.Failure(
                f"SFTP host {hostname!r} ({key.get_name()}) is not in known_hosts "
                f"— add its verified key to the file SFTP_KNOWN_HOSTS points at",
                allow_retries=False,
            )

    return _Policy()


class SFTPResource(dg.ConfigurableResource):
    """Minimal SFTP client for landing pulls.

    Credentials are looked up from the environment by the component using
    `secret_ref`, so nothing sensitive lives in a config field here.

    Host-key verification: in production the server key MUST already be known
    — point `known_hosts` at the file the deploy step mounts
    (`SFTP_KNOWN_HOSTS`, see deploy/compose.prod.yaml), or rely on the
    container user's ~/.ssh/known_hosts. An unknown or changed key is a
    permanent failure (no retry can fix it), so it alerts at once. Outside
    production the key is auto-added (AutoAddPolicy) so local dev against the
    compose `sftp` service works with no setup.
    """

    # Path to an OpenSSH known_hosts file. Falls back to the system/user file.
    known_hosts: str | None = None

    def _connect(
        self, host: str, username: str, password: str, port: int
    ) -> paramiko.SSHClient:
        import paramiko

        ssh = paramiko.SSHClient()
        if self.known_hosts:
            if not os.path.isfile(self.known_hosts):
                raise dg.Failure(
                    f"SFTP known_hosts file {self.known_hosts!r} does not exist",
                    allow_retries=False,
                )
            ssh.load_host_keys(self.known_hosts)  # explicit file (dev or prod)
        elif is_production():
            ssh.load_system_host_keys()  # container user's ~/.ssh/known_hosts
        # else dev, no explicit file: load nothing, so a stale ~/.ssh entry for
        # `localhost` can't collide with the compose sftp container's key.

        # Fail closed in production; auto-learn an unknown key in dev.
        ssh.set_missing_host_key_policy(
            _reject_unknown_host_policy()
            if is_production()
            else paramiko.AutoAddPolicy()
        )
        try:
            ssh.connect(hostname=host, port=port, username=username, password=password)
        except paramiko.BadHostKeyException as e:
            raise dg.Failure(
                f"SFTP host {host!r} presented a key that does not match "
                f"known_hosts — verify it with the publisher before updating "
                f"the file ({e})",
                allow_retries=False,
            ) from e
        return ssh

    def list_matching(
        self,
        host: str,
        username: str,
        password: str,
        path_glob: str,
        port: int = 22,
    ) -> list[dict[str, Any]]:
        remote_dir = posixpath.dirname(path_glob)
        pattern = posixpath.basename(path_glob)
        ssh = self._connect(host, username, password, port)
        try:
            sftp = ssh.open_sftp()
            out: list[dict[str, Any]] = []
            for attr in sftp.listdir_attr(remote_dir):
                if fnmatch.fnmatch(attr.filename, pattern):
                    out.append(
                        {
                            "path": posixpath.join(remote_dir, attr.filename),
                            "mtime": attr.st_mtime,  # cheap change-detection
                            "size": attr.st_size,
                        }
                    )
            return out
        finally:
            ssh.close()

    def fetch(
        self,
        host: str,
        username: str,
        password: str,
        remote_path: str,
        port: int = 22,
    ) -> bytes:
        """Fetch a whole file into memory. Only for small payloads — large
        files should use fetch_to_file to avoid holding them in RAM."""
        ssh = self._connect(host, username, password, port)
        try:
            sftp = ssh.open_sftp()
            buf = io.BytesIO()
            sftp.getfo(remote_path, buf)
            return buf.getvalue()
        finally:
            ssh.close()

    def fetch_to_file(
        self,
        host: str,
        username: str,
        password: str,
        remote_path: str,
        local_path: str,
        port: int = 22,
    ) -> None:
        """Stream a remote file to a local path in chunks (paramiko's get()),
        so the file never lives fully in memory."""
        ssh = self._connect(host, username, password, port)
        try:
            sftp = ssh.open_sftp()
            sftp.get(remote_path, local_path)
        finally:
            ssh.close()


# --------------------------------------------------------------------------
# CKAN / DataPusher+ target
# --------------------------------------------------------------------------
class CkanResource(dg.ConfigurableResource):
    base_url: str = "https://data.wprdc.org"
    api_key: str = ""
    # ckanext-spatialdata action that makes a DataStore table spatial-ready —
    # builds `dataspatial_wkb` plus its indexes from a WKT or lat/lng column.
    # Deployment-level, so it's set once on the resource rather than per
    # dataset. Empty until confirmed against the deployment; see
    # make_spatial_ready.
    spatial_action: str = ""
    # Action that ingests an uploaded GeoJSON *file* resource into the
    # DataStore, creating the geometry column as part of the load. This is the
    # route a mirrored geojson takes when `mirror[].datastore` is set — NOT
    # `datapusher_submit`, which would build a table with no geometry and look
    # like success. Empty because the endpoint does not exist yet; set it once
    # here (see definitions.py) when it does.
    spatial_load_action: str = ""

    def _action(
        self,
        name: str,
        json: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
    ) -> Any:
        """POST to the CKAN Action API and unwrap the result envelope."""
        import requests

        resp = requests.post(
            f"{self.base_url}/api/3/action/{name}",
            headers={"Authorization": self.api_key},
            json=json,
            data=data,
            files=files,
            timeout=300,
        )
        resp.raise_for_status()
        payload = resp.json()
        if not payload.get("success"):
            raise RuntimeError(f"CKAN {name} failed: {payload.get('error')}")
        return payload["result"]

    def make_spatial_ready(self, resource_id: str, spatial: GeometryModel) -> None:
        """STUB — make a freshly rebuilt DataStore table spatial-ready.

        ckanext-spatialdata adds `dataspatial_wkb` and its indexes to a table,
        derived from a WKT column or a lat/lng pair. Those columns are not part
        of the resource's publicly viewable schema, so a `rebuild` (which DROPS
        the table) takes them with it and they have to be rebuilt afterwards. A
        plain truncating replace does NOT need this — the table, and therefore
        the geometry column, survives.

        TO WIRE THIS UP, two things are needed:

        1. The action name, set as `spatial_action` on this resource (see
           definitions.py). Check whether the extension already exposes one
           before adding it.
        2. Whether it can run immediately. `datapusher_submit` is async and
           returns when the job is QUEUED, so calling this straight after would
           likely hit a table with no rows in it yet. If the extension doesn't
           tolerate that, this needs to poll for the dpp job to finish first —
           there is no completion wait anywhere in this codebase yet.

        `replace()` refuses to start a spatial rebuild while this is a stub, so
        the table is never dropped without a way to restore its geometry.
        """
        raise NotImplementedError(
            "ckanext-spatialdata regeneration is not wired up. Set "
            "`spatial_action` on CkanResource to the action that makes a table "
            "spatial-ready, and implement the call here (see the docstring for "
            "the DataPusher+ timing caveat). "
            f"Wanted it for resource {resource_id} using "
            f"{'wkt=' + spatial.wkt if spatial.wkt else f'lat={spatial.lat} lng={spatial.lng}'}."
        )

    def replace(
        self,
        resource_id: str,
        dataframe: pd.DataFrame,
        rebuild: bool = False,
        spatial: GeometryModel | None = None,
    ) -> bool:
        """Full-refresh load. Upload the new file onto the resource, clear the
        old DataStore rows, then trigger DataPusher+ to reload. Returns False,
        having written nothing, when CKAN already holds exactly this table.

        UNCHANGED means the CSV's sha256 matches the fingerprint recorded on
        the resource at the last upload AND the DataStore holds as many rows as
        the frame. The row count matters: DataPusher+ runs after the upload,
        asynchronously, so a push that failed would otherwise leave a stale
        table behind a matching fingerprint for good. A rebuild always
        publishes; so does WPRDC_FORCE_PUBLISH=1.

        TRUNCATE, NOT DROP. `datastore_delete` with no `filters` drops the whole
        table, which takes with it any column something else added — most
        importantly ckanext-spatialdata's `dataspatial_wkb` and its GiST index,
        which are not part of the resource's publicly viewable columns. Passing
        `filters: {}` deletes the rows and leaves the table standing.

        `rebuild=True` restores the old drop-and-recreate, which is what you want
        when the column set or types genuinely changed. The loader blocks a
        structural change unless it's set, because a truncated table keeps its
        existing types and the reload would fail or silently coerce.

        We submit to DataPusher+ explicitly rather than relying on CKAN's
        auto-trigger on resource change — that hook has a long history of not
        firing when only the file changes (ckan/datapusher#151, ckan/ckan#5727).

        THE DATA DICTIONARY PINS THE TYPES. Production's DataPusher+ drops and
        re-creates the table on every load, typing columns by qsv inference —
        ids, wards and tracts become numeric while the frame publishes them as
        text, so the next run meets a "type change" and the guard blocks it.
        It honours `info.type_override` when it re-creates the table, so every
        load first writes one per column into the data dictionary (merged into
        any curator labels/notes): when creating the table on a first load or
        after a rebuild, and in place on an existing table. Verified on
        data.wprdc.org: without overrides, text ids came back numeric every
        time; with them, they stayed text. `bool` has no override there — the
        replace loader publishes booleans as text or 0/1 (`ckan.bool_format`).
        """
        import tempfile

        guard_ckan_write(self.base_url)

        # Refuse BEFORE anything is uploaded or dropped. A spatial rebuild that
        # can't regenerate geometry leaves the table silently non-spatial, and
        # failing after the drop would report that without preventing it.
        if rebuild and spatial and not self.spatial_action:
            raise RuntimeError(
                f"resource {resource_id}: `ckan.rebuild` drops the DataStore "
                "table, which destroys ckanext-spatialdata's geometry column and "
                "indexes, and regeneration is not wired up yet (see "
                "CkanResource.make_spatial_ready). Refusing to rebuild. Either "
                "set `spatial_action` on the CKAN resource, or drop `rebuild` "
                "and let the reload truncate instead."
            )

        # Stream from a temp file rather than a BytesIO: an accumulated table is
        # the whole dataset, not a delta, and requests reads an open handle in
        # chunks instead of holding the encoded CSV in memory (same reason
        # publish_file takes a path).
        fields = self._infer_fields(dataframe)
        live, live_rows = self._datastore_state(resource_id)
        first_load = not live

        fd, path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        try:
            dataframe.to_csv(path, index=False)
            sha = sha256_file(path)
            if (
                not rebuild
                and not first_load
                and live_rows == len(dataframe)
                and self._fingerprint_matches(resource_id, sha)
            ):
                return False

            if first_load:
                self._create_table(resource_id, _with_overrides(fields, {}))
            elif rebuild:
                # Drop and re-create BEFORE the upload, as a first load does:
                # the upload sets off a DataPusher+ job of its own, and one that
                # starts while the old table still stands loads into it — a
                # water_features rebuild came out with its old bool column's
                # True/False instead of the new file's text.
                self._action(
                    "datastore_delete", json={"resource_id": resource_id, "force": True}
                )
                self._create_table(resource_id, _with_overrides(fields, live))
            else:
                self._set_type_overrides(resource_id, fields, live)

            # 1. Attach the new file (patch preserves the resource's other
            #    metadata) and record what it was.
            with open(path, "rb") as fh:
                self._action(
                    "resource_patch",
                    data={"id": resource_id, FINGERPRINT_FIELD: sha},
                    files={"upload": ("data.csv", fh, "text/csv")},
                )
        finally:
            os.unlink(path)

        # 2. Clear the existing rows. Skipped when the table was just created
        #    (a first load or a rebuild): it is already empty.
        if not first_load and not rebuild:
            try:
                self._action(
                    "datastore_delete",
                    json={"resource_id": resource_id, "force": True, "filters": {}},
                )
            except Exception:
                pass
        # 3. Kick off the DataPusher+ job. This is async — it returns once the
        #    job is queued, not once the rows have landed in the DataStore.
        self._action("datapusher_submit", json={"resource_id": resource_id})
        # 4. Re-add the geometry column + indexes the drop destroyed. Only after
        #    a rebuild: a truncating reload leaves them in place.
        if rebuild and spatial:
            self.make_spatial_ready(resource_id, spatial)
        return True

    def upsert(
        self,
        resource_id: str,
        dataframe: pd.DataFrame,
        primary_key: list[str] | None = None,
        chunk_size: int = 10000,
    ) -> None:
        """Incremental load: upsert changed rows by primary key into the CKAN
        DataStore. Ensures the table + PK exist, then upserts in batches.
        No DataPusher+ — this writes to the DataStore API directly."""
        guard_ckan_write(self.base_url)
        if not primary_key:
            raise RuntimeError("upsert requires ckan.primary_key")

        # NaN -> None so the JSON payload is valid.
        records = dataframe.where(dataframe.notna(), None).to_dict(orient="records")
        if not records:
            return  # empty delta -> no-op

        # Ensure the table exists with the right fields + primary key. force=True
        # lets this run against an existing resource; safe to call each run.
        self._action(
            "datastore_create",
            json={
                "resource_id": resource_id,
                "fields": self._infer_fields(dataframe),
                "primary_key": primary_key,
                "force": True,
            },
        )
        for i in range(0, len(records), chunk_size):
            self._action(
                "datastore_upsert",
                json={
                    "resource_id": resource_id,
                    "records": records[i : i + chunk_size],
                    "method": "upsert",
                    "force": True,
                },
            )

    def live_fields(self, resource_id: str) -> dict[str, str]:
        """Return {field_id: ckan_type} for the resource's current DataStore
        table, or {} if it has no DataStore table yet (fresh resource). Drops
        CKAN's internal _id / _full_text fields."""
        return {k: f["type"] for k, f in self._datastore_state(resource_id)[0].items()}

    def _datastore_state(
        self, resource_id: str
    ) -> tuple[dict[str, dict[str, Any]], int]:
        """({field_id: {"type", "info"}}, row count) for the resource's
        DataStore table, or ({}, 0) if it has none yet. `info` is the field's
        data dictionary (labels, notes, type_override)."""
        try:
            result = self._action(
                "datastore_search", json={"resource_id": resource_id, "limit": 0}
            )
        except Exception:
            return {}, 0  # no datastore table / not datastore-active
        fields = {
            f["id"]: {"type": f["type"], "info": f.get("info") or {}}
            for f in result.get("fields", [])
            if not f["id"].startswith("_")
        }
        return fields, int(result.get("total") or 0)

    def resource(self, resource_id: str) -> dict[str, Any]:
        """Read a resource. No write gate — reading is always allowed."""
        return self._action_get("resource_show", {"id": resource_id})

    def _fingerprint_matches(self, resource_id: str, sha: str) -> bool:
        """Whether the resource's recorded fingerprint is `sha`. False when it
        can't be read: an unknown state publishes rather than skips."""
        if force_publish():
            return False
        try:
            return self.resource(resource_id).get(FINGERPRINT_FIELD) == sha
        except Exception:
            return False

    def _set_type_overrides(
        self,
        resource_id: str,
        fields: list[dict[str, str]],
        live: dict[str, dict[str, Any]],
    ) -> None:
        """Write a type_override for each of `fields` into an existing table's
        data dictionary, merged into whatever info a curator left there.

        Every live column is sent with its LIVE type — the override changes
        what DataPusher+ will create next, not the table now — and columns the
        frame doesn't have keep their info untouched. Skipped when every
        override is already in place.
        """
        wanted = {
            f["id"]: _OVERRIDE_TYPES[f["type"]]
            for f in fields
            if f["type"] in _OVERRIDE_TYPES and f["id"] in live
        }
        if all(live[c]["info"].get("type_override") == t for c, t in wanted.items()):
            return
        payload = [
            {
                "id": c,
                "type": f["type"],
                "info": (
                    {**f["info"], "type_override": wanted[c]}
                    if c in wanted
                    else f["info"]
                ),
            }
            for c, f in live.items()
        ]
        self._action(
            "datastore_create",
            json={"resource_id": resource_id, "fields": payload, "force": True},
        )

    def _create_table(self, resource_id: str, fields: list[dict[str, str]]) -> None:
        """Create the resource's DataStore table with exactly `fields`.

        Empty: DataPusher+ fills it. `force` because a resource with an
        uploaded file counts as read-only to the DataStore API otherwise.
        """
        self._action(
            "datastore_create",
            json={"resource_id": resource_id, "fields": fields, "force": True},
        )

    @staticmethod
    def _infer_fields(dataframe: pd.DataFrame) -> list[dict[str, str]]:
        """Map pandas dtypes to CKAN DataStore field types."""
        import pandas as pd

        fields = []
        for col, dtype in dataframe.dtypes.items():
            if pd.api.types.is_bool_dtype(dtype):
                t = "bool"
            elif pd.api.types.is_integer_dtype(dtype) or pd.api.types.is_float_dtype(
                dtype
            ):
                t = "numeric"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                t = "timestamp"
            else:
                t = "text"
            fields.append({"id": str(col), "type": t})
        return fields

    # -- package metadata + mirrored resources ---------------------------
    def package(self, package_id: str) -> dict[str, Any]:
        """Read a package. No write gate — reading is always allowed."""
        return self._action_get("package_show", {"id": package_id})

    def _action_get(self, name: str, params: dict[str, Any]) -> Any:
        """GET against the Action API, for reads.

        Separate from `_action` because CKAN's read actions take query
        parameters and a POST with an empty body confuses some of them behind
        a WAF. Reads intentionally skip guard_ckan_write.
        """
        import requests

        resp = requests.get(
            f"{self.base_url}/api/3/action/{name}",
            headers={"Authorization": self.api_key} if self.api_key else {},
            params=params,
            timeout=120,
        )
        resp.raise_for_status()
        payload = resp.json()
        if not payload.get("success"):
            raise RuntimeError(f"CKAN {name} failed: {payload.get('error')}")
        return payload["result"]

    def patch_package(
        self,
        package_id: str,
        *,
        notes: str | None = None,
        tags: list[str] | None = None,
    ) -> bool:
        """Update a package's description and/or tags. Returns False, writing
        nothing, when the package already says exactly that.

        `package_patch`, not `package_update`: patch leaves every field we
        don't name alone. `package_update` would blank the groups, license and
        extras a curator set, because CKAN treats an omitted field as cleared.
        """
        guard_ckan_write(self.base_url)
        payload: dict[str, Any] = {"id": package_id}
        if notes is not None:
            payload["notes"] = notes
        if tags is not None:
            payload["tags"] = [{"name": t} for t in tags]
        if len(payload) == 1:
            return False
        if not force_publish():
            try:
                live = self.package(package_id)
                same_notes = notes is None or (live.get("notes") or "") == notes
                same_tags = tags is None or sorted(
                    t["name"] for t in live.get("tags") or []
                ) == sorted(tags)
                if same_notes and same_tags:
                    return False
            except Exception:
                pass  # unknown state: publish
        self._action("package_patch", json=payload)
        return True

    def find_resource(self, package_id: str, name: str, fmt: str) -> str | None:
        """The id of the resource in `package_id` named `name`, else None.

        By NAME ONLY. There used to be a format fallback, for a resource a
        curator had renamed by hand, and it caused a silent data swap: a GIS
        package holds two ZIPs ("Shapefile" and "File Geodatabase") and two
        HTMLs (the Hub page and the Esri REST link), so a package missing one
        of a pair handed the mirror the OTHER one's resource. The File
        Geodatabase uploaded straight over the Shapefile, every step reported
        success, and the only clue was the resource's byte count.

        Returning None instead means the caller creates the resource it
        wanted. Missing a hand-rename costs a duplicate resource; guessing
        costs the wrong file under the right name.

        `fmt` is kept for the signature's shape and for logging, not matched.
        """
        resources = self.package(package_id).get("resources", [])
        wanted_name = name.strip().lower()
        for res in resources:
            if (res.get("name") or "").strip().lower() == wanted_name:
                return res["id"]
        return None

    def add_resource(self, package_id: str, name: str, fmt: str, url: str = "") -> str:
        """Create a CKAN resource in `package_id` and return its id.

        NOT named `create_resource`: that is `dg.ConfigurableResource`'s own
        hook for building the resource value, and overriding it with a
        different signature breaks resource initialisation for every asset
        that requests `ckan` — the failure surfaces as a RESOURCE_INIT_FAILURE
        long before any of this code runs.

        A link resource is created with its `url` and nothing is uploaded; a
        file resource is created empty and `publish_file` fills it, because
        `resource_create` with an upload wants the bytes in the same request.
        """
        guard_ckan_write(self.base_url)
        payload = {"package_id": package_id, "name": name, "format": fmt}
        # CKAN requires *some* url on create; a placeholder is replaced by the
        # upload, which sets url_type=upload and rewrites it.
        payload["url"] = url or "https://data.wprdc.org/"
        return self._action("resource_create", json=payload)["id"]

    def submit_to_datastore(self, resource_id: str) -> None:
        """Ask DataPusher+ to ingest an uploaded tabular file. Async: it
        returns once the job is QUEUED, not once the table exists."""
        guard_ckan_write(self.base_url)
        self._action("datapusher_submit", json={"resource_id": resource_id})

    def load_geojson_to_datastore(self, resource_id: str) -> None:
        """STUB — ingest an uploaded GeoJSON resource into the DataStore.

        The endpoint is expected to do the whole job from the uploaded file:
        create the table, the geometry column and its index. That is why this
        does NOT fall back to `datapusher_submit` — DataPusher+ would produce a
        table with the properties as columns and no geometry, which is worse
        than failing, because nothing downstream would notice.

        TO WIRE THIS UP: set `spatial_load_action` on this resource to the
        action name, and check whether it is synchronous. `datapusher_submit`
        is not, and anything that reads the table straight afterwards would
        race it.
        """
        guard_ckan_write(self.base_url)
        if not self.spatial_load_action:
            raise NotImplementedError(
                "loading a GeoJSON into the DataStore needs the spatial load "
                "endpoint, which is not built yet. Set `spatial_load_action` "
                "on CkanResource once it exists, or drop `datastore: true` "
                f"from this dataset's geojson mirror. Wanted it for resource "
                f"{resource_id}."
            )
        self._action(self.spatial_load_action, json={"resource_id": resource_id})

    def publish_link(self, resource_id: str, url: str) -> bool:
        """Point an existing resource at a URL, with no upload. Returns False,
        writing nothing, when it already points there."""
        guard_ckan_write(self.base_url)
        if not force_publish():
            try:
                if self.resource(resource_id).get("url") == url:
                    return False
            except Exception:
                pass  # unknown state: publish
        self._action("resource_patch", json={"id": resource_id, "url": url})
        return True

    def publish_file(
        self,
        resource_id: str,
        local_path: str,
        filename: str,
        fingerprint: str | None = None,
    ) -> bool:
        """Upload a file as a CKAN resource — no DataStore, no DataPusher+.
        Used for blobs (PDFs, GeoTIFFs, images). Streams from the open file
        handle so a large blob isn't buffered in memory (requests reads it in
        chunks and sets Content-Length from the file size).

        Returns False, uploading nothing, when the resource's recorded
        fingerprint already matches. `fingerprint` defaults to the file's
        sha256; pass one when the bytes aren't stable for the same content
        (see frame_fingerprint)."""
        guard_ckan_write(self.base_url)
        sha = fingerprint or sha256_file(local_path)
        if self._fingerprint_matches(resource_id, sha):
            return False
        with open(local_path, "rb") as fh:
            self._action(
                "resource_patch",
                data={"id": resource_id, FINGERPRINT_FIELD: sha},
                files={"upload": (filename, fh, "application/octet-stream")},
            )
        return True


# --------------------------------------------------------------------------
# Cached geocoder (only used when a publisher config sets geocode: true)
# --------------------------------------------------------------------------
class GeocoderResource(dg.ConfigurableResource):
    def geocode_frame(self, dataframe: pd.DataFrame, address_col: str) -> pd.DataFrame:
        """Add lat/lon columns, caching lookups. Stub."""
        raise NotImplementedError("wire to your cached geocoding backend")


# --------------------------------------------------------------------------
# Administrative region store (PostGIS)
# --------------------------------------------------------------------------
# Reverse geocoding — point -> which neighborhood / ward / council district /
# fire zone / municipality contains it — runs against a PostGIS database we own,
# NOT against CKAN. WPRDC's DataStore is not usable for this: the CDN in front of
# it rejects any datastore_search_sql mentioning the geometry column, and the
# non-spatial copy of that column is unprojected WKT with no SRID. Boundary
# layers are instead read from their GeoJSON file resources by ordinary
# pipelines, which write them here via a `region_layer:` block.

_ADMIN_REGION_DDL = """
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS admin_region (
  layer       text NOT NULL,
  region_id   text NOT NULL,
  region_name text,
  attrs       jsonb NOT NULL DEFAULT '{}'::jsonb,
  geom        geometry(MultiPolygon, 4326) NOT NULL,
  loaded_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (layer, region_id)
);
CREATE INDEX IF NOT EXISTS admin_region_geom_gix  ON admin_region USING gist (geom);
CREATE INDEX IF NOT EXISTS admin_region_layer_idx ON admin_region (layer);

CREATE TABLE IF NOT EXISTS admin_region_layer (
  layer       text PRIMARY KEY,
  value_field text NOT NULL,
  label_field text,
  source      text,
  row_count   integer,
  loaded_at   timestamptz NOT NULL DEFAULT now()
);
-- What was written, so an identical refresh can skip the rewrite. ALTER, not
-- part of CREATE: stores that predate it get the column in place.
ALTER TABLE admin_region_layer ADD COLUMN IF NOT EXISTS content_sha256 text;
"""

# Boundary files are messy: mixed Polygon/MultiPolygon, occasional Z coordinates,
# and self-intersecting rings that make ST_Contains unreliable. Normalize every
# geometry on the way in so the column type is uniform and the predicate is sound.
# CollectionExtract(..., 3) keeps only the polygonal parts MakeValid may return.
_NORMALIZE_GEOM = (
    "ST_Multi(ST_CollectionExtract("
    "ST_MakeValid(ST_Force2D(ST_GeomFromWKB(%s, 4326))), 3))"
)


# Geometry looked up BY IDENTIFIER rather than by location — an address point
# by ADDRESS_ID, a parcel by PIN. Kept out of admin_region on purpose: that
# table dissolves by id and answers ST_Contains, and a point contains nothing,
# so mixing ~660k address points in would only slow every reverse geocode.
# No GiST index: every read is by (layer, key), which the primary key covers.
_KEYED_GEOMETRY_DDL = """
CREATE TABLE IF NOT EXISTS keyed_geometry (
  layer  text NOT NULL,
  key    text NOT NULL,
  geom   geometry(Geometry, 4326) NOT NULL,
  PRIMARY KEY (layer, key)
);

CREATE TABLE IF NOT EXISTS keyed_geometry_layer (
  layer      text PRIMARY KEY,
  key_field  text NOT NULL,
  source     text,
  row_count  integer,
  loaded_at  timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE keyed_geometry_layer ADD COLUMN IF NOT EXISTS content_sha256 text;
"""


class SpatialResource(dg.ConfigurableResource):
    """PostGIS store of administrative boundary layers.

    Lives on the ETL server's Postgres, in its own `spatial` database next to
    Dagster's run storage (see compose.yaml). psycopg is imported lazily so a
    checkout without it still loads.
    """

    dsn: str = ""  # postgresql://user:pass@host:5432/spatial

    def _connect(self) -> psycopg.Connection:
        import psycopg

        if not self.dsn:
            raise dg.Failure(
                "the spatial store is not configured: set SPATIAL_DSN (e.g. "
                "postgresql://dagster:dagster@localhost:5434/spatial — the "
                "compose postgis service, NOT the 5432 Dagster server). It backs "
                "the reverse_geocode transform op and region_layer pipelines."
            )
        return psycopg.connect(self.dsn)

    # -- schema ------------------------------------------------------------
    def ensure_schema(self) -> None:
        """Create the region tables if absent. Idempotent; the project has no
        migration tool, so writers call this before every load."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_ADMIN_REGION_DDL)
                cur.execute(_KEYED_GEOMETRY_DDL)

    def layers(self) -> dict[str, int]:
        """{layer: row_count} for every loaded layer, or {} if the store has no
        schema yet. Used to fail loud (naming what IS available) when a dataset
        asks to reverse geocode against a layer nobody has loaded."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("SELECT layer, row_count FROM admin_region_layer")
                except Exception:
                    return {}
                return {row[0]: row[1] for row in cur.fetchall()}

    # -- write side (region_layer pipelines) -------------------------------
    def replace_layer(
        self,
        layer: str,
        gdf: gpd.GeoDataFrame,
        *,
        value_field: str,
        label_field: str | None = None,
        source: str | None = None,
    ) -> LayerLoad:
        """Replace every region in `layer` with the rows of `gdf`.

        Skipped — nothing written — when the store already holds exactly these
        regions under this config (WPRDC_FORCE_PUBLISH=1 overrides).

        Boundary files routinely split one region across several features (the
        ward layer ships 35 features for 32 wards), so features are dissolved by
        value_field first — one row per region, geometry unioned. Returns the
        number of regions written.
        """
        import geopandas as gpd
        import json as _json

        guard_spatial_write(self.dsn)

        if not isinstance(gdf, gpd.GeoDataFrame):
            raise dg.Failure(
                f"region_layer {layer!r}: expected a GeoDataFrame, got "
                f"{type(gdf).__name__}. The source must be a geospatial format "
                "(geojson / shapefile) so it reads as geometry."
            )
        for field in (value_field, label_field):
            if field and field not in gdf.columns:
                raise dg.Failure(
                    f"region_layer {layer!r}: column {field!r} not in the source "
                    f"({sorted(c for c in gdf.columns if c != 'geometry')})"
                )

        gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty]
        if gdf.empty:
            raise dg.Failure(f"region_layer {layer!r}: source has no geometry")
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
        elif gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(4326)

        # One row per region. aggfunc="first" keeps the other attributes from
        # the first feature of each group.
        dissolved = gdf.dissolve(by=value_field, aggfunc="first").reset_index()

        rows: list[tuple[Any, ...]] = []
        for rec in dissolved.to_dict("records"):
            geom = rec.pop("geometry")
            region_id = rec.get(value_field)
            if region_id is None or str(region_id).strip() == "":
                continue
            attrs = {k: _json_safe(v) for k, v in rec.items() if k != "geometry"}
            rows.append(
                (
                    layer,
                    _region_text(region_id),
                    _region_text(rec.get(label_field)) if label_field else None,
                    _json.dumps(attrs, default=str),
                    geom.wkb,
                )
            )
        if not rows:
            raise dg.Failure(
                f"region_layer {layer!r}: every feature had an empty {value_field!r}"
            )

        self.ensure_schema()
        sha = _rows_fingerprint((value_field, label_field or ""), rows)
        if self._layer_fingerprint("admin_region_layer", layer) == sha:
            return LayerLoad(rows=len(rows), changed=False)
        with self._connect() as conn:
            with conn.cursor() as cur:
                # Delete + insert in one transaction: readers see either the old
                # layer or the new one, never a half-loaded layer.
                cur.execute("DELETE FROM admin_region WHERE layer = %s", (layer,))
                cur.executemany(
                    "INSERT INTO admin_region "
                    "(layer, region_id, region_name, attrs, geom) "
                    f"VALUES (%s, %s, %s, %s::jsonb, {_NORMALIZE_GEOM})",
                    rows,
                )
                cur.execute(
                    "INSERT INTO admin_region_layer "
                    "(layer, value_field, label_field, source, row_count, "
                    "content_sha256, loaded_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, now()) "
                    "ON CONFLICT (layer) DO UPDATE SET "
                    "value_field = EXCLUDED.value_field, "
                    "label_field = EXCLUDED.label_field, "
                    "source = EXCLUDED.source, "
                    "row_count = EXCLUDED.row_count, "
                    "content_sha256 = EXCLUDED.content_sha256, "
                    "loaded_at = EXCLUDED.loaded_at",
                    (layer, value_field, label_field, source, len(rows), sha),
                )
        return LayerLoad(rows=len(rows), changed=True)

    def _layer_fingerprint(self, table: str, layer: str) -> str | None:
        """The content_sha256 recorded for `layer`, or None (never loaded, or
        WPRDC_FORCE_PUBLISH=1 — either way, write). `table` is one of our two
        constant metadata tables, never user input. The fingerprint and the
        rows are written in one transaction, so they can't disagree."""
        if force_publish():
            return None
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT content_sha256 FROM {table} WHERE layer = %s", (layer,)
                )
                row = cur.fetchone()
        return row[0] if row else None

    # -- read side (reverse_geocode) ---------------------------------------
    def locate(
        self, points: Iterable[tuple[float, float]], layers: Iterable[str]
    ) -> RegionHits:
        """Point-in-polygon lookup for many points at once.

        `points` is a sequence of (lng, lat) float pairs; `layers` the layer
        names to resolve. Returns {(lng, lat): {layer: (region_id, region_name)}},
        omitting points that matched nothing.

        One round trip regardless of row count: the points go up as arrays into
        a temp table, then a single GiST-indexed join resolves every layer at
        once. ST_Contains (not ST_Intersects) so a point on a shared border
        resolves to exactly one region instead of duplicating the row.
        """
        points = list(points)
        layers = list(layers)
        if not points or not layers:
            return {}

        known = self.layers()
        missing = [layer for layer in layers if layer not in known]
        if missing:
            raise dg.Failure(
                f"spatial store has no layer(s) {missing}; loaded layers are "
                f"{sorted(known) or '(none)'}. Materialize the matching "
                "region_layer pipeline first."
            )

        idx = list(range(len(points)))
        xs = [float(p[0]) for p in points]
        ys = [float(p[1]) for p in points]

        out: RegionHits = {}
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "CREATE TEMP TABLE _pts (idx int, geom geometry(Point, 4326)) "
                    "ON COMMIT DROP"
                )
                cur.execute(
                    "INSERT INTO _pts (idx, geom) "
                    "SELECT i, ST_SetSRID(ST_MakePoint(x, y), 4326) "
                    "FROM unnest(%s::int[], %s::float8[], %s::float8[]) AS t(i, x, y)",
                    (idx, xs, ys),
                )
                cur.execute("ANALYZE _pts")
                cur.execute(
                    "SELECT p.idx, r.layer, r.region_id, r.region_name "
                    "FROM _pts p "
                    "JOIN admin_region r ON ST_Contains(r.geom, p.geom) "
                    "WHERE r.layer = ANY(%s)",
                    (layers,),
                )
                for i, layer, region_id, region_name in cur.fetchall():
                    out.setdefault(points[i], {})[layer] = (region_id, region_name)
        return out

    # -- keyed geometry (key_layer pipelines / join_geometry) ----------------
    def key_layers(self) -> dict[str, int]:
        """{layer: row_count} for every loaded key layer, or {} if the store
        has no keyed-geometry schema yet."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("SELECT layer, row_count FROM keyed_geometry_layer")
                except Exception:
                    return {}
                return {row[0]: row[1] for row in cur.fetchall()}

    def replace_key_layer(
        self,
        layer: str,
        gdf: gpd.GeoDataFrame,
        *,
        key_field: str,
        source: str | None = None,
    ) -> LayerLoad:
        """Replace every geometry in key layer `layer` with the rows of `gdf`.

        `duplicates` counts dropped repeats: a key that repeats keeps its
        first row, since the key is the lookup's identity and a second
        geometry for it could never be returned anyway. Skipped — nothing
        written — when the store already holds exactly these rows
        (WPRDC_FORCE_PUBLISH=1 overrides).

        Loaded with COPY into a temp table and swapped in one transaction —
        an address-point layer is ~660k rows, which row-by-row INSERTs make
        painfully slow on the emulated amd64 PostGIS used in dev.
        """
        import geopandas as gpd
        import shapely

        guard_spatial_write(self.dsn)

        if not isinstance(gdf, gpd.GeoDataFrame):
            raise dg.Failure(
                f"key_layer {layer!r}: expected a GeoDataFrame, got "
                f"{type(gdf).__name__}. The source must be a geospatial format "
                "(geojson / shapefile) so it reads as geometry."
            )
        if key_field not in gdf.columns:
            raise dg.Failure(
                f"key_layer {layer!r}: column {key_field!r} not in the source "
                f"({sorted(c for c in gdf.columns if c != 'geometry')})"
            )

        keys = gdf[key_field].map(key_text)
        keep = keys.notna() & gdf.geometry.notna() & ~gdf.geometry.is_empty
        gdf, keys = gdf[keep], keys[keep]
        if gdf.empty:
            raise dg.Failure(f"key_layer {layer!r}: no rows with a key and geometry")
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
        elif gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(4326)

        dupes = keys.duplicated()
        gdf, keys = gdf[~dupes], keys[~dupes]
        wkbs = shapely.to_wkb(gdf.geometry.values)
        written, duplicates = len(keys), int(dupes.sum())

        self.ensure_schema()
        sha = _rows_fingerprint((key_field,), zip(keys, wkbs))
        if self._layer_fingerprint("keyed_geometry_layer", layer) == sha:
            return LayerLoad(rows=written, changed=False, duplicates=duplicates)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "CREATE TEMP TABLE _kg (key text, wkb bytea) ON COMMIT DROP"
                )
                with cur.copy("COPY _kg (key, wkb) FROM STDIN") as copy:
                    for key, wkb in zip(keys, wkbs):
                        copy.write_row((key, wkb))
                # Delete + insert in one transaction: readers see the old layer
                # or the new one, never a half-loaded one.
                cur.execute("DELETE FROM keyed_geometry WHERE layer = %s", (layer,))
                cur.execute(
                    "INSERT INTO keyed_geometry (layer, key, geom) "
                    "SELECT %s, key, ST_Force2D(ST_GeomFromWKB(wkb, 4326)) FROM _kg",
                    (layer,),
                )
                cur.execute(
                    "INSERT INTO keyed_geometry_layer "
                    "(layer, key_field, source, row_count, content_sha256, loaded_at) "
                    "VALUES (%s, %s, %s, %s, %s, now()) "
                    "ON CONFLICT (layer) DO UPDATE SET "
                    "key_field = EXCLUDED.key_field, "
                    "source = EXCLUDED.source, "
                    "row_count = EXCLUDED.row_count, "
                    "content_sha256 = EXCLUDED.content_sha256, "
                    "loaded_at = EXCLUDED.loaded_at",
                    (layer, key_field, source, written, sha),
                )
        return LayerLoad(rows=written, changed=True, duplicates=duplicates)

    def lookup_keys(self, layer: str, keys: Iterable[str]) -> KeyHits:
        """{key: (lng, lat)} for every key in `layer` that exists.

        The point is ST_PointOnSurface: the geometry itself for a point layer,
        and a point guaranteed to lie inside a polygon one (a parcel), where a
        centroid can fall outside a concave shape. Missing keys are omitted.
        """
        keys = sorted(set(keys))
        if not keys:
            return {}
        known = self.key_layers()
        if layer not in known:
            raise dg.Failure(
                f"spatial store has no key layer {layer!r}; loaded key layers "
                f"are {sorted(known) or '(none)'}. Materialize the matching "
                "key_layer pipeline first."
            )
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT key, ST_X(p), ST_Y(p) FROM ("
                    "  SELECT key, ST_PointOnSurface(geom) AS p FROM keyed_geometry"
                    "  WHERE layer = %s AND key = ANY(%s)"
                    ") s",
                    (layer, keys),
                )
                return {k: (x, y) for k, x, y in cur.fetchall()}


def _json_safe(value: Any) -> Any:
    """Coerce one boundary-file attribute into something jsonb accepts.

    Two traps live here. json.dumps renders a float NaN as a bare `NaN` token,
    which is not valid JSON and which Postgres rejects outright — boundary files
    are full of them (a missing perimeter, an unmeasured acreage). And numpy
    scalars aren't JSON-serializable at all. Both become null / plain Python.
    """
    import math

    import pandas as pd

    if value is None:
        return None
    if hasattr(value, "item") and getattr(value, "shape", None) == ():
        value = value.item()  # numpy scalar -> python scalar
    if isinstance(value, bool) or isinstance(value, (str, int)):
        return value
    if isinstance(value, float):
        return None if (math.isnan(value) or math.isinf(value)) else value
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass  # arrays and the like aren't scalar-NA testable
    return str(value)


def key_text(value: Any) -> str | None:
    """A join key as exact text: 450843, 450843.0 and " 450843 " all become
    "450843". Unlike _region_text this never uses `:g`, which turns 1234567
    into "1.23457e+06" — harmless for a small region code, fatal for a key."""
    import pandas as pd

    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip()
    return text or None


def _region_text(value: Any) -> str | None:
    """Render a region key/label as compact text. Boundary files carry these as
    ints or floats (division 2.0, MUNICODE 100) but they are codes, not
    quantities — publish them the way coerce_text would."""
    import pandas as pd

    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    if isinstance(value, float) and value.is_integer():
        return f"{value:g}"
    if isinstance(value, (int, float)):
        return f"{value:g}"
    return str(value).strip()
