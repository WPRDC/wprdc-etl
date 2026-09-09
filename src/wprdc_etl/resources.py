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
from datetime import datetime, timezone

import dagster as dg

from wprdc_etl.runtime import guard_real_s3_write, is_production


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

    def _client(self):
        import boto3
        from botocore.config import Config

        kwargs = {"region_name": self.region}
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

    def prefix(self, publisher, dataset, partition, department=None) -> str:
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
        self, publisher, dataset, partition, department=None
    ) -> dict | None:
        key = f"{self.prefix(publisher, dataset, partition, department)}/manifest.json"
        try:
            obj = self._client().get_object(Bucket=self.bucket, Key=key)
            return json.loads(obj["Body"].read())
        except Exception:
            return None

    def land(
        self,
        publisher,
        dataset,
        partition,
        data: bytes,
        source_meta: dict,
        department=None,
    ) -> dict:
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
    def _sha256_file(path, chunk=1 << 20) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for block in iter(lambda: f.read(chunk), b""):
                h.update(block)
        return h.hexdigest()

    def land_file(
        self,
        publisher,
        dataset,
        partition,
        local_path,
        source_meta,
        department=None,
        filename="data.csv",
    ) -> dict:
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

    def download(self, key, local_path) -> None:
        """Stream an object from the landing bucket to a local path (multipart
        via the transfer manager)."""
        self._client().download_file(self.bucket, key, local_path)

    # -- incremental watermark state ---------------------------------------
    def _state_key(self, publisher, dataset, department=None) -> str:
        parts = [
            "_state",
            publisher,
            *([department] if department else []),
            dataset,
            "watermark.json",
        ]
        return "/".join(parts)

    def read_watermark(self, publisher, dataset, department=None):
        """Return the stored high-water mark (opaque value) or None if unset."""
        key = self._state_key(publisher, dataset, department)
        try:
            obj = self._client().get_object(Bucket=self.bucket, Key=key)
            return json.loads(obj["Body"].read()).get("watermark")
        except Exception:
            return None

    def write_watermark(self, publisher, dataset, value, department=None) -> None:
        """Persist the new high-water mark (a datetime string, id, token, ...)."""
        guard_real_s3_write(self.endpoint_url)
        key = self._state_key(publisher, dataset, department)
        body = json.dumps(
            {"watermark": value, "updated_at": datetime.now(timezone.utc).isoformat()}
        )
        self._client().put_object(Bucket=self.bucket, Key=key, Body=body.encode())


# --------------------------------------------------------------------------
# SFTP (paramiko) — thin, only what a landing pull needs
# --------------------------------------------------------------------------
class SFTPResource(dg.ConfigurableResource):
    """Minimal SFTP client for landing pulls.

    Credentials are looked up from the environment by the component using
    `secret_ref`, so nothing sensitive lives in a config field here.

    Host-key verification: in production the server key MUST already be known
    (RejectPolicy) — point `known_hosts` at a file baked into the image / a
    mounted secret, or rely on the container user's ~/.ssh/known_hosts. Outside
    production the key is auto-added (AutoAddPolicy) so local dev against the
    compose `sftp` service works with no setup.
    """

    # Path to an OpenSSH known_hosts file. Falls back to the system/user file.
    known_hosts: str | None = None

    def _connect(self, host, username, password, port):
        import paramiko

        ssh = paramiko.SSHClient()
        if self.known_hosts:
            ssh.load_host_keys(self.known_hosts)  # explicit file (dev or prod)
        elif is_production():
            ssh.load_system_host_keys()  # container user's ~/.ssh/known_hosts
        # else dev, no explicit file: load nothing, so a stale ~/.ssh entry for
        # `localhost` can't collide with the compose sftp container's key.

        # Fail closed in production; auto-learn an unknown key in dev.
        ssh.set_missing_host_key_policy(
            paramiko.RejectPolicy() if is_production() else paramiko.AutoAddPolicy()
        )
        ssh.connect(hostname=host, port=port, username=username, password=password)
        return ssh

    def list_matching(self, host, username, password, path_glob, port=22) -> list[dict]:
        remote_dir = posixpath.dirname(path_glob)
        pattern = posixpath.basename(path_glob)
        ssh = self._connect(host, username, password, port)
        try:
            sftp = ssh.open_sftp()
            out = []
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

    def fetch(self, host, username, password, remote_path, port=22) -> bytes:
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
        self, host, username, password, remote_path, local_path, port=22
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

    def _action(self, name, json=None, data=None, files=None):
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

    def replace(self, resource_id: str, dataframe) -> None:
        """Full-refresh load. Upload the new file onto the resource, drop the
        old DataStore table, then trigger DataPusher+ to reload from scratch.

        We submit to DataPusher+ explicitly rather than relying on CKAN's
        auto-trigger on resource change — that hook has a long history of not
        firing when only the file changes (ckan/datapusher#151, ckan/ckan#5727).
        """
        import io

        buf = io.BytesIO()
        dataframe.to_csv(buf, index=False)
        buf.seek(0)

        # 1. Attach the new file (patch preserves the resource's other metadata).
        self._action(
            "resource_patch",
            data={"id": resource_id},
            files={"upload": ("data.csv", buf, "text/csv")},
        )
        # 2. Clear the existing DataStore table so the reload starts clean and
        #    re-infers types. Ignored on the very first load (no table yet).
        try:
            self._action(
                "datastore_delete",
                json={"resource_id": resource_id, "force": True},
            )
        except Exception:
            pass
        # 3. Kick off the DataPusher+ job. This is async — it returns once the
        #    job is queued, not once the rows have landed in the DataStore.
        self._action("datapusher_submit", json={"resource_id": resource_id})

    def upsert(
        self, resource_id: str, dataframe, primary_key=None, chunk_size: int = 10000
    ) -> None:
        """Incremental load: upsert changed rows by primary key into the CKAN
        DataStore. Ensures the table + PK exist, then upserts in batches.
        No DataPusher+ — this writes to the DataStore API directly."""
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

    def live_fields(self, resource_id: str) -> dict:
        """Return {field_id: ckan_type} for the resource's current DataStore
        table, or {} if it has no DataStore table yet (fresh resource). Drops
        CKAN's internal _id / _full_text fields."""
        try:
            result = self._action(
                "datastore_search", json={"resource_id": resource_id, "limit": 0}
            )
        except Exception:
            return {}  # no datastore table / not datastore-active
        return {
            f["id"]: f["type"]
            for f in result.get("fields", [])
            if not f["id"].startswith("_")
        }

    @staticmethod
    def _infer_fields(dataframe):
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

    def publish_file(self, resource_id: str, local_path: str, filename: str) -> None:
        """Upload a file as a CKAN resource — no DataStore, no DataPusher+.
        Used for blobs (PDFs, GeoTIFFs, images). Streams from the open file
        handle so a large blob isn't buffered in memory (requests reads it in
        chunks and sets Content-Length from the file size)."""
        with open(local_path, "rb") as fh:
            self._action(
                "resource_patch",
                data={"id": resource_id},
                files={"upload": (filename, fh, "application/octet-stream")},
            )


# --------------------------------------------------------------------------
# Cached geocoder (only used when a publisher config sets geocode: true)
# --------------------------------------------------------------------------
class GeocoderResource(dg.ConfigurableResource):
    def geocode_frame(self, dataframe, address_col: str):
        """Add lat/lon columns, caching lookups. Stub."""
        raise NotImplementedError("wire to your cached geocoding backend")
