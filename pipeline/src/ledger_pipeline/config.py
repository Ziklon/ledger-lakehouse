"""Settings from the environment (the same .env as the stack).

The stack's .env holds in-network addresses (trino:8080, iceberg-rest:8181).
From the host (make run-day) we reach the same services through published
ports, so PIPELINE_* overrides default to localhost + HOST_*_PORT. Inside the
compose network (Airflow, milestone f) set PIPELINE_* to the service names.
Same pattern as GEN_PG_HOST in the generator.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"missing environment variable {name}")
    return value


@dataclass(frozen=True)
class Settings:
    trino_host: str
    trino_port: int
    trino_user: str
    catalog_type: str
    iceberg_rest_uri: str
    s3_endpoint: str
    s3_region: str
    s3_access_key: str
    s3_secret_key: str
    dbt_dir: Path
    dbt_target: str

    @classmethod
    def from_env(cls) -> Settings:
        repo = Path(__file__).resolve().parents[3]
        return cls(
            trino_host=_env("PIPELINE_TRINO_HOST", "localhost"),
            trino_port=int(_env("PIPELINE_TRINO_PORT", _env("HOST_TRINO_PORT", "18080"))),
            trino_user=_env("PIPELINE_TRINO_USER", "pipeline"),
            catalog_type=_env("ICEBERG_CATALOG_TYPE", "rest"),
            iceberg_rest_uri=_env(
                "PIPELINE_ICEBERG_REST_URI",
                f"http://localhost:{_env('HOST_ICEBERG_REST_PORT', '8181')}",
            ),
            s3_endpoint=_env(
                "PIPELINE_S3_ENDPOINT", f"http://localhost:{_env('HOST_MINIO_PORT', '9000')}"
            ),
            s3_region=_env("S3_REGION", "us-east-1"),
            s3_access_key=_env("S3_ACCESS_KEY"),
            s3_secret_key=_env("S3_SECRET_KEY"),
            dbt_dir=Path(_env("PIPELINE_DBT_DIR", str(repo / "dbt"))),
            dbt_target=_env("DBT_TARGET", "local"),
        )

    def iceberg_catalog_properties(self) -> dict[str, str]:
        """pyiceberg catalog config. REST locally; glue in Phase 3 (config only)."""
        props = {
            "type": self.catalog_type,
            "s3.region": self.s3_region,
        }
        if self.catalog_type == "rest":
            props["uri"] = self.iceberg_rest_uri
        if self.s3_endpoint:  # empty on AWS: real S3 + IAM credentials
            props.update(
                {
                    "s3.endpoint": self.s3_endpoint,
                    "s3.access-key-id": self.s3_access_key,
                    "s3.secret-access-key": self.s3_secret_key,
                }
            )
        return props
