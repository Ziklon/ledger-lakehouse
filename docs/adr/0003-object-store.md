---
status: accepted
date: 2026-10-31
---

# 0003 — Local object store: Chainguard MinIO pinned by index digest

## Context and problem statement

The brief specifies MinIO as the local S3. In late 2025 MinIO Inc. stopped
publishing community images: `minio/minio` no longer exists on Docker Hub and
`quay.io/minio/minio` has no pullable tags (verified 2026-10-31). We need a
pinned, multi-arch (arm64 + amd64), S3-compatible store.

## Decision drivers

* S3 API fidelity for Iceberg `S3FileIO` and Trino native S3
* Pinned, reproducible artifact; arm64 support (Apple Silicon)
* Minimal bootstrap; low memory
* Zero code difference vs AWS S3 (endpoint + credentials only)

## Considered options

1. **Chainguard MinIO** `cgr.dev/chainguard/minio@sha256:<index digest>`
2. Garage `dxflrs/garage:v2.5.0`
3. RustFS `rustfs/rustfs:1.0.4`
4. SeaweedFS (only a moving `dev` tag is published — cannot pin)

## Decision outcome

Chosen: **option 1**. Same MinIO server (RELEASE.2026-10-23 at time of pinning),
built and signed by Chainguard. The free tier only exposes `latest`, so we pin
the **multi-arch index digest** — stricter than a tag, which can move.

Lesson recorded: the first digest copied was a *platform* manifest (amd64 only)
and ran under emulation on arm64. Always pin the OCI **index** digest
(`docker buildx imagetools inspect <ref>`), which resolves per platform.

The bucket is created by a one-shot `amazon/aws-cli` container (not `mc`), so
the same commands work against real S3.

### Consequences

* Good: well-trodden MinIO + Iceberg path; ships `mc` for a native healthcheck.
* Good: digest pin is immutable and reproducible.
* Bad: no human-readable version in compose; upgrading = deliberately
  re-resolving the digest (documented in the compose comment).
* Bad: depends on Chainguard continuing a free MinIO image. Fallback: Garage
  (option 2) — only the `minio` service and its healthcheck change.

## Confirmation

`make up` reports `minio` healthy (`mc ready local`), `minio-init` exits 0,
and `make smoke` writes Parquet files under `s3://warehouse/`.
