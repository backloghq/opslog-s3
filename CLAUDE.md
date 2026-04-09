# opslog-s3

Amazon S3 storage backend for [@backloghq/opslog](https://github.com/backloghq/opslog). Enables multi-writer concurrency across machines using S3 as the storage layer.

## What It Is

An implementation of opslog's `StorageBackend` interface backed by Amazon S3. All data (snapshots, WAL files, manifests, archives) is stored as S3 objects. Designed for multi-agent scenarios where agents on different machines need to write to the same opslog store.

## Architecture

```
s3://bucket/prefix/
  manifest.json                       # Current snapshot + ops file pointers
  snapshots/
    snap-<timestamp>.json             # Immutable full-state captures
  ops/
    ops-<timestamp>.jsonl             # Single-writer WAL
    agent-<id>-<timestamp>.jsonl      # Per-agent WAL (multi-writer)
  archive/
    archive-<period>.json             # Archived records
  .lock                               # Advisory write lock (JSON with TTL)
  .compact-lock                       # Compaction coordination lock
```

### S3 Semantics Mapping

| opslog operation | S3 strategy |
|---|---|
| Manifest read/write | GetObject / PutObject (atomic for single objects) |
| Snapshot write/load | PutObject / GetObject |
| WAL append | GetObject + concatenate + PutObject (no native append in S3) |
| WAL truncate | GetObject + remove last line + PutObject |
| Lock acquire | PutObject with `IfNoneMatch: *` (conditional create) |
| Lock release | DeleteObject |
| Stale lock recovery | Read lock body, check TTL, delete if expired |
| List files | ListObjectsV2 with prefix |
| Manifest version check | HeadObject → ETag |

### Locking

S3 conditional writes (`IfNoneMatch: *`) provide exclusive-create semantics similar to POSIX `O_CREAT | O_EXCL`. Lock objects contain a TTL timestamp; stale locks from crashed agents are automatically recovered.

## Project Structure

```
src/
  types.ts            # S3BackendOptions interface
  s3-backend.ts       # S3Backend implementing StorageBackend
  index.ts            # Exports
tests/
  mock-s3.ts          # In-memory mock S3 client for testing
  s3-backend.test.ts  # S3Backend unit tests
  store-integration.test.ts  # opslog Store + S3Backend end-to-end tests
```

## Dependencies

- **Runtime**: `@aws-sdk/client-s3` — AWS SDK v3 S3 client
- **Peer**: `@backloghq/opslog` (>=0.3.0) — provides `StorageBackend` interface and validators

## Coding Conventions

- Single runtime dependency (`@aws-sdk/client-s3`)
- All S3 errors handled via error name checks (portable across environments)
- Tests use an in-memory mock S3 (no AWS credentials needed)
- Prefix support for multi-tenant bucket usage

## Release Process

When making changes:
1. Update `CHANGELOG.md` with a new version entry
2. Bump version in `package.json`
3. Run `npm run build && npm run lint && npm test`
4. Commit, push, create PR
5. After merge: `npm publish --access public`
