# Changelog

## 0.1.1 (2026-04-09)

### Changed
- **Per-batch S3 objects** — WAL writes now create one S3 object per `appendOps` call instead of downloading, concatenating, and re-uploading a single growing file. Each write is a single PutObject (no GetObject needed). Reduces write latency from ~200-400ms (2 round-trips) to ~50-200ms (1 round-trip) and eliminates O(n) data transfer per write.
- **Incremental read caching** — `readOps()` caches previously downloaded batches. Subsequent reads (e.g., `refresh()`) only download new batch objects via ListObjectsV2 + parallel GetObject. Turns O(all ops) reads into O(new ops since last read).
- **Parallel batch downloads** — when reading ops, all batch objects are fetched concurrently via `Promise.all`.

### Added
- **Backward compatibility** — stores created with v0.1.0 (single `.jsonl` files) are transparently supported. The backend detects the format by path extension (`.jsonl` = legacy, no extension = per-batch).
- WAL-specific tests: per-batch creation, incremental reads, single-op batch deletion, multi-op batch truncation, legacy format compat.

## 0.1.0 (2026-04-09)

Initial release.

### Added
- **S3Backend** — `StorageBackend` implementation for Amazon S3
  - Manifest, snapshot, archive read/write via GetObject/PutObject
  - WAL append via download-concatenate-upload
  - WAL truncate via download-modify-upload
  - Locking via S3 conditional writes (`IfNoneMatch: *`) with TTL-based stale recovery
  - Multi-writer extensions: per-agent ops files, compaction lock, manifest version check via ETag
  - Configurable key prefix for multi-tenant bucket usage
  - Pre-configured S3Client injection support
- **In-memory mock S3 client** for testing without AWS credentials
- **39 tests** covering all backend methods and Store integration (single-writer, multi-writer, conflict resolution, compaction, undo)
