# Changelog

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
