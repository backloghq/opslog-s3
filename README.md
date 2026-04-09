# opslog-s3

Amazon S3 storage backend for [@backloghq/opslog](https://github.com/backloghq/opslog). Store your event-sourced data in S3 and enable multi-writer concurrency across machines.

## Install

```bash
npm install @backloghq/opslog @backloghq/opslog-s3
```

## Usage

```typescript
import { Store } from "@backloghq/opslog";
import { S3Backend } from "@backloghq/opslog-s3";

const store = new Store<{ title: string; status: string }>();
await store.open("my-store", {
  backend: new S3Backend({
    bucket: "my-bucket",
    prefix: "opslog/my-store",
    region: "us-east-1",
  }),
  agentId: "agent-A",
});

await store.set("task-1", { title: "Build API", status: "active" });
const task = store.get("task-1");
await store.close();
```

Another agent on a different machine can write to the same store:

```typescript
const store = new Store<{ title: string; status: string }>();
await store.open("my-store", {
  backend: new S3Backend({
    bucket: "my-bucket",
    prefix: "opslog/my-store",
    region: "us-east-1",
  }),
  agentId: "agent-B",
});

// Sees agent-A's writes
store.get("task-1"); // { title: "Build API", status: "active" }

// Writes to its own WAL — no contention with agent-A
await store.set("task-2", { title: "Write tests", status: "active" });
await store.close();
```

## Options

```typescript
new S3Backend({
  bucket: "my-bucket",         // S3 bucket name (required)
  prefix: "path/to/store",    // Key prefix (optional, no trailing slash)
  region: "us-east-1",        // AWS region (required if no client provided)
  client: myS3Client,         // Pre-configured S3Client (optional)
  lockTtlMs: 60000,           // Lock TTL in ms (default: 60000)
});
```

### Pre-configured client

Pass your own `S3Client` if you need custom configuration (credentials, endpoint, middleware):

```typescript
import { S3Client } from "@aws-sdk/client-s3";

const client = new S3Client({
  region: "us-east-1",
  endpoint: "http://localhost:9000", // MinIO, LocalStack, etc.
  forcePathStyle: true,
});

const backend = new S3Backend({ bucket: "my-bucket", client });
```

## How It Works

### Storage layout

```
s3://bucket/prefix/
  manifest.json                       # Store metadata
  snapshots/snap-<ts>.json            # Immutable state captures
  ops/agent-<id>-<ts>.jsonl           # Per-agent WAL files
  archive/archive-<period>.json       # Archived records
```

### WAL operations (per-batch objects)

S3 has no native append. Instead of downloading, concatenating, and re-uploading a single growing file, the S3Backend stores each write as a separate S3 object:

```
ops/agent-A-1744200000/
  batch-1744200001-0000.jsonl   (ops from first write)
  batch-1744200002-0000.jsonl   (ops from second write)
  batch-1744200003-0000.jsonl   (ops from third write)
```

- **Append**: single PutObject per write (no download needed)
- **Read**: ListObjectsV2 + parallel GetObject per batch, with incremental caching (only new batches are downloaded on subsequent reads)
- **Truncate**: delete last batch object (or modify if it contains multiple ops)

Stores created with v0.1.0 (single `.jsonl` files) are transparently supported — the backend detects the format automatically.

### Locking

Uses S3 conditional writes (`IfNoneMatch: *`) for lock acquisition — fails atomically if the lock already exists. Lock objects carry a TTL; stale locks from crashed agents are automatically recovered.

### Conflict resolution

When multiple agents write to the same key, opslog's Lamport clock ordering resolves conflicts via last-writer-wins. See [opslog multi-writer docs](https://github.com/backloghq/opslog#multi-writer-mode) for details.

## IAM Permissions

The S3Backend needs these S3 permissions on the bucket/prefix:

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObject",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:ListBucket"
  ],
  "Resource": [
    "arn:aws:s3:::my-bucket",
    "arn:aws:s3:::my-bucket/opslog/*"
  ]
}
```

## Development

```bash
npm run build          # Compile TypeScript
npm run lint           # ESLint
npm test               # Run tests (uses in-memory mock S3, no AWS needed)
npm run test:coverage  # Tests with coverage
```

## License

MIT
