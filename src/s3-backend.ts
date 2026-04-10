import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
} from "@aws-sdk/client-s3";
import { hostname } from "node:os";
import type {
  StorageBackend,
  LockHandle,
  Manifest,
  Operation,
} from "@backloghq/opslog";
import {
  validateManifest,
  validateSnapshot,
  validateArchiveSegment,
  validateOp,
} from "@backloghq/opslog";
import type { S3BackendOptions } from "./types.js";

const DEFAULT_LOCK_TTL_MS = 60_000;

function isNotFound(err: unknown): boolean {
  if (err instanceof Error) {
    const name = (err as Error & { name: string }).name;
    return (
      name === "NoSuchKey" ||
      name === "NotFound" ||
      (err as Error & { $metadata?: { httpStatusCode?: number } }).$metadata
        ?.httpStatusCode === 404
    );
  }
  return false;
}

function isPreconditionFailed(err: unknown): boolean {
  if (err instanceof Error) {
    const name = (err as Error & { name: string }).name;
    return (
      name === "PreconditionFailed" ||
      (err as Error & { $metadata?: { httpStatusCode?: number } }).$metadata
        ?.httpStatusCode === 412
    );
  }
  return false;
}

function parseOpsFromContent(content: string, source: string): Operation[] {
  const lines = content.trim().split("\n").filter(Boolean);
  const ops: Operation[] = [];
  let skipped = 0;
  for (const line of lines) {
    try {
      ops.push(validateOp(JSON.parse(line)));
    } catch {
      skipped++;
    }
  }
  if (skipped > 0) {
    console.error(
      `opslog-s3: skipped ${skipped} malformed line(s) in ${source}`,
    );
  }
  return ops;
}

interface S3LockData {
  agent: string;
  hostname: string;
  pid: number;
  timestamp: number;
  ttlMs: number;
}

interface OpsCache {
  ops: Operation[];
  lastBatchKey: string;
}

class S3LockHandle implements LockHandle {
  constructor(readonly lockKey: string) {}
}

/** Amazon S3 storage backend for opslog. */
export class S3Backend implements StorageBackend {
  private client: S3Client;
  private ownsClient: boolean;
  private bucket: string;
  private prefix: string;
  private lockTtlMs: number;
  private batchCounter = 0;
  private opsCache = new Map<string, OpsCache>();

  constructor(options: S3BackendOptions) {
    this.bucket = options.bucket;
    this.prefix = options.prefix ?? "";
    this.lockTtlMs = options.lockTtlMs ?? DEFAULT_LOCK_TTL_MS;

    if (options.client) {
      this.client = options.client;
      this.ownsClient = false;
    } else {
      this.client = new S3Client({ region: options.region });
      this.ownsClient = true;
    }
  }

  private key(relativePath: string): string {
    return this.prefix ? `${this.prefix}/${relativePath}` : relativePath;
  }

  private relativeFromKey(fullKey: string): string {
    return this.prefix ? fullKey.slice(this.prefix.length + 1) : fullKey;
  }

  /**
   * Paths ending with .jsonl are legacy single-file WALs (v0.1.0).
   * Paths without .jsonl use per-batch objects (v0.1.1+).
   */
  private isBatchFormat(relativePath: string): boolean {
    return !relativePath.endsWith(".jsonl");
  }

  private nextBatchKey(relativePath: string): string {
    const ts = Date.now();
    const seq = String(this.batchCounter++).padStart(4, "0");
    return this.key(`${relativePath}/batch-${ts}-${seq}.jsonl`);
  }

  // -- Lifecycle --

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async initialize(dir: string, opts: { readOnly: boolean }): Promise<void> {
    // S3 has no directories to create. dir is ignored (bucket/prefix set in constructor).
  }

  async shutdown(): Promise<void> {
    this.opsCache.clear();
    if (this.ownsClient) {
      this.client.destroy();
    }
  }

  // -- S3 helpers --

  private async putObject(
    key: string,
    body: string,
    contentType: string,
    ifNoneMatch?: string,
  ): Promise<string> {
    const result = await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        Body: body,
        ContentType: contentType,
        ...(ifNoneMatch ? { IfNoneMatch: ifNoneMatch } : {}),
      }),
    );
    return result.ETag ?? "";
  }

  private async getObject(key: string): Promise<string> {
    const result = await this.client.send(
      new GetObjectCommand({ Bucket: this.bucket, Key: key }),
    );
    return (await result.Body?.transformToString()) ?? "";
  }

  private async getObjectOrNull(key: string): Promise<string | null> {
    try {
      return await this.getObject(key);
    } catch (err) {
      if (isNotFound(err)) return null;
      throw err;
    }
  }

  private async deleteObject(key: string): Promise<void> {
    await this.client.send(
      new DeleteObjectCommand({ Bucket: this.bucket, Key: key }),
    );
  }

  private async listKeys(prefix: string): Promise<string[]> {
    const keys: string[] = [];
    let continuationToken: string | undefined;
    do {
      const result = await this.client.send(
        new ListObjectsV2Command({
          Bucket: this.bucket,
          Prefix: prefix,
          ContinuationToken: continuationToken,
        }),
      );
      for (const obj of result.Contents ?? []) {
        if (obj.Key) keys.push(obj.Key);
      }
      continuationToken = result.IsTruncated
        ? result.NextContinuationToken
        : undefined;
    } while (continuationToken);
    return keys;
  }

  /** List batch keys under a WAL prefix, sorted lexicographically. */
  private async listBatchKeys(relativePath: string): Promise<string[]> {
    const prefix = this.key(relativePath) + "/";
    const keys = await this.listKeys(prefix);
    return keys.filter((k) => k.endsWith(".jsonl")).sort();
  }

  /** Download multiple batch objects in parallel and parse ops from each. */
  private async downloadBatches(batchKeys: string[]): Promise<Operation[]> {
    const contents = await Promise.all(
      batchKeys.map((k) => this.getObject(k)),
    );
    const ops: Operation[] = [];
    for (let i = 0; i < contents.length; i++) {
      if (contents[i].trim().length > 0) {
        ops.push(...parseOpsFromContent(contents[i], batchKeys[i]));
      }
    }
    return ops;
  }

  // -- Manifest --

  async readManifest(): Promise<Manifest | null> {
    const content = await this.getObjectOrNull(this.key("manifest.json"));
    if (content === null) return null;
    return validateManifest(JSON.parse(content));
  }

  async writeManifest(manifest: Manifest): Promise<void> {
    await this.putObject(
      this.key("manifest.json"),
      JSON.stringify(manifest, null, 2),
      "application/json",
    );
  }

  // -- Snapshots --

  async writeSnapshot(
    records: Map<string, unknown>,
    version: number,
  ): Promise<string> {
    const timestamp = new Date().toISOString();
    const filename = `snap-${Date.now()}.json`;
    const relativePath = `snapshots/${filename}`;
    const snapshot = {
      version,
      timestamp,
      records: Object.fromEntries(records),
    };
    await this.putObject(
      this.key(relativePath),
      JSON.stringify(snapshot, null, 2),
      "application/json",
    );
    return relativePath;
  }

  async loadSnapshot(
    relativePath: string,
  ): Promise<{ records: Map<string, unknown>; version: number }> {
    const content = await this.getObject(this.key(relativePath));
    const snapshot = validateSnapshot(JSON.parse(content));
    return {
      records: new Map(Object.entries(snapshot.records)),
      version: snapshot.version,
    };
  }

  // -- WAL --
  // v0.1.1+: per-batch S3 objects. Each appendOps is a single PutObject (no download).
  // v0.1.0 legacy: single .jsonl file (download-append-upload).

  async appendOps(relativePath: string, ops: Operation[]): Promise<void> {
    if (!this.isBatchFormat(relativePath)) {
      // Legacy single-file format (v0.1.0 backward compat)
      const key = this.key(relativePath);
      const existing = (await this.getObjectOrNull(key)) ?? "";
      const lines = ops.map((op) => JSON.stringify(op)).join("\n") + "\n";
      await this.putObject(key, existing + lines, "application/x-ndjson");
      return;
    }

    // Per-batch: single PutObject, no download needed
    const batchKey = this.nextBatchKey(relativePath);
    const lines = ops.map((op) => JSON.stringify(op)).join("\n") + "\n";
    await this.putObject(batchKey, lines, "application/x-ndjson");

    // Update cache if present
    const cached = this.opsCache.get(relativePath);
    if (cached) {
      cached.ops.push(...ops);
      cached.lastBatchKey = batchKey;
    }
  }

  async readOps(relativePath: string): Promise<Operation[]> {
    if (!this.isBatchFormat(relativePath)) {
      // Legacy single-file format
      const content = await this.getObjectOrNull(this.key(relativePath));
      if (content === null || content.trim().length === 0) return [];
      return parseOpsFromContent(content, relativePath);
    }

    // Per-batch: list batches, download (with incremental caching)
    const allBatchKeys = await this.listBatchKeys(relativePath);
    if (allBatchKeys.length === 0) return [];

    const cached = this.opsCache.get(relativePath);
    if (cached) {
      const newKeys = allBatchKeys.filter((k) => k > cached.lastBatchKey);
      if (newKeys.length === 0) return [...cached.ops];

      // Only download new batches
      const newOps = await this.downloadBatches(newKeys);
      cached.ops.push(...newOps);
      cached.lastBatchKey = allBatchKeys.at(-1)!;
      return [...cached.ops];
    }

    // First read: download all batches in parallel
    const ops = await this.downloadBatches(allBatchKeys);
    this.opsCache.set(relativePath, {
      ops: [...ops],
      lastBatchKey: allBatchKeys.at(-1)!,
    });
    return ops;
  }

  async truncateLastOp(relativePath: string): Promise<boolean> {
    if (!this.isBatchFormat(relativePath)) {
      // Legacy single-file format
      const key = this.key(relativePath);
      const content = await this.getObjectOrNull(key);
      if (content === null || content.trim().length === 0) return false;
      const lines = content.trimEnd().split("\n");
      if (lines.length <= 1) {
        await this.putObject(key, "", "application/x-ndjson");
        return true;
      }
      lines.pop();
      await this.putObject(
        key,
        lines.join("\n") + "\n",
        "application/x-ndjson",
      );
      return true;
    }

    // Per-batch: find last batch, modify or delete it
    const batchKeys = await this.listBatchKeys(relativePath);
    if (batchKeys.length === 0) return false;

    const lastKey = batchKeys.at(-1)!;
    const content = await this.getObject(lastKey);
    const lines = content.trimEnd().split("\n").filter(Boolean);

    if (lines.length <= 1) {
      // Single op in this batch — delete the whole object
      await this.deleteObject(lastKey);
    } else {
      // Multiple ops — remove last line, re-upload
      lines.pop();
      await this.putObject(
        lastKey,
        lines.join("\n") + "\n",
        "application/x-ndjson",
      );
    }

    // Invalidate cache (state changed)
    this.opsCache.delete(relativePath);
    return true;
  }

  async createOpsFile(): Promise<string> {
    // Per-batch format: return a virtual prefix (no S3 object created)
    return `ops/wal-${Date.now()}`;
  }

  // -- Archive --

  async writeArchiveSegment(
    period: string,
    records: Map<string, unknown>,
  ): Promise<string> {
    const filename = `archive-${period}.json`;
    const relativePath = `archive/${filename}`;
    const key = this.key(relativePath);

    // Merge with existing archive if present
    let existing: Record<string, unknown> = {};
    const content = await this.getObjectOrNull(key);
    if (content !== null) {
      const parsed = validateArchiveSegment(JSON.parse(content));
      existing = parsed.records;
    }

    const merged = { ...existing, ...Object.fromEntries(records) };
    const segment = {
      version: 1,
      period,
      timestamp: new Date().toISOString(),
      records: merged,
    };
    await this.putObject(
      key,
      JSON.stringify(segment, null, 2),
      "application/json",
    );
    return relativePath;
  }

  async loadArchiveSegment(
    relativePath: string,
  ): Promise<Map<string, unknown>> {
    const content = await this.getObject(this.key(relativePath));
    const segment = validateArchiveSegment(JSON.parse(content));
    return new Map(Object.entries(segment.records));
  }

  async listArchiveSegments(): Promise<string[]> {
    const keys = await this.listKeys(this.key("archive/"));
    return keys
      .filter((k) => k.endsWith(".json"))
      .map((k) => this.relativeFromKey(k));
  }

  // -- Locking (S3 conditional writes) --

  private async tryAcquireLockKey(lockKey: string): Promise<LockHandle> {
    const body: S3LockData = {
      agent: `pid-${process.pid}`,
      hostname: hostname(),
      pid: process.pid,
      timestamp: Date.now(),
      ttlMs: this.lockTtlMs,
    };

    try {
      await this.putObject(
        lockKey,
        JSON.stringify(body),
        "application/json",
        "*", // IfNoneMatch — fails if key exists
      );
      return new S3LockHandle(lockKey);
    } catch (err) {
      if (!isPreconditionFailed(err)) throw err;
    }

    // Lock exists — check if stale
    const content = await this.getObjectOrNull(lockKey);
    if (content !== null) {
      try {
        const lock: S3LockData = JSON.parse(content);
        if (Date.now() - lock.timestamp > lock.ttlMs) {
          // Stale — delete and retry
          await this.deleteObject(lockKey);
          return this.tryAcquireLockKey(lockKey);
        }
      } catch {
        // Corrupted lock body — treat as stale
        await this.deleteObject(lockKey);
        return this.tryAcquireLockKey(lockKey);
      }
    }

    throw new Error(
      `Store is locked by another agent. Lock key: ${lockKey}`,
    );
  }

  async acquireLock(): Promise<LockHandle> {
    return this.tryAcquireLockKey(this.key(".lock"));
  }

  async releaseLock(handle: LockHandle): Promise<void> {
    const s3Handle = handle as S3LockHandle;
    await this.deleteObject(s3Handle.lockKey);
  }

  // -- Multi-writer extensions --

  async createAgentOpsFile(agentId: string): Promise<string> {
    // Per-batch format: return a virtual prefix (no S3 object created)
    return `ops/agent-${agentId}-${Date.now()}`;
  }

  async listOpsFiles(): Promise<string[]> {
    const keys = await this.listKeys(this.key("ops/"));
    // Collect unique WAL prefixes: both legacy .jsonl files and batch directories
    const paths = new Set<string>();
    for (const k of keys) {
      const rel = this.relativeFromKey(k);
      if (rel.endsWith(".jsonl") && !rel.includes("/batch-")) {
        // Legacy single-file WAL
        paths.add(rel);
      } else if (rel.includes("/batch-")) {
        // Per-batch: extract prefix (everything before /batch-)
        const prefix = rel.slice(0, rel.indexOf("/batch-"));
        paths.add(prefix);
      }
    }
    return Array.from(paths);
  }

  async acquireCompactionLock(): Promise<LockHandle> {
    return this.tryAcquireLockKey(this.key(".compact-lock"));
  }

  async releaseCompactionLock(handle: LockHandle): Promise<void> {
    await this.releaseLock(handle);
  }

  async getManifestVersion(): Promise<string | null> {
    try {
      const result = await this.client.send(
        new HeadObjectCommand({
          Bucket: this.bucket,
          Key: this.key("manifest.json"),
        }),
      );
      return result.ETag ?? null;
    } catch (err) {
      if (isNotFound(err)) return null;
      throw err;
    }
  }

  // -- Blob storage --

  async writeBlob(relativePath: string, content: Buffer): Promise<void> {
    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: this.key(relativePath),
        Body: content,
      }),
    );
  }

  async readBlob(relativePath: string): Promise<Buffer> {
    const result = await this.client.send(
      new GetObjectCommand({
        Bucket: this.bucket,
        Key: this.key(relativePath),
      }),
    );
    return Buffer.from(await result.Body!.transformToByteArray());
  }

  async listBlobs(prefix: string): Promise<string[]> {
    const fullPrefix = this.key(prefix) + "/";
    const result = await this.client.send(
      new ListObjectsV2Command({
        Bucket: this.bucket,
        Prefix: fullPrefix,
      }),
    );
    return (result.Contents ?? [])
      .map((obj) => obj.Key!.slice(fullPrefix.length))
      .filter((name) => name.length > 0 && !name.includes("/"));
  }

  async deleteBlob(relativePath: string): Promise<void> {
    await this.client.send(
      new DeleteObjectCommand({
        Bucket: this.bucket,
        Key: this.key(relativePath),
      }),
    );
  }

  async deleteBlobDir(prefix: string): Promise<void> {
    const blobs = await this.listBlobs(prefix);
    for (const name of blobs) {
      await this.deleteBlob(`${prefix}/${name}`);
    }
  }
}
