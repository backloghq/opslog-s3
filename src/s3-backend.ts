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

interface S3LockData {
  agent: string;
  hostname: string;
  pid: number;
  timestamp: number;
  ttlMs: number;
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

  // -- Lifecycle --

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async initialize(dir: string, opts: { readOnly: boolean }): Promise<void> {
    // S3 has no directories to create. dir is ignored (bucket/prefix set in constructor).
  }

  async shutdown(): Promise<void> {
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
    const result = await this.client.send(
      new ListObjectsV2Command({ Bucket: this.bucket, Prefix: prefix }),
    );
    return (result.Contents ?? []).map((obj) => obj.Key!).filter(Boolean);
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
  // S3 has no append — download, concatenate, re-upload.

  async appendOps(relativePath: string, ops: Operation[]): Promise<void> {
    const key = this.key(relativePath);
    const existing = (await this.getObjectOrNull(key)) ?? "";
    const lines = ops.map((op) => JSON.stringify(op)).join("\n") + "\n";
    await this.putObject(key, existing + lines, "application/x-ndjson");
  }

  async readOps(relativePath: string): Promise<Operation[]> {
    const content = await this.getObjectOrNull(this.key(relativePath));
    if (content === null || content.trim().length === 0) return [];
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
        `opslog-s3: skipped ${skipped} malformed line(s) in ${relativePath}`,
      );
    }
    return ops;
  }

  async truncateLastOp(relativePath: string): Promise<boolean> {
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

  async createOpsFile(): Promise<string> {
    const filename = `ops-${Date.now()}.jsonl`;
    const relativePath = `ops/${filename}`;
    await this.putObject(
      this.key(relativePath),
      "",
      "application/x-ndjson",
    );
    return relativePath;
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
    const filename = `agent-${agentId}-${Date.now()}.jsonl`;
    const relativePath = `ops/${filename}`;
    await this.putObject(
      this.key(relativePath),
      "",
      "application/x-ndjson",
    );
    return relativePath;
  }

  async listOpsFiles(): Promise<string[]> {
    const keys = await this.listKeys(this.key("ops/"));
    return keys
      .filter((k) => k.endsWith(".jsonl"))
      .map((k) => this.relativeFromKey(k));
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
}
