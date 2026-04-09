/**
 * Real S3 integration tests.
 * Skipped unless S3_TEST_BUCKET is set.
 *
 * Usage:
 *   S3_TEST_BUCKET=my-test-bucket \
 *   S3_TEST_REGION=us-east-1 \
 *   AWS_PROFILE=my-profile \
 *   npm run test:integration
 */
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import {
  S3Client,
  ListObjectsV2Command,
  DeleteObjectsCommand,
} from "@aws-sdk/client-s3";
import { fromNodeProviderChain } from "@aws-sdk/credential-providers";
import { Store } from "@backloghq/opslog";
import type { Manifest, Operation } from "@backloghq/opslog";
import { S3Backend } from "../src/s3-backend.js";

const bucket = process.env.S3_TEST_BUCKET;
const region = process.env.S3_TEST_REGION ?? "us-east-1";
const endpoint = process.env.S3_TEST_ENDPOINT;
const profile = process.env.AWS_PROFILE;

interface TestRecord {
  name: string;
  status: string;
  value?: number;
}

describe.skipIf(!bucket)("S3Backend (real S3)", () => {
  let client: S3Client;
  let prefix: string;

  function makeBackend(pfx?: string) {
    return new S3Backend({
      bucket: bucket!,
      prefix: pfx ?? prefix,
      client,
    });
  }

  beforeAll(() => {
    client = new S3Client({
      region,
      ...(endpoint ? { endpoint, forcePathStyle: true } : {}),
      ...(profile ? { credentials: fromNodeProviderChain({ profile }) } : {}),
    });
    prefix = `opslog-test-${Date.now()}`;
  });

  afterAll(async () => {
    // Clean up all objects under the test prefix
    if (!client || !bucket) return;
    let continuationToken: string | undefined;
    do {
      const list = await client.send(
        new ListObjectsV2Command({
          Bucket: bucket,
          Prefix: prefix,
          ContinuationToken: continuationToken,
        }),
      );
      if (list.Contents && list.Contents.length > 0) {
        await client.send(
          new DeleteObjectsCommand({
            Bucket: bucket,
            Delete: {
              Objects: list.Contents.map((o) => ({ Key: o.Key! })),
            },
          }),
        );
      }
      continuationToken = list.IsTruncated
        ? list.NextContinuationToken
        : undefined;
    } while (continuationToken);
    client.destroy();
  });

  describe("manifest", () => {
    it("returns null when no manifest exists", async () => {
      const backend = makeBackend(`${prefix}/manifest-test-empty`);
      await backend.initialize("", { readOnly: false });
      expect(await backend.readManifest()).toBeNull();
    });

    it("writes and reads a manifest", async () => {
      const backend = makeBackend(`${prefix}/manifest-test-rw`);
      await backend.initialize("", { readOnly: false });
      const manifest: Manifest = {
        version: 1,
        currentSnapshot: "snapshots/snap-1.json",
        activeOps: "ops/ops-1.jsonl",
        archiveSegments: [],
        stats: {
          activeRecords: 0,
          archivedRecords: 0,
          opsCount: 0,
          created: "2026-01-01T00:00:00Z",
          lastCheckpoint: "2026-01-01T00:00:00Z",
        },
      };
      await backend.writeManifest(manifest);
      const loaded = await backend.readManifest();
      expect(loaded).toEqual(manifest);
    });

    it("getManifestVersion returns ETag", async () => {
      const backend = makeBackend(`${prefix}/manifest-test-ver`);
      await backend.initialize("", { readOnly: false });
      expect(await backend.getManifestVersion()).toBeNull();
      await backend.writeManifest({
        version: 1,
        currentSnapshot: "s",
        activeOps: "o",
        archiveSegments: [],
        stats: {
          activeRecords: 0,
          archivedRecords: 0,
          opsCount: 0,
          created: "t",
          lastCheckpoint: "t",
        },
      });
      const ver = await backend.getManifestVersion();
      expect(ver).not.toBeNull();
    });
  });

  describe("snapshots", () => {
    it("writes and loads a snapshot", async () => {
      const backend = makeBackend(`${prefix}/snap-test`);
      await backend.initialize("", { readOnly: false });
      const records = new Map<string, unknown>([
        ["a", { x: 1 }],
        ["b", { x: 2 }],
      ]);
      const path = await backend.writeSnapshot(records, 1);
      const loaded = await backend.loadSnapshot(path);
      expect(loaded.version).toBe(1);
      expect(loaded.records.get("a")).toEqual({ x: 1 });
      expect(loaded.records.get("b")).toEqual({ x: 2 });
    });
  });

  describe("WAL", () => {
    it("creates, appends, reads, and truncates", async () => {
      const backend = makeBackend(`${prefix}/wal-test`);
      await backend.initialize("", { readOnly: false });

      const path = await backend.createOpsFile();
      expect(await backend.readOps(path)).toEqual([]);

      const ops: Operation[] = [
        { ts: "1", op: "set", id: "a", data: { x: 1 }, prev: null },
        { ts: "2", op: "set", id: "b", data: { x: 2 }, prev: null },
      ];
      await backend.appendOps(path, ops);
      let loaded = await backend.readOps(path);
      expect(loaded).toHaveLength(2);
      expect(loaded[0].id).toBe("a");

      // Append more
      await backend.appendOps(path, [
        { ts: "3", op: "set", id: "c", data: { x: 3 }, prev: null },
      ]);
      loaded = await backend.readOps(path);
      expect(loaded).toHaveLength(3);

      // Truncate last
      expect(await backend.truncateLastOp(path)).toBe(true);
      loaded = await backend.readOps(path);
      expect(loaded).toHaveLength(2);
      expect(loaded[1].id).toBe("b");
    });
  });

  describe("archive", () => {
    it("writes, merges, loads, and lists", async () => {
      const backend = makeBackend(`${prefix}/archive-test`);
      await backend.initialize("", { readOnly: false });

      await backend.writeArchiveSegment(
        "2026-Q1",
        new Map([["a", { x: 1 }]]),
      );
      await backend.writeArchiveSegment(
        "2026-Q1",
        new Map([["b", { x: 2 }]]),
      );
      const loaded = await backend.loadArchiveSegment(
        "archive/archive-2026-Q1.json",
      );
      expect(loaded.size).toBe(2);

      await backend.writeArchiveSegment(
        "2026-Q2",
        new Map([["c", { x: 3 }]]),
      );
      const segments = await backend.listArchiveSegments();
      expect(segments).toHaveLength(2);
    });
  });

  describe("locking", () => {
    it("acquires, prevents double acquire, and releases", async () => {
      const backend = makeBackend(`${prefix}/lock-test`);
      await backend.initialize("", { readOnly: false });

      const handle = await backend.acquireLock();
      await expect(backend.acquireLock()).rejects.toThrow("locked");
      await backend.releaseLock(handle);

      // Re-acquire after release
      const h2 = await backend.acquireLock();
      await backend.releaseLock(h2);
    });

    it("acquires and releases compaction lock", async () => {
      const backend = makeBackend(`${prefix}/compact-lock-test`);
      await backend.initialize("", { readOnly: false });

      const handle = await backend.acquireCompactionLock();
      await expect(backend.acquireCompactionLock()).rejects.toThrow("locked");
      await backend.releaseCompactionLock(handle);
    });
  });

  describe("Store lifecycle (single-writer)", () => {
    it("opens, writes, compacts, closes, and reopens", async () => {
      const pfx = `${prefix}/store-sw`;
      const store1 = new Store<TestRecord>();
      await store1.open("ignored", {
        backend: makeBackend(pfx),
        checkpointThreshold: 1000,
      });
      await store1.set("a", { name: "A", status: "active" });
      await store1.set("b", { name: "B", status: "done" });
      await store1.compact();
      await store1.set("c", { name: "C", status: "active" });
      await store1.close();

      const store2 = new Store<TestRecord>();
      await store2.open("ignored", { backend: makeBackend(pfx) });
      expect(store2.count()).toBe(3);
      expect(store2.get("a")?.name).toBe("A");
      expect(store2.get("c")?.name).toBe("C");
      await store2.close();
    });
  });

  describe("Store multi-writer", () => {
    it("two agents write and read each other's data", async () => {
      const pfx = `${prefix}/store-mw`;

      const storeA = new Store<TestRecord>();
      await storeA.open("ignored", {
        backend: makeBackend(pfx),
        agentId: "agent-A",
        checkpointOnClose: false,
        checkpointThreshold: 1000,
      });
      await storeA.set("a", { name: "from-A", status: "active" });
      await storeA.close();

      const storeB = new Store<TestRecord>();
      await storeB.open("ignored", {
        backend: makeBackend(pfx),
        agentId: "agent-B",
        checkpointThreshold: 1000,
      });
      expect(storeB.get("a")?.name).toBe("from-A");
      await storeB.set("b", { name: "from-B", status: "active" });
      await storeB.close();

      const reader = new Store<TestRecord>();
      await reader.open("ignored", {
        backend: makeBackend(pfx),
        agentId: "reader",
      });
      expect(reader.count()).toBe(2);
      expect(reader.get("a")?.name).toBe("from-A");
      expect(reader.get("b")?.name).toBe("from-B");
      await reader.close();
    });

    it("LWW conflict resolution", async () => {
      const pfx = `${prefix}/store-lww`;

      const storeA = new Store<TestRecord>();
      await storeA.open("ignored", {
        backend: makeBackend(pfx),
        agentId: "agent-A",
        checkpointOnClose: false,
        checkpointThreshold: 1000,
      });
      await storeA.set("key", { name: "from-A", status: "active" });
      await storeA.close();

      const storeB = new Store<TestRecord>();
      await storeB.open("ignored", {
        backend: makeBackend(pfx),
        agentId: "agent-B",
        checkpointOnClose: false,
        checkpointThreshold: 1000,
      });
      await storeB.set("key", { name: "from-B", status: "done" });
      await storeB.close();

      const reader = new Store<TestRecord>();
      await reader.open("ignored", {
        backend: makeBackend(pfx),
        agentId: "reader",
      });
      expect(reader.get("key")?.name).toBe("from-B");
      await reader.close();
    });
  });

  describe("prefix isolation", () => {
    it("two stores with different prefixes do not interfere", async () => {
      const pfxA = `${prefix}/isolated-A`;
      const pfxB = `${prefix}/isolated-B`;

      const storeA = new Store<TestRecord>();
      await storeA.open("ignored", {
        backend: makeBackend(pfxA),
        checkpointThreshold: 1000,
      });
      await storeA.set("x", { name: "from-A", status: "active" });
      await storeA.close();

      const storeB = new Store<TestRecord>();
      await storeB.open("ignored", {
        backend: makeBackend(pfxB),
        checkpointThreshold: 1000,
      });
      expect(storeB.count()).toBe(0);
      await storeB.set("y", { name: "from-B", status: "active" });
      await storeB.close();

      // Re-check A hasn't been polluted
      const readerA = new Store<TestRecord>();
      await readerA.open("ignored", { backend: makeBackend(pfxA) });
      expect(readerA.count()).toBe(1);
      expect(readerA.has("y")).toBe(false);
      await readerA.close();
    });
  });
});
