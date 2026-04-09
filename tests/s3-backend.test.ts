import { describe, it, expect, beforeEach } from "vitest";
import type { Manifest, Operation } from "@backloghq/opslog";
import { S3Backend } from "../src/s3-backend.js";
import { createMockS3 } from "./mock-s3.js";

describe("S3Backend", () => {
  let backend: S3Backend;

  beforeEach(() => {
    const { client } = createMockS3();
    backend = new S3Backend({ bucket: "test-bucket", prefix: "store", client });
  });

  describe("initialize / shutdown", () => {
    it("initialize is a no-op for S3", async () => {
      await expect(
        backend.initialize("ignored", { readOnly: false }),
      ).resolves.toBeUndefined();
    });

    it("shutdown does not throw", async () => {
      await expect(backend.shutdown()).resolves.toBeUndefined();
    });
  });

  describe("manifest", () => {
    it("returns null when no manifest exists", async () => {
      await backend.initialize("", { readOnly: false });
      expect(await backend.readManifest()).toBeNull();
    });

    it("writes and reads a manifest", async () => {
      await backend.initialize("", { readOnly: false });
      const manifest: Manifest = {
        version: 1,
        currentSnapshot: "snapshots/snap-1.json",
        activeOps: "ops/ops-1.jsonl",
        archiveSegments: [],
        stats: {
          activeRecords: 5,
          archivedRecords: 0,
          opsCount: 3,
          created: "2026-01-01T00:00:00Z",
          lastCheckpoint: "2026-01-01T00:00:00Z",
        },
      };
      await backend.writeManifest(manifest);
      const loaded = await backend.readManifest();
      expect(loaded).toEqual(manifest);
    });

    it("getManifestVersion returns ETag after write", async () => {
      await backend.initialize("", { readOnly: false });
      expect(await backend.getManifestVersion()).toBeNull();

      await backend.writeManifest({
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
      });
      const ver = await backend.getManifestVersion();
      expect(ver).not.toBeNull();
      expect(ver!.startsWith('"')).toBe(true); // ETag format
    });
  });

  describe("snapshots", () => {
    beforeEach(async () => {
      await backend.initialize("", { readOnly: false });
    });

    it("writes and loads a snapshot", async () => {
      const records = new Map<string, unknown>([
        ["a", { x: 1 }],
        ["b", { x: 2 }],
      ]);
      const path = await backend.writeSnapshot(records, 1);
      expect(path).toMatch(/^snapshots\/snap-\d+\.json$/);

      const loaded = await backend.loadSnapshot(path);
      expect(loaded.version).toBe(1);
      expect(loaded.records.get("a")).toEqual({ x: 1 });
      expect(loaded.records.get("b")).toEqual({ x: 2 });
    });

    it("throws on missing snapshot", async () => {
      await expect(
        backend.loadSnapshot("snapshots/nonexistent.json"),
      ).rejects.toThrow();
    });
  });

  describe("WAL", () => {
    beforeEach(async () => {
      await backend.initialize("", { readOnly: false });
    });

    it("creates an empty ops file", async () => {
      const path = await backend.createOpsFile();
      expect(path).toMatch(/^ops\/ops-\d+\.jsonl$/);
      const ops = await backend.readOps(path);
      expect(ops).toEqual([]);
    });

    it("appends and reads ops", async () => {
      const path = await backend.createOpsFile();
      const ops: Operation[] = [
        { ts: "1", op: "set", id: "a", data: { x: 1 }, prev: null },
        { ts: "2", op: "set", id: "b", data: { x: 2 }, prev: null },
      ];
      await backend.appendOps(path, ops);

      const loaded = await backend.readOps(path);
      expect(loaded).toHaveLength(2);
      expect(loaded[0].id).toBe("a");
      expect(loaded[1].id).toBe("b");
    });

    it("appends multiple times to the same file", async () => {
      const path = await backend.createOpsFile();
      await backend.appendOps(path, [
        { ts: "1", op: "set", id: "a", data: { x: 1 }, prev: null },
      ]);
      await backend.appendOps(path, [
        { ts: "2", op: "set", id: "b", data: { x: 2 }, prev: null },
      ]);
      const loaded = await backend.readOps(path);
      expect(loaded).toHaveLength(2);
    });

    it("truncates the last op", async () => {
      const path = await backend.createOpsFile();
      await backend.appendOps(path, [
        { ts: "1", op: "set", id: "a", data: { x: 1 }, prev: null },
        { ts: "2", op: "set", id: "b", data: { x: 2 }, prev: null },
      ]);
      const result = await backend.truncateLastOp(path);
      expect(result).toBe(true);

      const loaded = await backend.readOps(path);
      expect(loaded).toHaveLength(1);
      expect(loaded[0].id).toBe("a");
    });

    it("truncates the only op to empty", async () => {
      const path = await backend.createOpsFile();
      await backend.appendOps(path, [
        { ts: "1", op: "set", id: "a", data: { x: 1 }, prev: null },
      ]);
      expect(await backend.truncateLastOp(path)).toBe(true);
      expect(await backend.readOps(path)).toEqual([]);
    });

    it("truncateLastOp returns false for missing file", async () => {
      expect(await backend.truncateLastOp("ops/nonexistent.jsonl")).toBe(
        false,
      );
    });

    it("readOps returns empty for missing file", async () => {
      expect(await backend.readOps("ops/nonexistent.jsonl")).toEqual([]);
    });

    it("skips malformed lines in ops file", async () => {
      const { client: c, store: s } = createMockS3();
      const b = new S3Backend({ bucket: "b", prefix: "p", client: c });
      await b.initialize("", { readOnly: false });
      // Write a mix of valid and invalid lines directly
      s.objects.set("p/ops/bad.jsonl", {
        body: '{"ts":"1","op":"set","id":"a","data":{"x":1},"prev":null}\ngarbage\n{"ts":"2","op":"set","id":"b","data":{"x":2},"prev":null}\n',
        etag: '"e"',
      });
      const ops = await b.readOps("ops/bad.jsonl");
      expect(ops).toHaveLength(2);
      expect(ops[0].id).toBe("a");
      expect(ops[1].id).toBe("b");
    });
  });

  describe("archive", () => {
    beforeEach(async () => {
      await backend.initialize("", { readOnly: false });
    });

    it("writes and loads an archive segment", async () => {
      const records = new Map<string, unknown>([["a", { x: 1 }]]);
      const path = await backend.writeArchiveSegment("2026-Q1", records);
      expect(path).toBe("archive/archive-2026-Q1.json");

      const loaded = await backend.loadArchiveSegment(path);
      expect(loaded.get("a")).toEqual({ x: 1 });
    });

    it("merges archive segments on write", async () => {
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
      expect(loaded.get("a")).toEqual({ x: 1 });
      expect(loaded.get("b")).toEqual({ x: 2 });
    });

    it("lists archive segments", async () => {
      await backend.writeArchiveSegment(
        "2026-Q1",
        new Map([["a", { x: 1 }]]),
      );
      await backend.writeArchiveSegment(
        "2026-Q2",
        new Map([["b", { x: 2 }]]),
      );
      const segments = await backend.listArchiveSegments();
      expect(segments).toHaveLength(2);
      expect(segments).toContain("archive/archive-2026-Q1.json");
      expect(segments).toContain("archive/archive-2026-Q2.json");
    });

    it("returns empty list when no archives", async () => {
      expect(await backend.listArchiveSegments()).toEqual([]);
    });
  });

  describe("locking", () => {
    beforeEach(async () => {
      await backend.initialize("", { readOnly: false });
    });

    it("acquires and releases a write lock", async () => {
      const handle = await backend.acquireLock();
      expect(handle).toBeDefined();
      await backend.releaseLock(handle);
    });

    it("prevents double lock acquisition", async () => {
      const handle = await backend.acquireLock();
      await expect(backend.acquireLock()).rejects.toThrow("locked");
      await backend.releaseLock(handle);
    });

    it("allows re-acquisition after release", async () => {
      const h1 = await backend.acquireLock();
      await backend.releaseLock(h1);
      const h2 = await backend.acquireLock();
      await backend.releaseLock(h2);
    });

    it("recovers corrupted lock body", async () => {
      const { client: c, store: s } = createMockS3();
      const b = new S3Backend({ bucket: "b", prefix: "p", client: c });
      await b.initialize("", { readOnly: false });
      // Write a lock with unparseable body
      s.objects.set("p/.lock", { body: "not-json{{{", etag: '"e"' });
      const handle = await b.acquireLock();
      expect(handle).toBeDefined();
      await b.releaseLock(handle);
    });

    it("recovers stale lock (expired TTL)", async () => {
      // Create a backend with very short TTL
      const { client } = createMockS3();
      const shortTtl = new S3Backend({
        bucket: "test-bucket",
        prefix: "store",
        client,
        lockTtlMs: 1, // 1ms TTL
      });
      await shortTtl.initialize("", { readOnly: false });

      // Acquire lock, wait for it to expire
      const h1 = await shortTtl.acquireLock();
      expect(h1).toBeDefined();

      // Wait for TTL to pass
      await new Promise((r) => setTimeout(r, 10));

      // Should recover stale lock
      const h2 = await shortTtl.acquireLock();
      expect(h2).toBeDefined();
      await shortTtl.releaseLock(h2);
    });
  });

  describe("multi-writer extensions", () => {
    beforeEach(async () => {
      await backend.initialize("", { readOnly: false });
    });

    it("creates agent-specific ops files", async () => {
      const pathA = await backend.createAgentOpsFile("agent-A");
      const pathB = await backend.createAgentOpsFile("agent-B");
      expect(pathA).toMatch(/^ops\/agent-agent-A-\d+\.jsonl$/);
      expect(pathB).toMatch(/^ops\/agent-agent-B-\d+\.jsonl$/);
    });

    it("lists all ops files", async () => {
      await backend.createOpsFile();
      await backend.createAgentOpsFile("A");
      await backend.createAgentOpsFile("B");
      const files = await backend.listOpsFiles();
      expect(files).toHaveLength(3);
    });

    it("acquires and releases compaction lock", async () => {
      const handle = await backend.acquireCompactionLock();
      expect(handle).toBeDefined();
      await backend.releaseCompactionLock(handle);
    });

    it("prevents double compaction lock", async () => {
      const handle = await backend.acquireCompactionLock();
      await expect(backend.acquireCompactionLock()).rejects.toThrow("locked");
      await backend.releaseCompactionLock(handle);
    });
  });

  describe("prefix handling", () => {
    it("stores objects under the configured prefix", async () => {
      const { client, store } = createMockS3();
      const prefixed = new S3Backend({
        bucket: "b",
        prefix: "my/store",
        client,
      });
      await prefixed.initialize("", { readOnly: false });
      await prefixed.writeManifest({
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
      });
      expect(store.objects.has("my/store/manifest.json")).toBe(true);
    });

    it("works without prefix", async () => {
      const { client, store } = createMockS3();
      const noPrefix = new S3Backend({ bucket: "b", client });
      await noPrefix.initialize("", { readOnly: false });
      await noPrefix.createOpsFile();
      const keys = Array.from(store.objects.keys());
      expect(keys.some((k) => k.startsWith("ops/"))).toBe(true);
    });
  });
});
