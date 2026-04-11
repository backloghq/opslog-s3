import { describe, it, expect, beforeEach } from "vitest";
import type { Manifest } from "@backloghq/opslog";
import { S3Backend } from "../src/s3-backend.js";
import { createMockS3, type MockS3Store } from "./mock-s3.js";

describe("S3Backend", () => {
  let backend: S3Backend;
  let mockStore: MockS3Store;

  beforeEach(() => {
    const { client, store } = createMockS3();
    mockStore = store;
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
        activeOps: "ops/wal-1",
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
        activeOps: "ops/wal-1",
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
      expect(ver!.startsWith('"')).toBe(true);
    });
  });

  describe("snapshots", () => {
    beforeEach(async () => {
      await backend.initialize("", { readOnly: false });
    });

    it("writes JSONL snapshot and loads it back", async () => {
      const records = new Map<string, unknown>([
        ["a", { x: 1 }],
        ["b", { x: 2 }],
      ]);
      const path = await backend.writeSnapshot(records, 1);
      expect(path).toMatch(/^snapshots\/snap-\d+\.jsonl$/);

      const loaded = await backend.loadSnapshot(path);
      expect(loaded.version).toBe(1);
      expect(loaded.records.get("a")).toEqual({ x: 1 });
      expect(loaded.records.get("b")).toEqual({ x: 2 });
    });

    it("JSONL snapshot has correct line structure", async () => {
      const records = new Map<string, unknown>([
        ["a", { name: "Alice" }],
        ["b", { name: "Bob" }],
      ]);
      await backend.writeSnapshot(records, 1);

      // Read raw content from mock S3 (keys are prefixed with "store/")
      let content = "";
      for (const [, obj] of mockStore.objects) {
        if (obj.contentType === "application/x-jsonlines") {
          content = obj.body;
          break;
        }
      }
      const lines = content.trim().split("\n");
      expect(lines.length).toBe(3); // header + 2 records

      const header = JSON.parse(lines[0]);
      expect(header.version).toBe(1);
      expect(header.timestamp).toBeTruthy();
      expect("records" in header).toBe(false);

      const rec = JSON.parse(lines[1]);
      expect(rec.id).toBeTruthy();
      expect(rec.data).toBeTruthy();
    });

    it("loads legacy JSON snapshot (backward compat)", async () => {
      // Write a legacy monolithic JSON snapshot directly to mock S3
      const legacySnapshot = JSON.stringify({
        version: 1,
        timestamp: new Date().toISOString(),
        records: { x: { val: 10 }, y: { val: 20 } },
      });
      mockStore.objects.set("store/snapshots/snap-legacy.json", {
        body: legacySnapshot,
        etag: '"legacy"',
        contentType: "application/json",
      });

      const loaded = await backend.loadSnapshot("snapshots/snap-legacy.json");
      expect(loaded.version).toBe(1);
      expect(loaded.records.get("x")).toEqual({ val: 10 });
      expect(loaded.records.get("y")).toEqual({ val: 20 });
    });

    it("throws on missing snapshot", async () => {
      await expect(
        backend.loadSnapshot("snapshots/nonexistent.json"),
      ).rejects.toThrow();
    });
  });

  describe("WAL (per-batch objects)", () => {
    beforeEach(async () => {
      await backend.initialize("", { readOnly: false });
    });

    it("creates a virtual ops path (no S3 object)", async () => {
      const path = await backend.createOpsFile();
      expect(path).toMatch(/^ops\/wal-\d+$/);
      expect(path).not.toContain(".jsonl");
      // readOps returns empty — no batches yet
      const ops = await backend.readOps(path);
      expect(ops).toEqual([]);
    });

    it("appendOps writes a single batch object (no download)", async () => {
      const { client: c, store: s } = createMockS3();
      const b = new S3Backend({ bucket: "b", prefix: "p", client: c });
      await b.initialize("", { readOnly: false });

      const path = await b.createOpsFile();
      await b.appendOps(path, [
        { ts: "1", op: "set", id: "a", data: { x: 1 }, prev: null },
      ]);

      // Should have exactly one batch object under the path prefix
      const batchKeys = Array.from(s.objects.keys()).filter((k) =>
        k.includes("/batch-"),
      );
      expect(batchKeys).toHaveLength(1);
    });

    it("appends and reads ops across multiple batches", async () => {
      const path = await backend.createOpsFile();
      await backend.appendOps(path, [
        { ts: "1", op: "set", id: "a", data: { x: 1 }, prev: null },
      ]);
      await backend.appendOps(path, [
        { ts: "2", op: "set", id: "b", data: { x: 2 }, prev: null },
      ]);
      await backend.appendOps(path, [
        { ts: "3", op: "set", id: "c", data: { x: 3 }, prev: null },
      ]);

      const ops = await backend.readOps(path);
      expect(ops).toHaveLength(3);
      expect(ops[0].id).toBe("a");
      expect(ops[1].id).toBe("b");
      expect(ops[2].id).toBe("c");
    });

    it("appends batch of multiple ops in one call", async () => {
      const path = await backend.createOpsFile();
      await backend.appendOps(path, [
        { ts: "1", op: "set", id: "a", data: { x: 1 }, prev: null },
        { ts: "2", op: "set", id: "b", data: { x: 2 }, prev: null },
      ]);
      const ops = await backend.readOps(path);
      expect(ops).toHaveLength(2);
    });

    it("incremental read: only downloads new batches", async () => {
      const { client: c } = createMockS3();
      const b = new S3Backend({ bucket: "b", prefix: "p", client: c });
      await b.initialize("", { readOnly: false });

      const path = await b.createOpsFile();
      await b.appendOps(path, [
        { ts: "1", op: "set", id: "a", data: { x: 1 }, prev: null },
      ]);

      // First read — downloads 1 batch
      const ops1 = await b.readOps(path);
      expect(ops1).toHaveLength(1);

      // Append another batch
      await b.appendOps(path, [
        { ts: "2", op: "set", id: "b", data: { x: 2 }, prev: null },
      ]);

      // Second read — should include both ops, only downloading the new batch
      const ops2 = await b.readOps(path);
      expect(ops2).toHaveLength(2);
      expect(ops2[0].id).toBe("a");
      expect(ops2[1].id).toBe("b");

      // Third read with no changes — cache hit, no downloads
      const ops3 = await b.readOps(path);
      expect(ops3).toHaveLength(2);
    });

    it("truncates last op from single-op batch (deletes object)", async () => {
      const { client: c, store: s } = createMockS3();
      const b = new S3Backend({ bucket: "b", prefix: "p", client: c });
      await b.initialize("", { readOnly: false });

      const path = await b.createOpsFile();
      await b.appendOps(path, [
        { ts: "1", op: "set", id: "a", data: { x: 1 }, prev: null },
      ]);
      await b.appendOps(path, [
        { ts: "2", op: "set", id: "b", data: { x: 2 }, prev: null },
      ]);

      expect(await b.truncateLastOp(path)).toBe(true);
      const ops = await b.readOps(path);
      expect(ops).toHaveLength(1);
      expect(ops[0].id).toBe("a");

      // The batch object for "b" should be deleted
      const batchKeys = Array.from(s.objects.keys()).filter((k) =>
        k.includes("/batch-"),
      );
      expect(batchKeys).toHaveLength(1);
    });

    it("truncates last op from multi-op batch (modifies object)", async () => {
      const path = await backend.createOpsFile();
      await backend.appendOps(path, [
        { ts: "1", op: "set", id: "a", data: { x: 1 }, prev: null },
        { ts: "2", op: "set", id: "b", data: { x: 2 }, prev: null },
      ]);

      expect(await backend.truncateLastOp(path)).toBe(true);
      const ops = await backend.readOps(path);
      expect(ops).toHaveLength(1);
      expect(ops[0].id).toBe("a");
    });

    it("truncateLastOp returns false for empty WAL", async () => {
      const path = await backend.createOpsFile();
      expect(await backend.truncateLastOp(path)).toBe(false);
    });

    it("readOps returns empty for missing path", async () => {
      expect(await backend.readOps("ops/nonexistent")).toEqual([]);
    });
  });

  describe("WAL (legacy single-file backward compat)", () => {
    beforeEach(async () => {
      await backend.initialize("", { readOnly: false });
    });

    it("reads and appends to legacy .jsonl files", async () => {
      const { client: c, store: s } = createMockS3();
      const b = new S3Backend({ bucket: "b", prefix: "p", client: c });
      await b.initialize("", { readOnly: false });

      // Simulate a v0.1.0 ops file
      const legacyPath = "ops/ops-123.jsonl";
      s.objects.set(`p/${legacyPath}`, {
        body: '{"ts":"1","op":"set","id":"a","data":{"x":1},"prev":null}\n',
        etag: '"e"',
      });

      const ops = await b.readOps(legacyPath);
      expect(ops).toHaveLength(1);
      expect(ops[0].id).toBe("a");

      // Append to legacy file
      await b.appendOps(legacyPath, [
        { ts: "2", op: "set", id: "b", data: { x: 2 }, prev: null },
      ]);
      const ops2 = await b.readOps(legacyPath);
      expect(ops2).toHaveLength(2);
    });

    it("truncates legacy .jsonl files", async () => {
      const { client: c, store: s } = createMockS3();
      const b = new S3Backend({ bucket: "b", prefix: "p", client: c });
      await b.initialize("", { readOnly: false });

      const legacyPath = "ops/ops-123.jsonl";
      s.objects.set(`p/${legacyPath}`, {
        body: '{"ts":"1","op":"set","id":"a","data":{"x":1},"prev":null}\n{"ts":"2","op":"set","id":"b","data":{"x":2},"prev":null}\n',
        etag: '"e"',
      });

      expect(await b.truncateLastOp(legacyPath)).toBe(true);
      const ops = await b.readOps(legacyPath);
      expect(ops).toHaveLength(1);
      expect(ops[0].id).toBe("a");
    });

    it("skips malformed lines in legacy files", async () => {
      const { client: c, store: s } = createMockS3();
      const b = new S3Backend({ bucket: "b", prefix: "p", client: c });
      await b.initialize("", { readOnly: false });

      s.objects.set("p/ops/bad.jsonl", {
        body: '{"ts":"1","op":"set","id":"a","data":{"x":1},"prev":null}\ngarbage\n{"ts":"2","op":"set","id":"b","data":{"x":2},"prev":null}\n',
        etag: '"e"',
      });
      const ops = await b.readOps("ops/bad.jsonl");
      expect(ops).toHaveLength(2);
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
      s.objects.set("p/.lock", { body: "not-json{{{", etag: '"e"' });
      const handle = await b.acquireLock();
      expect(handle).toBeDefined();
      await b.releaseLock(handle);
    });

    it("recovers stale lock (expired TTL)", async () => {
      const { client } = createMockS3();
      const shortTtl = new S3Backend({
        bucket: "test-bucket",
        prefix: "store",
        client,
        lockTtlMs: 1,
      });
      await shortTtl.initialize("", { readOnly: false });

      const h1 = await shortTtl.acquireLock();
      expect(h1).toBeDefined();

      await new Promise((r) => setTimeout(r, 10));

      const h2 = await shortTtl.acquireLock();
      expect(h2).toBeDefined();
      await shortTtl.releaseLock(h2);
    });
  });

  describe("multi-writer extensions", () => {
    beforeEach(async () => {
      await backend.initialize("", { readOnly: false });
    });

    it("creates agent-specific ops paths (batch format)", async () => {
      const pathA = await backend.createAgentOpsFile("agent-A");
      const pathB = await backend.createAgentOpsFile("agent-B");
      expect(pathA).toMatch(/^ops\/agent-agent-A-\d+$/);
      expect(pathB).toMatch(/^ops\/agent-agent-B-\d+$/);
      expect(pathA).not.toContain(".jsonl");
    });

    it("lists all ops paths (batch and legacy)", async () => {
      const { client: c, store: s } = createMockS3();
      const b = new S3Backend({ bucket: "b", prefix: "p", client: c });
      await b.initialize("", { readOnly: false });

      // Create batch-format WALs
      const walPath = await b.createOpsFile();
      await b.appendOps(walPath, [
        { ts: "1", op: "set", id: "a", data: { x: 1 }, prev: null },
      ]);
      const agentPath = await b.createAgentOpsFile("A");
      await b.appendOps(agentPath, [
        { ts: "2", op: "set", id: "b", data: { x: 2 }, prev: null },
      ]);

      // Add a legacy file
      s.objects.set("p/ops/old.jsonl", { body: "", etag: '"e"' });

      const files = await b.listOpsFiles();
      expect(files).toHaveLength(3);
      expect(files).toContain("ops/old.jsonl");
      expect(files).toContain(walPath);
      expect(files).toContain(agentPath);
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
        activeOps: "ops/wal-1",
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
      const { client } = createMockS3();
      const noPrefix = new S3Backend({ bucket: "b", client });
      await noPrefix.initialize("", { readOnly: false });
      const path = await noPrefix.createOpsFile();
      await noPrefix.appendOps(path, [
        { ts: "1", op: "set", id: "a", data: { x: 1 }, prev: null },
      ]);
      const ops = await noPrefix.readOps(path);
      expect(ops).toHaveLength(1);
    });
  });

  describe("readBlobRange", () => {
    beforeEach(async () => {
      await backend.initialize("", { readOnly: false });
    });

    it("reads byte range from S3 object", async () => {
      await backend.writeBlob("range-test.txt", Buffer.from("Hello, World!"));
      const buf = await backend.readBlobRange("range-test.txt", 0, 5);
      expect(buf.toString("utf-8")).toBe("Hello");
    });

    it("reads middle of file", async () => {
      await backend.writeBlob("range-test.txt", Buffer.from("Hello, World!"));
      const buf = await backend.readBlobRange("range-test.txt", 7, 5);
      expect(buf.toString("utf-8")).toBe("World");
    });

    it("works with JSONL record store pattern", async () => {
      const records = [
        JSON.stringify({ _id: "a", title: "First" }),
        JSON.stringify({ _id: "b", title: "Second" }),
        JSON.stringify({ _id: "c", title: "Third" }),
      ];
      const content = records.join("\n") + "\n";
      await backend.writeBlob("records.jsonl", Buffer.from(content));

      // Build offset index
      let offset = 0;
      const offsets: Array<{ offset: number; length: number }> = [];
      for (const line of records) {
        const len = Buffer.byteLength(line, "utf-8");
        offsets.push({ offset, length: len });
        offset += len + 1;
      }

      // Read second record by offset
      const buf = await backend.readBlobRange("records.jsonl", offsets[1].offset, offsets[1].length);
      const record = JSON.parse(buf.toString("utf-8"));
      expect(record._id).toBe("b");
      expect(record.title).toBe("Second");
    });

    it("returns empty buffer for zero length", async () => {
      await backend.writeBlob("data.txt", Buffer.from("content"));
      const buf = await backend.readBlobRange("data.txt", 0, 0);
      expect(buf.length).toBe(0);
    });

    it("throws on negative offset or length", async () => {
      await backend.writeBlob("neg.txt", Buffer.from("data"));
      await expect(backend.readBlobRange("neg.txt", -1, 5)).rejects.toThrow("non-negative");
      await expect(backend.readBlobRange("neg.txt", 0, -1)).rejects.toThrow("non-negative");
    });
  });
});
