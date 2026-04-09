import { describe, it, expect, beforeEach } from "vitest";
import { Store } from "@backloghq/opslog";
import { S3Backend } from "../src/s3-backend.js";
import { createMockS3 } from "./mock-s3.js";
import type { S3Client } from "@aws-sdk/client-s3";

interface TestRecord {
  name: string;
  status: string;
  value?: number;
}

describe("Store + S3Backend integration", () => {
  let client: S3Client;

  beforeEach(() => {
    ({ client } = createMockS3());
  });

  function makeBackend(prefix = "store") {
    return new S3Backend({ bucket: "test-bucket", prefix, client });
  }

  describe("single-writer", () => {
    it("opens a fresh store, writes, and reads", async () => {
      const store = new Store<TestRecord>();
      await store.open("ignored", {
        backend: makeBackend(),
        checkpointThreshold: 1000,
      });

      await store.set("a", { name: "A", status: "active" });
      await store.set("b", { name: "B", status: "done" });

      expect(store.get("a")?.name).toBe("A");
      expect(store.get("b")?.status).toBe("done");
      expect(store.count()).toBe(2);
      await store.close();
    });

    it("persists state across reopen", async () => {
      const store1 = new Store<TestRecord>();
      await store1.open("ignored", {
        backend: makeBackend(),
        checkpointOnClose: true,
        checkpointThreshold: 1000,
      });
      await store1.set("a", { name: "A", status: "active" });
      await store1.close();

      const store2 = new Store<TestRecord>();
      await store2.open("ignored", { backend: makeBackend() });
      expect(store2.get("a")?.name).toBe("A");
      await store2.close();
    });

    it("undo works", async () => {
      const store = new Store<TestRecord>();
      await store.open("ignored", {
        backend: makeBackend(),
        checkpointThreshold: 1000,
      });
      await store.set("a", { name: "A", status: "active" });
      await store.set("b", { name: "B", status: "active" });

      const undone = await store.undo();
      expect(undone).toBe(true);
      expect(store.has("b")).toBe(false);
      expect(store.has("a")).toBe(true);
      await store.close();
    });

    it("batch operations work", async () => {
      const store = new Store<TestRecord>();
      await store.open("ignored", {
        backend: makeBackend(),
        checkpointThreshold: 1000,
      });
      await store.batch(() => {
        store.set("a", { name: "A", status: "active" });
        store.set("b", { name: "B", status: "active" });
        store.set("c", { name: "C", status: "active" });
      });
      expect(store.count()).toBe(3);
      await store.close();
    });

    it("compaction works", async () => {
      const store = new Store<TestRecord>();
      await store.open("ignored", {
        backend: makeBackend(),
        checkpointThreshold: 1000,
      });
      await store.set("a", { name: "A", status: "active" });
      await store.set("b", { name: "B", status: "active" });
      expect(store.stats().opsCount).toBe(2);

      await store.compact();
      expect(store.stats().opsCount).toBe(0);
      expect(store.count()).toBe(2);
      await store.close();
    });

    it("archive works", async () => {
      const store = new Store<TestRecord>();
      await store.open("ignored", {
        backend: makeBackend(),
        checkpointThreshold: 1000,
      });
      await store.set("a", { name: "A", status: "done" });
      await store.set("b", { name: "B", status: "active" });

      const count = await store.archive(
        (r) => r.status === "done",
        "2026-Q1",
      );
      expect(count).toBe(1);
      expect(store.count()).toBe(1);

      const archived = await store.loadArchive("2026-Q1");
      expect(archived.get("a")?.name).toBe("A");
      await store.close();
    });
  });

  describe("multi-writer", () => {
    it("two agents write to the same store", async () => {
      // Agent A writes
      const storeA = new Store<TestRecord>();
      await storeA.open("ignored", {
        backend: makeBackend(),
        agentId: "agent-A",
        checkpointOnClose: false,
        checkpointThreshold: 1000,
      });
      await storeA.set("a", { name: "from-A", status: "active" });
      await storeA.close();

      // Agent B opens (same mock S3 = same "bucket"), sees A's data
      const storeB = new Store<TestRecord>();
      await storeB.open("ignored", {
        backend: makeBackend(),
        agentId: "agent-B",
        checkpointThreshold: 1000,
      });
      expect(storeB.get("a")?.name).toBe("from-A");
      await storeB.set("b", { name: "from-B", status: "active" });
      await storeB.close();

      // Verify both records via a third agent
      const reader = new Store<TestRecord>();
      await reader.open("ignored", {
        backend: makeBackend(),
        agentId: "reader",
      });
      expect(reader.count()).toBe(2);
      expect(reader.get("a")?.name).toBe("from-A");
      expect(reader.get("b")?.name).toBe("from-B");
      await reader.close();
    });

    it("LWW conflict resolution works over S3", async () => {
      // Agent A sets "shared"
      const storeA = new Store<TestRecord>();
      await storeA.open("ignored", {
        backend: makeBackend(),
        agentId: "agent-A",
        checkpointOnClose: false,
        checkpointThreshold: 1000,
      });
      await storeA.set("shared", { name: "from-A", status: "active" });
      await storeA.close();

      // Agent B sets "shared" — higher clock wins
      const storeB = new Store<TestRecord>();
      await storeB.open("ignored", {
        backend: makeBackend(),
        agentId: "agent-B",
        checkpointOnClose: false,
        checkpointThreshold: 1000,
      });
      await storeB.set("shared", { name: "from-B", status: "done" });
      await storeB.close();

      const reader = new Store<TestRecord>();
      await reader.open("ignored", {
        backend: makeBackend(),
        agentId: "reader",
      });
      expect(reader.get("shared")?.name).toBe("from-B");
      await reader.close();
    });

    it("compaction works in multi-writer mode", async () => {
      const storeA = new Store<TestRecord>();
      await storeA.open("ignored", {
        backend: makeBackend(),
        agentId: "agent-A",
        checkpointOnClose: false,
        checkpointThreshold: 1000,
      });
      await storeA.set("a", { name: "from-A", status: "active" });
      await storeA.close();

      const storeB = new Store<TestRecord>();
      await storeB.open("ignored", {
        backend: makeBackend(),
        agentId: "agent-B",
        checkpointThreshold: 1000,
      });
      await storeB.set("b", { name: "from-B", status: "active" });
      await storeB.compact();

      expect(storeB.stats().opsCount).toBe(0);
      expect(storeB.count()).toBe(2);
      await storeB.close();

      // Verify after compaction
      const reader = new Store<TestRecord>();
      await reader.open("ignored", {
        backend: makeBackend(),
        agentId: "reader",
      });
      expect(reader.count()).toBe(2);
      await reader.close();
    });

    it("per-agent undo works over S3", async () => {
      const storeA = new Store<TestRecord>();
      await storeA.open("ignored", {
        backend: makeBackend(),
        agentId: "agent-A",
        checkpointOnClose: false,
        checkpointThreshold: 1000,
      });
      await storeA.set("a", { name: "from-A", status: "active" });
      await storeA.close();

      const storeB = new Store<TestRecord>();
      await storeB.open("ignored", {
        backend: makeBackend(),
        agentId: "agent-B",
        checkpointOnClose: false,
        checkpointThreshold: 1000,
      });
      await storeB.set("b", { name: "from-B", status: "active" });
      await storeB.undo();

      expect(storeB.has("b")).toBe(false);
      expect(storeB.has("a")).toBe(true);
      await storeB.close();
    });
  });

  describe("read-only mode", () => {
    it("opens an existing S3 store in readOnly", async () => {
      const writer = new Store<TestRecord>();
      await writer.open("ignored", {
        backend: makeBackend(),
        checkpointThreshold: 1000,
      });
      await writer.set("a", { name: "A", status: "active" });
      await writer.close();

      const reader = new Store<TestRecord>();
      await reader.open("ignored", {
        backend: makeBackend(),
        readOnly: true,
      });
      expect(reader.get("a")?.name).toBe("A");
      await reader.close();
    });
  });
});
