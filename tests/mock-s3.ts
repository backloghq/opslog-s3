/**
 * In-memory mock S3 client for testing.
 * Simulates PutObject, GetObject, HeadObject, DeleteObject, ListObjectsV2.
 * Supports conditional writes (IfNoneMatch: "*").
 */
import {
  PutObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
} from "@aws-sdk/client-s3";
import type { S3Client } from "@aws-sdk/client-s3";

interface StoredObject {
  body: string;
  etag: string;
  contentType?: string;
}

export interface MockS3Store {
  objects: Map<string, StoredObject>;
}

function makeError(name: string, message: string, statusCode: number): Error {
  const err = new Error(message);
  err.name = name;
  Object.assign(err, { $metadata: { httpStatusCode: statusCode } });
  return err;
}

let etagCounter = 0;

export function createMockS3(): { client: S3Client; store: MockS3Store } {
  const store: MockS3Store = { objects: new Map() };

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const send = async (command: any): Promise<any> => {
    if (command instanceof PutObjectCommand) {
      const { Key, Body, ContentType, IfNoneMatch } = command.input;
      if (!Key) throw new Error("PutObject: Key required");

      if (IfNoneMatch === "*" && store.objects.has(Key)) {
        throw makeError(
          "PreconditionFailed",
          "At least one of the pre-conditions you specified did not hold",
          412,
        );
      }

      const etag = `"etag-${++etagCounter}"`;
      store.objects.set(Key, {
        body: String(Body ?? ""),
        etag,
        contentType: ContentType,
      });
      return { ETag: etag };
    }

    if (command instanceof GetObjectCommand) {
      const { Key, Range } = command.input;
      const obj = store.objects.get(Key!);
      if (!obj) {
        throw makeError(
          "NoSuchKey",
          "The specified key does not exist.",
          404,
        );
      }
      let body = obj.body;
      // Support Range header for byte-range reads
      if (Range) {
        const match = Range.match(/^bytes=(\d+)-(\d+)$/);
        if (match) {
          const start = parseInt(match[1], 10);
          const end = parseInt(match[2], 10);
          body = body.slice(start, end + 1);
        }
      }
      return {
        Body: {
          transformToString: async () => body,
          transformToByteArray: async () => Buffer.from(body),
        },
        ETag: obj.etag,
        ContentType: obj.contentType,
      };
    }

    if (command instanceof HeadObjectCommand) {
      const { Key } = command.input;
      const obj = store.objects.get(Key!);
      if (!obj) {
        throw makeError("NotFound", "Not Found", 404);
      }
      return { ETag: obj.etag, ContentType: obj.contentType };
    }

    if (command instanceof DeleteObjectCommand) {
      const { Key } = command.input;
      store.objects.delete(Key!);
      return {};
    }

    if (command instanceof ListObjectsV2Command) {
      const { Prefix } = command.input;
      const prefix = Prefix ?? "";
      const contents = Array.from(store.objects.keys())
        .filter((key) => key.startsWith(prefix))
        .sort()
        .map((key) => ({ Key: key }));
      return {
        Contents: contents.length > 0 ? contents : undefined,
        KeyCount: contents.length,
      };
    }

    throw new Error(`Unmocked S3 command: ${command.constructor.name}`);
  };

  const client = { send, destroy: () => {} } as unknown as S3Client;
  return { client, store };
}
