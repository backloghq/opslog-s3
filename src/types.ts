import type { S3Client } from "@aws-sdk/client-s3";

export interface S3BackendOptions {
  /** S3 bucket name. */
  bucket: string;
  /** Key prefix within the bucket (e.g. "my-app/store"). No trailing slash. */
  prefix?: string;
  /** AWS region (required if no client provided). */
  region?: string;
  /** Pre-configured S3Client. If omitted, one is created from region. */
  client?: S3Client;
  /** Lock TTL in milliseconds. Locks older than this are considered stale (default: 60000). */
  lockTtlMs?: number;
}
