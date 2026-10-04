import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import { hostname } from "node:os";
import type { DatabaseSync } from "node:sqlite";
import { readSqliteWalState } from "../infra/sqlite-wal-checkpoint.js";
import { getFileLockProcessStartTime, isPidDefinitelyDead } from "../shared/pid-alive.js";
import { VERSION } from "../version.js";

let boot: string | null | undefined;

export function agentDatabaseLeaseStaleReason(row: {
  owner_pid: number;
  owner_start_time: number | null;
}): "owner-pid-dead" | "owner-start-time-changed" | undefined {
  if (isPidDefinitelyDead(row.owner_pid)) {
    return "owner-pid-dead";
  }
  const currentStartTime = getFileLockProcessStartTime(row.owner_pid);
  return row.owner_start_time !== null &&
    currentStartTime !== null &&
    row.owner_start_time !== currentStartTime
    ? "owner-start-time-changed"
    : undefined;
}

function leaseProvenance(pathname: string): string | undefined {
  try {
    if (boot === undefined) {
      // procfs supplies both a boot identity and the namespace in which PID death is observed.
      const bootId =
        process.platform === "linux"
          ? fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim()
          : "";
      boot = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(bootId)
        ? [hostname(), bootId, fs.readlinkSync("/proc/self/ns/pid"), VERSION].join("\0")
        : null;
    }
    if (!boot) {
      return undefined;
    }
    const file = fs.statSync(pathname, { bigint: true });
    return file.isFile()
      ? createHash("sha256").update(`${boot}\0${file.dev}:${file.ino}`).digest("hex")
      : undefined;
  } catch {
    return undefined;
  }
}

/** The opaque lease key retains provenance without changing the released lease table. */
export function createAgentDatabaseLeaseId(pathname: string): string {
  const provenance = leaseProvenance(pathname);
  return provenance ? `process-v1:${provenance}:${randomUUID()}` : randomUUID();
}

export function isSameBootAgentDatabaseLease(leaseId: string, pathname: string): boolean {
  const provenance = leaseProvenance(pathname);
  return provenance !== undefined && leaseId.startsWith(`process-v1:${provenance}:`);
}

/** SQLite recovers committed WAL frames; these checks do not scan table or index contents. */
export function canDeferAgentDatabaseIntegrity(database: DatabaseSync, pathname: string): boolean {
  try {
    const wal = fs.statSync(`${pathname}-wal`, { throwIfNoEntry: false });
    const journal = fs.statSync(`${pathname}-journal`, { throwIfNoEntry: false });
    if (!wal?.isFile() || wal.size < 32 || (journal && journal.size > 0)) {
      return false;
    }
    // sqlite-allow-raw -- Admission validates the recovered header and journal once.
    const pageSize = database.prepare("PRAGMA page_size").get()?.page_size;
    const checkpoint = readSqliteWalState(database);
    return (
      typeof pageSize === "number" &&
      pageSize >= 512 &&
      pageSize <= 65536 &&
      (pageSize & (pageSize - 1)) === 0 &&
      typeof database.prepare("PRAGMA schema_version").get()?.schema_version === "number" &&
      database.prepare("PRAGMA journal_mode").get()?.journal_mode === "wal" &&
      checkpoint?.busy === 0 &&
      typeof checkpoint.log === "number" &&
      checkpoint.log >= 0 &&
      typeof checkpoint.checkpointed === "number" &&
      checkpoint.checkpointed >= 0 &&
      checkpoint.checkpointed <= checkpoint.log
    );
  } catch {
    return false;
  }
}
