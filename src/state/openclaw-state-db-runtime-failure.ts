import { isSqliteCorruptionError } from "../infra/sqlite-error-diagnostics.js";
import { throwSqliteLifecycleErrors } from "../infra/sqlite-lifecycle-errors.js";
import { isSqliteSchemaVersionError } from "../infra/sqlite-user-version.js";
import type { CachedOpenClawStateDatabase } from "./openclaw-state-db-cache.types.js";
import type { OpenClawStateDatabase } from "./openclaw-state-db-contract.js";
import { markOpenClawStateDatabaseFailure } from "./openclaw-state-db-failure.js";

type FailureOwner = {
  cachedDatabases: Map<string, CachedOpenClawStateDatabase>;
  evict(database: OpenClawStateDatabase): boolean;
  recordSchemaFailure(pathname: string, error: Error): void;
  invalidate(pathname: string): void;
  notifyTerminalFailure(pathname: string, error: Error): void;
};

/** Classify actual admission/query failures against the cache's exact native owner. */
export function createOpenClawStateDatabaseRuntimeFailureOwner(owner: FailureOwner) {
  return {
    closeTerminalFailure(pathname: string, error: Error): void {
      markOpenClawStateDatabaseFailure(error, pathname);
      owner.invalidate(pathname);
      const cached = owner.cachedDatabases.get(pathname);
      const errors: unknown[] = [];
      try {
        if (cached) {
          owner.evict(cached);
        }
      } catch (cleanupError) {
        errors.push(cleanupError);
      }
      try {
        owner.notifyTerminalFailure(pathname, error);
      } catch (notificationError) {
        errors.push(notificationError);
      }
      throwSqliteLifecycleErrors(errors, "Terminal shared-state failure cleanup failed");
    },
    classify(database: OpenClawStateDatabase, error: unknown): Error | undefined {
      const failure = error instanceof Error ? error : new Error(String(error));
      if (isSqliteCorruptionError(failure)) {
        owner.evict(database);
        return undefined;
      }
      if (isSqliteSchemaVersionError(failure)) {
        owner.recordSchemaFailure(database.path, failure);
      }
      return failure;
    },
  };
}
