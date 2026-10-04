import { randomUUID } from "node:crypto";
import fs from "node:fs";
import { note } from "../../packages/terminal-core/src/note.js";
import {
  applySessionEntryLifecycleMutation,
  ensureTranscriptGenerationsForCanonicalRepair,
  loadCanonicalSessionRepairEntries,
  rehomeSessionDeliveryReferencesForCanonicalRepairBatch,
} from "../config/sessions/session-accessor.js";
import { loadCanonicalRepairEntriesFromDatabase } from "../config/sessions/session-accessor.sqlite-canonical-inventory.js";
import { setCanonicalSqliteSessionMainKey } from "../config/sessions/session-canonical-key.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { resolveTargetSqliteOptions } from "../infra/session-sqlite-migration-readers.js";
import {
  assertExistingDatabaseIdentity,
  readDatabasePathIdentitySync,
} from "../infra/sqlite-worker-identity.js";
import { ensureSessionEntryValidityProjection } from "../state/openclaw-agent-db-session-migrations.js";
import {
  openOpenClawAgentDatabase,
  runOpenClawAgentWriteTransaction,
} from "../state/openclaw-agent-db.js";
import { backupDoctorSqliteDatabases } from "./doctor-migration-backup.js";
import {
  collectCanonicalSessionRepairs,
  listCanonicalSessionStores,
  selectCanonicalSessionCandidate,
  type CanonicalSessionCandidate,
  type CanonicalSessionCandidateFact,
} from "./doctor-session-canonical-candidates.js";
import {
  assertRetainedSnapshotsMatchCanonicalEntry,
  finishCanonicalRetainedOwner,
} from "./doctor-session-canonical-retained.js";
import {
  applyCanonicalDestinationArtifacts,
  createCanonicalDestinationRemovals,
  listCanonicalDestinationAliasKeys,
} from "./doctor-session-canonical-state.js";
import { repairCanonicalSessionGroup } from "./doctor-session-canonical-transfer.js";
import {
  withDoctorSqliteMaintenanceLock,
  type DoctorSqliteMaintenanceAuthority,
} from "./doctor-sqlite-maintenance-lock.js";

export type CanonicalSessionKeyRepairReport = {
  archivedTranscriptDirectories: string[];
  foundGroups: number;
  repairBatches: number;
  removedRows: number;
  repairedGroups: number;
  scannedStores: number;
};

const CANONICAL_SESSION_REPAIR_BATCH_GROUP_LIMIT = 64;

function hydrateCanonicalSessionCandidate(
  fact: CanonicalSessionCandidateFact,
  loaded: ReturnType<typeof loadCanonicalSessionRepairEntries>[number],
): CanonicalSessionCandidate {
  const location = {
    agentId: fact.agentId,
    canonicalKey: fact.canonicalKey,
    ownerEvidenceOnly: fact.ownerEvidenceOnly,
    sessionKey: fact.sessionKey,
    sqlitePath: fact.sqlitePath,
    storePath: fact.storePath,
    inventoryFact: fact.inventoryFact,
  };
  if (loaded.kind === "retained") {
    return { ...location, ...loaded };
  }
  const entry = { ...loaded.entry };
  if (fact.normalizedParentSessionKey) {
    entry.parentSessionKey = fact.normalizedParentSessionKey;
  } else {
    delete entry.parentSessionKey;
  }
  if (fact.normalizedSpawnedBy) {
    entry.spawnedBy = fact.normalizedSpawnedBy;
  } else {
    delete entry.spawnedBy;
  }
  if (entry.forkSource && fact.normalizedForkSourceSessionKey) {
    entry.forkSource = {
      ...entry.forkSource,
      sessionKey: fact.normalizedForkSourceSessionKey,
    };
  } else if (entry.forkSource?.sessionKey !== undefined) {
    // A present but empty-normalized key cannot survive strict runtime validation. Missing
    // legacy keys remain untouched so unrelated repair does not erase independent provenance.
    const { sessionKey: _invalidSessionKey, ...forkProvenance } = entry.forkSource;
    entry.forkSource = forkProvenance as typeof entry.forkSource;
  }
  const candidate = {
    ...location,
    kind: "session" as const,
    entry,
    expectedEntry: loaded.entry,
  };
  return loaded.rawEntryJson !== undefined
    ? {
        ...candidate,
        rawEntryJson: loaded.rawEntryJson,
        rawSnapshotRevision: loaded.rawSnapshotRevision,
      }
    : candidate;
}

function hydrateCanonicalSessionCandidates(
  facts: readonly CanonicalSessionCandidateFact[],
): CanonicalSessionCandidate[] {
  const loaded = new Map<
    CanonicalSessionCandidateFact,
    ReturnType<typeof loadCanonicalSessionRepairEntries>[number]
  >();
  const byStore = new Map<string, CanonicalSessionCandidateFact[]>();
  for (const fact of facts) {
    const key = `${fact.agentId}\0${fact.storePath}`;
    byStore.set(key, [...(byStore.get(key) ?? []), fact]);
  }
  for (const group of byStore.values()) {
    const first = group[0]!;
    const entries = loadCanonicalSessionRepairEntries(
      { agentId: first.agentId, storePath: first.storePath },
      group.map((fact) => fact.inventoryFact),
    );
    group.forEach((fact, index) => loaded.set(fact, entries[index]!));
  }
  return facts.map((fact) => hydrateCanonicalSessionCandidate(fact, loaded.get(fact)!));
}

type SingleDatabaseCanonicalRepairGroup = {
  candidates: readonly CanonicalSessionCandidate[];
  selected: NonNullable<ReturnType<typeof selectCanonicalSessionCandidate>>;
};

function resolveSingleDatabaseCanonicalRepairGroup(
  candidates: readonly CanonicalSessionCandidate[],
  params: { cfg: OpenClawConfig; env: NodeJS.ProcessEnv },
): SingleDatabaseCanonicalRepairGroup | undefined {
  const selected = selectCanonicalSessionCandidate(candidates, params);
  if (
    !selected ||
    selected.winner.sqlitePath !== selected.destination.sqlitePath ||
    candidates.some((candidate) => candidate.sqlitePath !== selected.destination.sqlitePath)
  ) {
    return undefined;
  }
  return { candidates, selected };
}

async function repairCanonicalSessionGroupsInSingleDatabase(
  groups: readonly SingleDatabaseCanonicalRepairGroup[],
  assertCurrent: () => void,
  env: NodeJS.ProcessEnv,
): Promise<string[]> {
  const first = groups[0];
  if (!first) {
    return [];
  }
  await ensureTranscriptGenerationsForCanonicalRepair(
    groups.flatMap((group) => group.candidates).filter((candidate) => candidate.kind === "session"),
  );
  const destination = first.selected.destination;
  const result = await applySessionEntryLifecycleMutation({
    agentId: destination.agentId,
    allowCanonicalRepair: true,
    beforeCommitInTransaction: () => {
      assertCurrent();
      const database = openOpenClawAgentDatabase(resolveTargetSqliteOptions(destination, env));
      loadCanonicalRepairEntriesFromDatabase(
        database,
        groups.flatMap((group) => group.candidates.map((candidate) => candidate.inventoryFact)),
      );
      for (const group of groups) {
        if (group.selected.kind !== "session") {
          continue;
        }
        for (const candidate of group.candidates) {
          if (candidate.kind === "retained") {
            assertRetainedSnapshotsMatchCanonicalEntry(database, candidate, group.selected.entry);
          }
        }
      }
    },
    afterUpsertsInTransaction: (database) => {
      assertCurrent();
      rehomeSessionDeliveryReferencesForCanonicalRepairBatch(
        database,
        groups.map((group) => ({
          canonicalKey: group.selected.winner.canonicalKey,
          previousKeys: listCanonicalDestinationAliasKeys(group.candidates, group.selected.winner),
        })),
      );
      for (const group of groups) {
        const created = applyCanonicalDestinationArtifacts({
          copyWinnerAlias: true,
          database,
          destinationStore: group.candidates,
          rehomeDeliveries: false,
          winner: group.selected.winner,
        });
        if (group.candidates.some((candidate) => candidate.kind === "retained")) {
          finishCanonicalRetainedOwner(database, group.selected.winner.canonicalKey, created);
        }
      }
      assertCurrent();
    },
    removals: groups.flatMap((group) =>
      createCanonicalDestinationRemovals(group.candidates, group.selected),
    ),
    skipMaintenance: true,
    storePath: destination.storePath,
    upserts: groups.flatMap((group) =>
      group.selected.kind === "session"
        ? [
            {
              entry: group.selected.entry,
              sessionKey: group.selected.winner.canonicalKey,
            },
          ]
        : [],
    ),
  });
  return result.archivedTranscriptDirectories;
}

/** Doctor-owned durable repair; process-held incognito databases are intentionally excluded. */
export async function repairCanonicalSessionKeys(params: {
  apply: boolean;
  authority?: DoctorSqliteMaintenanceAuthority;
  cfg: OpenClawConfig;
  env?: NodeJS.ProcessEnv;
}): Promise<CanonicalSessionKeyRepairReport> {
  const env = params.env ?? process.env;
  const stores = listCanonicalSessionStores({
    cfg: params.cfg,
    env,
  });
  const archivedTranscriptDirectories = new Set<string>();
  let repairBatches = 0;
  let repairedGroups = 0;
  const inventory = collectCanonicalSessionRepairs({ cfg: params.cfg, env }, stores);
  let repairGroups = inventory.groups;
  const pendingPaths = new Set([
    ...repairGroups.flatMap((group) => group.candidates.map((candidate) => candidate.sqlitePath)),
    ...inventory.inventories
      .filter(({ inventory }) => inventory.pendingAdmission)
      .map(({ target }) => target.sqlitePath),
  ]);
  if (params.apply && pendingPaths.size > 0 && !params.authority) {
    return await withDoctorSqliteMaintenanceLock({
      env,
      operation: "canonical session-key repair",
      run: (authority) => repairCanonicalSessionKeys({ ...params, env, authority }),
    });
  }
  const identities = params.apply
    ? inventory.inventories.map(({ target, inventory }) => ({
        target,
        identity: readDatabasePathIdentitySync(target.sqlitePath),
        pendingAdmission: inventory.pendingAdmission,
      }))
    : [];
  const assertCurrent = () => {
    params.authority?.assertCurrent();
    for (const { target, identity } of identities) {
      assertExistingDatabaseIdentity(target.sqlitePath, identity.key, identity.birthtime);
    }
  };
  if (params.apply && pendingPaths.size > 0) {
    assertCurrent();
    const inventoriesByPath = new Map(
      inventory.inventories.map(({ target, inventory }) => [
        fs.realpathSync(target.sqlitePath),
        inventory,
      ]),
    );
    const backup = await backupDoctorSqliteDatabases({
      env,
      pendingDatabasePaths: [...pendingPaths],
      databasePaths: stores.map((target) => target.sqlitePath),
      authority: { assertCurrent },
      repair: {
        key: `canonical-session-keys-${randomUUID()}`,
        validate: (database, sourcePath) => {
          const expected = inventoriesByPath.get(sourcePath);
          if (expected) {
            loadCanonicalRepairEntriesFromDatabase(
              { db: database },
              expected.facts,
              expected.inventoryToken,
            );
          }
        },
      },
    });
    assertCurrent();
    // Keep the backed-up inventory current before admission changes derived entry validity.
    for (const store of stores) {
      const expected = inventoriesByPath.get(fs.realpathSync(store.sqlitePath));
      if (expected) {
        loadCanonicalSessionRepairEntries(
          { agentId: store.agentId, storePath: store.storePath, env },
          expected.facts,
          expected.inventoryToken,
        );
      }
    }
    note(
      [...backup.changes, ...backup.warnings].map((message) => `- ${message}`).join("\n"),
      "Session SQLite backups",
    );
  }
  if (params.apply) {
    for (const { target, identity, pendingAdmission } of identities) {
      const options = resolveTargetSqliteOptions(target, env);
      // Cached handles still need the admission owner's pending validity repair.
      const database = pendingAdmission
        ? runOpenClawAgentWriteTransaction(
            (database) => {
              assertCurrent();
              ensureSessionEntryValidityProjection(database.db);
              return database;
            },
            options,
            {
              operationLabel: "doctor.canonical-session-validity",
              repairAdmission: { assertCurrent, expectedIdentity: identity },
            },
          )
        : openOpenClawAgentDatabase(options);
      setCanonicalSqliteSessionMainKey(database, params.cfg.session?.mainKey);
    }
    // Writable admission settles pending entry validity, so plan from its committed projection.
    const admitted = collectCanonicalSessionRepairs({ cfg: params.cfg, env }, stores);
    if (
      admitted.inventories.some(({ inventory }) => inventory.pendingAdmission) ||
      (admitted.groups.length > 0 && (!params.authority || pendingPaths.size === 0))
    ) {
      throw new Error("Canonical session repair inputs changed during admission; rerun Doctor");
    }
    repairGroups = admitted.groups;
  }
  const foundGroups = repairGroups.length;
  const removedRows = repairGroups.reduce((total, group) => total + group.removedRows, 0);
  if (params.apply) {
    while (repairGroups.length > 0) {
      const candidateGroups = repairGroups.slice(0, CANONICAL_SESSION_REPAIR_BATCH_GROUP_LIMIT);
      const hydrated = hydrateCanonicalSessionCandidates(
        candidateGroups.flatMap((candidateGroup) => candidateGroup.candidates),
      );
      let hydratedOffset = 0;
      const hydratedGroups = candidateGroups.map((candidateGroup) => {
        const candidates = hydrated.slice(
          hydratedOffset,
          hydratedOffset + candidateGroup.candidates.length,
        );
        hydratedOffset += candidateGroup.candidates.length;
        return candidates;
      });
      const candidates = hydratedGroups[0]!;
      const singleDatabaseGroup = resolveSingleDatabaseCanonicalRepairGroup(candidates, {
        cfg: params.cfg,
        env,
      });
      if (!singleDatabaseGroup) {
        for (const directory of await repairCanonicalSessionGroup(candidates, {
          cfg: params.cfg,
          env,
          assertCurrent,
        })) {
          archivedTranscriptDirectories.add(directory);
        }
        repairBatches += 1;
        repairedGroups += 1;
        repairGroups = collectCanonicalSessionRepairs({ cfg: params.cfg, env }, stores).groups;
        continue;
      }
      const batch = [singleDatabaseGroup];
      // Keep commits bounded and preserve the original order around cross-store moves, while
      // collapsing the repeated whole-store projections for the common same-database path.
      for (const nextCandidates of hydratedGroups.slice(1)) {
        const nextSingleDatabaseGroup = resolveSingleDatabaseCanonicalRepairGroup(nextCandidates, {
          cfg: params.cfg,
          env,
        });
        if (
          !nextSingleDatabaseGroup ||
          nextSingleDatabaseGroup.selected.destination.sqlitePath !==
            singleDatabaseGroup.selected.destination.sqlitePath
        ) {
          break;
        }
        batch.push(nextSingleDatabaseGroup);
      }
      for (const directory of await repairCanonicalSessionGroupsInSingleDatabase(
        batch,
        assertCurrent,
        env,
      )) {
        archivedTranscriptDirectories.add(directory);
      }
      repairBatches += 1;
      repairedGroups += batch.length;
      repairGroups = collectCanonicalSessionRepairs({ cfg: params.cfg, env }, stores).groups;
    }
  }
  return {
    archivedTranscriptDirectories: [...archivedTranscriptDirectories].toSorted(),
    foundGroups,
    repairBatches,
    removedRows,
    repairedGroups,
    scannedStores: stores.length,
  };
}
