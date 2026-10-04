import type { IncognitoSessionAuthority } from "../../config/sessions/session-incognito-contract.js";
import { mergeSessionEntry, type SessionEntry } from "../../config/sessions/types.js";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { isIncognitoSessionKey } from "../../routing/session-key.js";
import { resolveIncognitoOpenClawAgentSqlitePath } from "../../state/openclaw-agent-db.paths.js";
import type { IncognitoAgentDatabaseExecution } from "../../state/openclaw-agent-execution-incognito.js";
import { captureOpenClawStateWorkerContext } from "../../state/openclaw-state-worker-context.js";
import { captureAcpSessionEntryBinding } from "./session-meta-entry.kernel.js";
import type { AcpSessionEntryMutation } from "./session-meta-entry.types.js";
import {
  buildAcpDatabaseSessionKey,
  legacyAcpDatabaseSessionKeys,
  resolveLegacyFreeAcpSessionKey,
} from "./session-meta-keys.js";
import { readAcpSessionMetaForEntries } from "./session-meta-readonly.js";
import {
  prepareAcpSessionMutation,
  commitAcpSessionMutation,
} from "./session-meta-worker-mutation.js";
import type { upsertAcpSessionMetaNative } from "./session-meta-write.native.js";

type Target = {
  actor: IncognitoAgentDatabaseExecution;
  authority: IncognitoSessionAuthority;
  sessionKey: string;
  cfg: OpenClawConfig;
  env: NodeJS.ProcessEnv;
  databasePath?: string;
};

function captureTarget(params: Target) {
  const { actor, authority } = params;
  const sessionKey = params.sessionKey.trim().toLowerCase();
  const context = captureOpenClawStateWorkerContext({ env: params.env, path: params.databasePath });
  const assertCurrent = () => {
    actor.assertCurrent();
    authority.assertCurrent();
    context.admission.assertCurrent();
  };
  assertCurrent();
  if (
    !isIncognitoSessionKey(sessionKey) ||
    actor.path !==
      resolveIncognitoOpenClawAgentSqlitePath({ agentId: actor.agentId, env: context.environment })
  ) {
    throw new Error("ACP incognito access differs from its retained actor");
  }
  return { actor, authority, sessionKey, context, assertCurrent };
}

/** Inactive join: volatile entry custody spans the existing shared metadata reader. */
export function readIncognitoAcpSessionEntry(params: Target): Promise<SessionEntry | undefined> {
  const { actor, authority, sessionKey, context, assertCurrent } = captureTarget(params);
  return actor.sessions.withSharedState(async () => {
    const { entry, claim } = await actor.sessions.read(authority, { sessionKey });
    const snapshot = actor.sessions.captureSnapshot(sessionKey);
    const [acp] = await readAcpSessionMetaForEntries({
      entries: [{ sessionKey, agentId: actor.agentId, entry }],
      cfg: params.cfg,
      env: context.environment,
      databasePath: context.admission.databasePath,
    });
    assertCurrent();
    snapshot.assertCurrent();
    claim.authorize(authority, "commit");
    if (!entry) {
      return undefined;
    }
    delete entry.acp;
    return acp ? { ...entry, acp } : entry;
  });
}

/** Preserve entry → shared metadata ordering without holding an actor grant across a second owner. */
export function upsertIncognitoAcpSessionMeta(
  params: Target &
    Pick<
      Parameters<typeof upsertAcpSessionMetaNative>[0],
      "mutate" | "now" | "expectedControlBinding"
    >,
): Promise<SessionEntry | null> {
  const { actor, authority, sessionKey, context, assertCurrent } = captureTarget(params);
  const expectedControlBinding =
    params.expectedControlBinding && structuredClone(params.expectedControlBinding);
  const updatedAt = params.now?.() ?? Date.now();
  const metadataRead = {
    keys: [
      buildAcpDatabaseSessionKey(sessionKey, actor.agentId),
      ...legacyAcpDatabaseSessionKeys(sessionKey, actor.agentId, params.cfg),
    ],
    legacyKey: resolveLegacyFreeAcpSessionKey(sessionKey),
  };
  return actor.sessions.withSharedState(async () => {
    const readSource = async () => {
      const { snapshot, claim } = await actor.sessions.acpSource(authority, sessionKey);
      const retained = actor.sessions.captureSnapshot(sessionKey);
      return {
        source: {
          kind: "ephemeral" as const,
          agentId: actor.agentId,
          path: actor.path,
          identity: actor.identity,
          snapshot,
        },
        assertCurrent(this: void) {
          assertCurrent();
          retained.assertCurrent();
        },
        authorize(this: void, stage: "transaction" | "commit") {
          claim.authorize(authority, stage);
        },
      };
    };
    const initial = await readSource();
    const entry = initial.source.snapshot.entry;
    const { preparation, decision } = await prepareAcpSessionMutation(
      context,
      {
        read: { ...metadataRead, entry },
        entry,
        updatedAt,
        source: initial.source,
        sessionKey,
        agentId: actor.agentId,
        expectedControlBinding,
      },
      params.mutate,
      initial.assertCurrent,
      initial.authorize,
    );
    initial.assertCurrent();
    initial.authorize("commit");
    if (decision.kind === "keep") {
      return preparation.current
        ? mergeSessionEntry(entry, { acp: preparation.current })
        : (entry ?? null);
    }
    const update = async (
      mutation: AcpSessionEntryMutation,
      expectedEntry: SessionEntry | undefined,
    ) => {
      // Row predicates are transaction-local. A pending actor projection cannot grant its own write.
      const result = await actor.sessions.sideData(authority, {
        type: "session.acp.entry",
        input: {
          agentId: actor.agentId,
          sessionKey,
          mutation,
          expectedEntry: expectedEntry ? captureAcpSessionEntryBinding(expectedEntry) : null,
          expectedControlBinding,
        },
      });
      assertCurrent();
      return result.entry;
    };
    const changed =
      decision.kind === "clear"
        ? entry
          ? await update({ kind: "clear" }, entry)
          : null
        : await update(
            { kind: "touch", updatedAt, fallbackEntry: preparation.preparedEntry },
            entry,
          );
    if (decision.kind === "set" && !changed) {
      return null;
    }
    const commitEntry = changed ?? entry;
    const cleanup = () => update({ kind: "clear-legacy" }, commitEntry);
    if (decision.kind === "set") {
      await cleanup();
    }
    const publication = await readSource();
    await commitAcpSessionMutation(
      context,
      {
        agentId: actor.agentId,
        storageSessionKey: sessionKey,
        sessionKey,
        entry: commitEntry,
        currentRowKey: preparation.currentRowKey,
        currentRowSessionId: preparation.currentRowSessionId,
        updatedAt,
        decision,
        source: publication.source,
        expectedControlBinding,
      },
      publication.assertCurrent,
      publication.authorize,
    );
    publication.assertCurrent();
    publication.authorize("commit");
    if (decision.kind === "clear") {
      await cleanup();
      publication.authorize("commit");
      return changed;
    }
    return mergeSessionEntry(changed ?? undefined, { acp: decision.meta });
  });
}
