import { randomUUID } from "node:crypto";
import { MessagePort } from "node:worker_threads";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import {
  mergeSessionEntry,
  type SessionAcpMeta,
  type SessionEntry,
} from "../../config/sessions/types.js";
import {
  createSqliteWorkerOperationAdmission,
  type SqliteWorkerOperationAdmission,
} from "../../infra/sqlite-worker-operation-admission.js";
import type { RetainedWorkerTransactionAdmission } from "../../infra/sqlite-worker-operation-settlement.js";
import { sessionChanges } from "../../sessions/session-row-changes.js";
import type { captureOpenClawStateWorkerContext } from "../../state/openclaw-state-worker-context.js";
import { runOpenClawStateWorkerOperation } from "../../state/openclaw-state-worker-store.js";
import type {
  AcpSessionMutationCommit,
  AcpSessionMutationDecision,
  AcpSessionMutationPreparation,
  AcpSessionMutationPrepareInput,
} from "./session-meta-write.types.js";

export async function prepareAcpSessionMutation(
  context: ReturnType<typeof captureOpenClawStateWorkerContext>,
  input: Omit<AcpSessionMutationPrepareInput, "nonce">,
  mutate: (
    current: SessionAcpMeta | undefined,
    entry: SessionEntry | undefined,
  ) => SessionAcpMeta | null | undefined,
  assertCurrent: () => void,
  authorize?: (stage: "transaction" | "commit") => void,
) {
  const nonce = randomUUID();
  let decision: AcpSessionMutationDecision | undefined;
  const preparation = await runOpenClawStateWorkerOperation(
    context,
    (scope) =>
      scope.execute({
        type: "acp.prepareMutation",
        input: { ...input, nonce },
      }),
    {
      assertCurrent,
      createAdmission() {
        let phase: "transaction" | "commit" | "settled" = "transaction";
        const admission = createSqliteWorkerOperationAdmission((request, grant) => {
          const facts = request.facts;
          const port =
            isRecord(facts) && facts.preparationPort instanceof MessagePort
              ? facts.preparationPort
              : undefined;
          try {
            assertCurrent();
            if (!isRecord(facts) || facts.nonce !== nonce || request.stage !== phase) {
              throw new Error("ACP callback differs from its retained transaction");
            }
            authorize?.(request.stage === "transaction" ? "transaction" : "commit");
            if (request.stage === "transaction") {
              if (!port || decision) {
                throw new Error("ACP callback has no unique decision port");
              }
              // SAFETY: this private worker supplies this operation's authoritative row snapshot.
              const prepared = facts.preparation as AcpSessionMutationPreparation;
              const next = mutate(
                prepared.current,
                prepared.current
                  ? mergeSessionEntry(prepared.preparedEntry, { acp: prepared.current })
                  : prepared.entry,
              );
              decision =
                next === undefined
                  ? { kind: "keep" }
                  : next === null
                    ? { kind: "clear" }
                    : { kind: "set", meta: next };
              assertCurrent();
              port.postMessage(decision, []);
              phase = "commit";
            } else {
              phase = "settled";
            }
            if (!grant()) {
              throw new Error("ACP callback admission expired");
            }
          } finally {
            port?.close();
          }
        });
        return {
          nativeLocations: [
            context.admission.databasePath,
            ...("kind" in input.source ? [] : [input.source.path]),
          ],
          admission,
        };
      },
    },
  );

  assertCurrent();
  if (!decision) {
    throw new Error("ACP metadata mutation returned no decision");
  }
  return { preparation, decision };
}

export async function commitAcpSessionMutation(
  context: ReturnType<typeof captureOpenClawStateWorkerContext>,
  input: AcpSessionMutationCommit,
  assertCurrent: () => void,
  authorize?: (stage: "transaction" | "commit") => void,
) {
  const nonce = randomUUID();
  let admitted:
    | { admission: SqliteWorkerOperationAdmission; retained: RetainedWorkerTransactionAdmission }
    | undefined;
  let published = false;
  const publish = () => {
    const receipt = admitted?.admission.committed?.facts;
    if (!published && isRecord(receipt) && receipt.nonce === nonce) {
      published = true;
      sessionChanges.emit({ agentId: input.agentId, sessionKey: input.sessionKey });
    }
  };
  try {
    await runOpenClawStateWorkerOperation(
      context,
      async (scope) => {
        try {
          await scope.execute({ type: "acp.commitMutation", input: { ...input, nonce } });
        } finally {
          await admitted?.retained.settled;
          publish();
        }
      },
      {
        assertCurrent,
        createAdmission(retained) {
          let phase: "transaction" | "commit" | "settled" = "transaction";
          const admission = createSqliteWorkerOperationAdmission((request, grant) => {
            assertCurrent();
            if (
              !isRecord(request.facts) ||
              request.facts.nonce !== nonce ||
              request.stage !== phase
            ) {
              throw new Error("ACP metadata commit differs from its retained owner");
            }
            authorize?.(request.stage === "transaction" ? "transaction" : "commit");
            phase = request.stage === "transaction" ? "commit" : "settled";
            if (!grant()) {
              throw new Error("ACP metadata commit admission expired");
            }
          });
          admitted = { admission, retained };
          return {
            nativeLocations: [
              context.admission.databasePath,
              ...("kind" in input.source ? [] : [input.source.path]),
            ],
            admission,
          };
        },
      },
    );
  } finally {
    await admitted?.retained.settled;
    publish();
  }
}
