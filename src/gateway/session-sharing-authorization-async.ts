import { resolveRequestedSessionAgentInput } from "./session-request-agent.js";
import { withSessionSharingTarget } from "./session-sharing-policy.js";
import { captureSessionMutationRouting } from "./session-sharing-preparation.js";
import {
  resolveChatSendAuthorizationParams,
  resolveDirectSessionTargets,
} from "./session-sharing-target-input.js";
import { resolveSessionMutationAuthorization } from "./session-sharing.js";

/** Read participation in the worker while retaining its owner through authorization. */
export async function resolveSessionMutationAuthorizationAsync(
  request: Parameters<typeof resolveSessionMutationAuthorization>[0] & {
    assertInvocationCurrent?: () => void;
  },
) {
  let params = request;
  params.assertInvocationCurrent?.();
  if (params.method === "chat.send") {
    const normalized = resolveChatSendAuthorizationParams(
      params.context.getRuntimeConfig(),
      params.requestParams,
    );
    if (!normalized.ok) {
      return { error: normalized.error };
    }
    params = { ...params, requestParams: normalized.value };
  }
  const targets = resolveDirectSessionTargets(params.method, params.requestParams);
  if (params.method !== "chat.send" || targets.length !== 1) {
    return resolveSessionMutationAuthorization(params);
  }
  const target = targets[0]!;
  const input = resolveRequestedSessionAgentInput(target.sessionKey, target.agentId);
  if (!input.ok) {
    return { error: input.error };
  }
  const cfg = params.context.getRuntimeConfig();
  const assertRoutingCurrent = captureSessionMutationRouting(cfg);
  return withSessionSharingTarget(
    { cfg, sessionKey: target.sessionKey, agentId: input.value },
    (read) => {
      const assertCurrent = () => {
        params.assertInvocationCurrent?.();
        read.assertCurrent();
        assertRoutingCurrent(params.context.getRuntimeConfig());
      };
      assertCurrent();
      return resolveSessionMutationAuthorization({
        ...params,
        preparedSharing: { ...read, assertCurrent },
      });
    },
  );
}
