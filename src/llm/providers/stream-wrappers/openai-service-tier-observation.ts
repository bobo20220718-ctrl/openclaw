import { responsesServiceTierObserver } from "@openclaw/ai/internal/openai";
import type { StreamFn } from "@openclaw/llm-core";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import { createSubsystemLogger } from "../../../logging/subsystem.js";
import { supportsOpenAIResponsesFastMode } from "../openai-fast-mode.js";

const log = createSubsystemLogger("llm/providers/stream-wrappers");

/** Keep negative tier facts with the selected account, never in a global model cache. */
export function createOpenAIServiceTierObservationWrapper(
  underlying: StreamFn,
  recordDowngrade: (model: Parameters<StreamFn>[0], serviceTiers: readonly string[]) => boolean,
  readServiceTiers?: (model: Parameters<StreamFn>[0]) => readonly string[] | undefined,
): StreamFn {
  return (model, context, options) => {
    if (model.api !== "openai-responses" || !supportsOpenAIResponsesFastMode(model)) {
      return underlying(model, context, options);
    }
    const observedOptions = { ...options };
    const originalOnPayload = options?.onPayload;
    observedOptions.onPayload = async (payload, target) => {
      const replacement = await originalOnPayload?.(payload, target);
      const request = replacement ?? payload;
      const tiers = readServiceTiers?.(model);
      if (tiers && isRecord(request)) {
        if (request.service_tier === "ultrafast" && !tiers.includes("ultrafast")) {
          request.service_tier = tiers.includes("priority") ? "priority" : "default";
        } else if (request.service_tier === "priority" && !tiers.includes("priority")) {
          request.service_tier = "default";
        }
      }
      return replacement;
    };
    const previous = options && responsesServiceTierObserver.get(options);
    responsesServiceTierObserver.set(observedOptions, (observation) => {
      previous?.(observation);
      const unavailable = observation.rejected
        ? observation.requestedTier
        : observation.requestedTier === "ultrafast" && observation.responseTier !== "ultrafast"
          ? "ultrafast"
          : undefined;
      if (unavailable !== "ultrafast" && unavailable !== "priority") {
        return;
      }
      const tiers = (readServiceTiers?.(model) ?? ["priority", "ultrafast"]).filter(
        (tier) => tier !== unavailable,
      );
      if (recordDowngrade(model, tiers)) {
        log.info(`OpenAI ${unavailable} tier is unavailable for the selected account/model route.`);
      }
    });
    return underlying(model, context, observedOptions);
  };
}
