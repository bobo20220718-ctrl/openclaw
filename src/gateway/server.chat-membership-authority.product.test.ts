import { randomUUID, X509Certificate } from "node:crypto";
import fs from "node:fs/promises";
import { createServer } from "node:http";
import { expectDefined } from "@openclaw/normalization-core";
import { asOptionalRecord } from "@openclaw/normalization-core/record-coerce";
import { expect, it, vi } from "vitest";
import type { UsersSelfResult } from "../../packages/gateway-protocol/src/schema/users.js";
import { writeOpenAiResponsesText } from "../../test/helpers/openai-responses-sse.js";
import { TEST_TLS_CERT_PEM, TEST_TLS_KEY_PEM } from "../../test/helpers/tls-fixture.js";
import { loadTranscriptEvents, replaceSessionEntry } from "../config/sessions/session-accessor.js";
import { addSessionMember, removeSessionMember } from "../config/sessions/session-sharing-store.js";
import { historyLane } from "../config/sessions/session-transcript-worker-resources.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { loadOrCreateDeviceIdentity } from "../infra/device-identity.js";
import { readPersistedMediaFacts } from "../media/media-facts.js";
import { resolveInboundMediaReference } from "../media/media-reference.js";
import { createDeferredCore } from "../shared/deferred.js";
import { setUserProfileRole } from "../state/user-profile-writes.worker.js";
import { ensureProfileForEmail } from "../state/user-profiles.js";
import { createOpenClawTestState } from "../test-utils/openclaw-test-state.js";
import { acquireTestPortBlock } from "../test-utils/port-claims.js";
import { GATEWAY_CLIENT_MODES, GATEWAY_CLIENT_NAMES } from "../utils/message-channel.js";
import {
  connectGatewayClient,
  disconnectGatewayClient,
  startGatewayWithClient,
} from "./test-helpers.e2e.js";
import { buildMockOpenAiResponsesProvider } from "./test-openai-responses-model.js";

it(
  "carries current membership and explicit global ownership through transcript persistence and agent dispatch",
  { timeout: 90_000 },
  async () => {
    const state = await createOpenClawTestState({
      label: "chat-membership-authority",
      env: {
        OPENCLAW_TEST_MINIMAL_GATEWAY: undefined,
        OPENCLAW_SKIP_CHANNELS: "1",
        OPENCLAW_SKIP_GMAIL_WATCHER: "1",
        OPENCLAW_SKIP_CRON: "1",
        OPENCLAW_SKIP_CANVAS_HOST: "1",
        OPENCLAW_SKIP_BROWSER_CONTROL_SERVER: "1",
        OPENCLAW_SKIP_PROVIDERS: "1",
        OPENCLAW_DISABLE_BUNDLED_PLUGINS: "1",
        OPENCLAW_GATEWAY_TOKEN: undefined,
        OPENCLAW_GATEWAY_PASSWORD: undefined,
      },
    });
    const requests: string[] = [];
    const providerErrors: unknown[] = [];
    let providerReply = "Authorized member reply.";
    const providerServer = createServer((request, response) => {
      void (async () => {
        const chunks: Buffer[] = [];
        for await (const chunk of request) {
          chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        }
        requests.push(Buffer.concat(chunks).toString("utf8"));
        writeOpenAiResponsesText(response, {
          text: providerReply,
          messageId: `msg_${randomUUID()}`,
          responseId: `resp_${randomUUID()}`,
        });
      })().catch((error: unknown) => {
        providerErrors.push(error);
        response.writeHead(500).end("fixture provider failed");
      });
    });
    let gateway: Awaited<ReturnType<typeof startGatewayWithClient>> | undefined;
    let administrator: Awaited<ReturnType<typeof connectGatewayClient>> | undefined;
    try {
      await new Promise<void>((resolve, reject) => {
        providerServer.once("error", reject);
        providerServer.listen(0, "127.0.0.1", resolve);
      });
      const address = providerServer.address();
      if (!address || typeof address === "string") {
        throw new Error("fixture provider did not bind");
      }
      const provider = buildMockOpenAiResponsesProvider(
        `http://127.0.0.1:${address.port}/v1`,
        "membership-authority",
      );
      const proxyUser = "membership-authority-member@example.test";
      const proxyAdministrator = "membership-authority-administrator@example.test";
      setUserProfileRole(ensureProfileForEmail(proxyAdministrator).id, "administrator");
      const certPath = await state.writeText("tls/cert.pem", TEST_TLS_CERT_PEM);
      const keyPath = await state.writeText("tls/key.pem", TEST_TLS_KEY_PEM);
      const trustedProxy = {
        userHeader: "x-forwarded-user",
        requiredHeaders: ["x-forwarded-proto"],
        allowUsers: [proxyUser, proxyAdministrator],
        allowLoopback: true,
        deviceAutoApprove: {
          enabled: true,
          scopes: ["operator.read", "operator.write"],
        },
      };
      const cfg = {
        agents: {
          ownership: "explicit",
          entries: { main: {}, work: {} },
          defaults: {
            workspace: state.workspaceDir,
            skipBootstrap: true,
            heartbeat: { every: "0m" },
            model: { primary: provider.modelRef },
            models: {
              [provider.modelRef]: {
                agentRuntime: { id: "openclaw" },
                params: { transport: "sse", openaiWsWarmup: false },
              },
            },
          },
        },
        session: { scope: "global" },
        models: {
          mode: "replace",
          providers: {
            [provider.providerId]: {
              ...provider.config,
              request: { allowPrivateNetwork: true },
            },
          },
        },
        plugins: { enabled: false, slots: { memory: "none" } },
        tools: { profile: "minimal" },
        gateway: {
          auth: {
            mode: "trusted-proxy",
            trustedProxy,
            identityScopes: { [proxyAdministrator]: ["operator.admin"] },
          },
          trustedProxies: ["127.0.0.1"],
          controlUi: { allowedOrigins: ["https://control.example.com"] },
          tls: { enabled: true, autoGenerate: false, certPath, keyPath },
          roles: {
            default: "view",
            definitions: {
              administrator: {
                sessions: { others: "write" },
                agents: "*",
                scopes: ["operator.admin"],
              },
              view: {
                sessions: { others: "view" },
                agents: "*",
                scopes: ["operator.read", "operator.write"],
              },
            },
          },
        },
      } satisfies OpenClawConfig;
      const portClaim = await acquireTestPortBlock({ offsets: [0, 1, 2, 3, 4] });
      gateway = await startGatewayWithClient({
        cfg,
        configPath: state.configPath,
        portClaim,
        clientName: GATEWAY_CLIENT_NAMES.CONTROL_UI,
        mode: GATEWAY_CLIENT_MODES.WEBCHAT,
        auth: cfg.gateway.auth,
        edgeAuthHeaders: {
          "x-forwarded-for": "203.0.113.50",
          "x-forwarded-proto": "https",
          "x-forwarded-user": proxyUser,
        },
        origin: "https://control.example.com",
        secure: true,
        tlsFingerprint: new X509Certificate(TEST_TLS_CERT_PEM).fingerprint256,
        scopes: ["operator.read", "operator.write"],
      });
      await gateway.server.startupSettled;
      administrator = await connectGatewayClient({
        url: `wss://127.0.0.1:${gateway.port}`,
        clientName: GATEWAY_CLIENT_NAMES.CONTROL_UI,
        mode: GATEWAY_CLIENT_MODES.WEBCHAT,
        edgeAuthHeaders: {
          "x-forwarded-for": "203.0.113.51",
          "x-forwarded-proto": "https",
          "x-forwarded-user": proxyAdministrator,
        },
        origin: "https://control.example.com",
        tlsFingerprint: new X509Certificate(TEST_TLS_CERT_PEM).fingerprint256,
        scopes: ["operator.read", "operator.write"],
        deviceIdentity: loadOrCreateDeviceIdentity({
          path: state.statePath("test-device-identities", "administrator.sqlite"),
        }),
      });
      const self = await gateway.client.request<UsersSelfResult>("users.self", {});
      const memberProfileId = self.profile.id;
      const sessionKey = `agent:main:member-authority-${randomUUID()}`;
      const sessionId = `member-authority-${randomUUID()}`;
      const scope = { agentId: "main", sessionKey, sessionId };
      const owner = ensureProfileForEmail("membership-authority-owner@example.test");
      await replaceSessionEntry(scope, {
        sessionId,
        updatedAt: Date.now(),
        visibility: "read-only",
        createdActor: { type: "human", source: "profile", id: owner.id },
      });
      await addSessionMember(scope, { identityId: memberProfileId, addedBy: owner.id });

      const allowedMessage = "Persist and dispatch this authorized member turn.";
      const accepted = await gateway.client.request<{ runId: string; status: string }>(
        "chat.send",
        {
          sessionKey,
          message: allowedMessage,
          deliver: false,
          idempotencyKey: randomUUID(),
        },
      );
      expect(accepted.status).toBe("started");
      await expect(
        gateway.client.request<{ status: string }>(
          "agent.wait",
          { runId: accepted.runId, timeoutMs: 30_000 },
          { timeoutMs: 35_000 },
        ),
      ).resolves.toMatchObject({ status: "ok" });
      const allowedTranscript = await loadTranscriptEvents(scope);
      expect(JSON.stringify(allowedTranscript)).toContain(allowedMessage);
      expect(JSON.stringify(allowedTranscript)).toContain("Authorized member reply.");
      expect(requests).toHaveLength(1);
      expect(requests[0]).toContain(allowedMessage);
      expect(providerErrors).toEqual([]);

      const transcriptBeforeDeniedTurn = structuredClone(await loadTranscriptEvents(scope));
      for (const change of ["membership revoked", "session replaced"] as const) {
        await addSessionMember(scope, { identityId: memberProfileId, addedBy: owner.id });
        const readCaptured = createDeferredCore();
        const resumeRead = createDeferredCore();
        const read = historyLane.pool.run.bind(historyLane.pool);
        let held = false;
        const workerRead = vi
          .spyOn(historyLane.pool, "run")
          .mockImplementation(async (prepare, options) => {
            if (typeof prepare !== "function") {
              return read(prepare, options);
            }
            let matchesDeniedAuthorization = false;
            const reply = await read(async () => {
              const input = await prepare();
              matchesDeniedAuthorization =
                input.kind === "session-exact-entries" &&
                input.projection === "full" &&
                input.includeMembers === true &&
                input.includeAuthorization === true &&
                input.sessionKeys.length === 1 &&
                input.sessionKeys[0] === sessionKey;
              return input;
            }, options);
            if (!held && matchesDeniedAuthorization) {
              held = true;
              readCaptured.resolve();
              await resumeRead.promise;
            }
            return reply;
          });
        const denied = gateway.client.request("chat.send", {
          sessionKey,
          message: "This in-flight revoked member turn must have no effects.",
          deliver: false,
          idempotencyKey: randomUUID(),
        });
        try {
          await Promise.race([
            readCaptured.promise,
            denied.then(() => {
              throw new Error("chat.send completed before its authorization read was held");
            }),
          ]);
          expect(requests).toHaveLength(1);
          expect(await loadTranscriptEvents(scope)).toEqual(transcriptBeforeDeniedTurn);
          if (change === "membership revoked") {
            await removeSessionMember(scope, memberProfileId);
          } else {
            await replaceSessionEntry(scope, {
              sessionId: `${sessionId}-successor`,
              updatedAt: Date.now(),
              visibility: "read-only",
              createdActor: { type: "human", source: "profile", id: owner.id },
            });
          }
          resumeRead.resolve();
          await expect(denied).rejects.toMatchObject({ code: "INVALID_REQUEST" });
        } finally {
          resumeRead.resolve();
          await Promise.allSettled([denied]);
          workerRead.mockRestore();
        }
        expect(held).toBe(true);
        expect(await loadTranscriptEvents(scope)).toEqual(transcriptBeforeDeniedTurn);
        if (change === "session replaced") {
          expect(
            await loadTranscriptEvents({ ...scope, sessionId: `${sessionId}-successor` }),
          ).toEqual([]);
        }
        expect(requests).toHaveLength(1);
        expect(providerErrors).toEqual([]);
      }
      await removeSessionMember(scope, memberProfileId);
      await expect(
        gateway.client.request("chat.send", {
          sessionKey,
          message: "This later revoked member turn must also have no effects.",
          deliver: false,
          idempotencyKey: randomUUID(),
        }),
      ).rejects.toMatchObject({ code: "INVALID_REQUEST" });
      expect(await loadTranscriptEvents(scope)).toEqual(transcriptBeforeDeniedTurn);
      expect(requests).toHaveLength(1);
      expect(providerErrors).toEqual([]);

      // Reset through a separate administrator; member turns must retain their narrow scopes.
      // The public lifecycle owner clears prior turns and retains only the reset marker.
      let resetGlobalSessions = false;
      for (const [first, second] of [
        ["main", "work"],
        ["work", "main"],
      ] as const) {
        for (const withAttachment of [false, true]) {
          if (resetGlobalSessions) {
            for (const agentId of [first, second]) {
              await expect(
                administrator.request("sessions.reset", { key: "global", agentId }),
              ).resolves.toMatchObject({ ok: true });
              await expect(
                gateway.client.request("chat.history", { sessionKey: "global", agentId }),
              ).resolves.toMatchObject({
                sessionKey: "global",
                messages: [
                  {
                    role: "system",
                    content: [{ type: "text", text: "Reset" }],
                    __openclaw: { kind: "reset" },
                  },
                ],
              });
            }
          }
          resetGlobalSessions = true;
          const markers = {
            main: `GLOBAL_MAIN_${randomUUID()}`,
            work: `GLOBAL_WORK_${randomUUID()}`,
          };
          const sessionIds = new Map<string, string>();
          const attachmentRefs = new Map<string, string>();
          for (const method of ["chat.send", "agent"] as const) {
            for (const agentId of [first, second]) {
              const otherAgentId = agentId === "main" ? "work" : "main";
              const message = `${markers[agentId]}_${method}: preserve this owner's global chat.`;
              providerReply = `${markers[agentId]}_${method}_REPLY`;
              const attachmentText = `${markers[agentId]} attachment`;
              const attachments =
                withAttachment && method === "chat.send"
                  ? [
                      {
                        fileName: `${agentId}-notes.txt`,
                        mimeType: "text/plain",
                        content: Buffer.from(attachmentText).toString("base64"),
                      },
                    ]
                  : undefined;
              const requestOffset = requests.length;
              const started = await gateway.client.request<{ runId: string; status: string }>(
                method,
                {
                  sessionKey: "global",
                  agentId,
                  message,
                  deliver: false,
                  idempotencyKey: randomUUID(),
                  attachments,
                },
              );
              expect(started.status).toBe(method === "chat.send" ? "started" : "accepted");
              await expect(
                gateway.client.request<{ status: string }>(
                  "agent.wait",
                  { runId: started.runId, timeoutMs: 30_000 },
                  { timeoutMs: 35_000 },
                ),
              ).resolves.toMatchObject({ status: "ok" });
              const providerInput = requests.slice(requestOffset).join("\n");
              expect(providerInput).toContain(message);
              expect(providerInput).not.toContain(markers[otherAgentId]);
              const history = await gateway.client.request<{
                sessionKey: string;
                sessionId: string;
                messages: unknown[];
              }>("chat.history", { sessionKey: "global", agentId, limit: 20 });
              expect(history.sessionKey).toBe("global");
              expect(history.sessionId).toEqual(expect.any(String));
              if (method === "agent") {
                expect(history.sessionId).toBe(sessionIds.get(agentId));
                expect(providerInput).toContain(`${markers[agentId]}_chat.send_REPLY`);
              }
              sessionIds.set(agentId, history.sessionId);
              expect(new Set(sessionIds.values()).size).toBe(sessionIds.size);
              if (attachments) {
                const uploaded = history.messages
                  .flatMap((message) => {
                    const record = asOptionalRecord(message);
                    return record ? (readPersistedMediaFacts(record) ?? []) : [];
                  })
                  .filter((fact) => fact.fileName === `${agentId}-notes.txt`);
                expect(uploaded).toMatchObject([
                  { contentType: "text/plain", sizeBytes: Buffer.byteLength(attachmentText) },
                ]);
                const mediaRef = expectDefined(uploaded[0]?.url, "uploaded media reference");
                expect(mediaRef).toMatch(/^media:\/\/inbound\//);
                const resolved = expectDefined(
                  await resolveInboundMediaReference(mediaRef),
                  "managed inbound attachment",
                );
                await expect(fs.readFile(resolved.physicalPath, "utf8")).resolves.toBe(
                  attachmentText,
                );
                attachmentRefs.set(agentId, mediaRef);
              }
              const attachmentRef = attachmentRefs.get(agentId);
              const otherAttachmentRef = attachmentRefs.get(otherAgentId);
              if (attachmentRef) {
                expect(providerInput).toContain(attachmentRef);
              }
              if (otherAttachmentRef) {
                expect(providerInput).not.toContain(otherAttachmentRef);
              }
              const transcript = await loadTranscriptEvents({
                agentId,
                sessionKey: "global",
                sessionId: history.sessionId,
              });
              for (const persisted of [history.messages, transcript]) {
                const text = JSON.stringify(persisted);
                expect(text).toContain(message);
                expect(text).toContain(providerReply);
                expect(text).not.toContain(markers[otherAgentId]);
                if (attachmentRef) {
                  expect(text).toContain(attachmentRef);
                }
                if (otherAttachmentRef) {
                  expect(text).not.toContain(otherAttachmentRef);
                }
              }
              const otherHistory = await gateway.client.request<{ messages: unknown[] }>(
                "chat.history",
                { sessionKey: "global", agentId: otherAgentId, limit: 20 },
              );
              expect(JSON.stringify(otherHistory.messages)).not.toContain(markers[agentId]);
              if (attachmentRef) {
                expect(JSON.stringify(otherHistory.messages)).not.toContain(attachmentRef);
              }
              expect(providerErrors).toEqual([]);
            }
          }
        }
      }
    } finally {
      try {
        try {
          if (administrator) {
            await disconnectGatewayClient(administrator);
          }
        } finally {
          if (gateway) {
            await disconnectGatewayClient(gateway.client);
            await gateway.server.close({ reason: "membership authority proof complete" });
          }
        }
      } finally {
        providerServer.closeAllConnections();
        await new Promise<void>((resolve) => {
          providerServer.close(() => resolve());
        });
        await state.cleanup();
      }
    }
  },
);
