# Upstream analysis: does the A1 reproduction match #1987?

Epic: [a2aproject/A2A#1987](https://github.com/a2aproject/A2A/issues/1987)
"[Epic] Idempotency & safe retries" (consolidates #928 and #1893).

## #1987's stated failure shape

> If a client crashes (or the connection drops) after sending the first
> message but before it receives and persists the server-generated
> `taskId`/`contextId`, it has no way to learn whether a task was started
> on its behalf — and no safe way to retry. There is no transport-level
> mechanism to deduplicate retried requests.

## Our experiment, mapped point by point

| #1987 element | Reproduction |
|---|---|
| Effect/task created on the initial send | Attempt 1: server runs the executor to completion; the external effect is committed and appended to the authoritative ledger (row with `attempt: 1`, server `task_id`). |
| Response lost | Deterministic fault drops the attempt-1 `message/send` response after effect commit, before the client receives it. |
| Client lacks server-generated identifiers | The client receives no `taskId`/`contextId` and persists nothing; the script states this explicitly. |
| Retry occurs | Exactly one application-level retry of the same logical operation. |
| Duplicate work/effect becomes possible | Attempt 2 uses the SAME `messageId` (`msg-a1`) and no taskId/contextId; the server creates a NEW task (fresh UUID) and runs the executor again; the ledger records a second effect row. 20/20 deterministic reproductions, 2 effects for 1 logical operation. |

**Verdict: yes — the experiment reproduces the exact #1987 failure shape**,
instantiated on the current Python reference path (`a2a-sdk` 1.1.5:
`BaseClient` → `ClientTransport` → `DefaultRequestHandler` → `AgentExecutor`).

## What this experiment adds beyond the issue text

- An **executable reference-SDK reproduction** (clone, venv, two commands)
  rather than a theoretical description.
- A **same-`messageId` retry** specifically: the retry carries the identical
  client-supplied `messageId` on both sends, and the tested handler still
  treats it as new work. This is stronger than "the retry is
  uncorrelated"; it shows the optional `messageId`-based duplicate
  detection the spec permits (§3.3.1: "Agents MAY utilize the messageId to
  detect duplicate messages") is **not exercised** by the tested
  reference-server path.
- **Authoritative external-effect counting**: one ledger row per actual
  mutation (effect_id, message_id, task_id, attempt, timestamp), so the
  "2 effects" claim is measured, not inferred.
- **Deterministic fault placement**: the drop happens strictly after effect
  commit and before client receipt — no random packet loss, no failure
  before execution.
- A **positive control** showing the same two sends and same fault with
  application-level effect dedup produce 1 effect — evidence that the
  mitigation exists outside the protocol, and that A2A itself did not
  dedup (2 tasks in both cases).
- **Repeated reproduction**: 20/20 baseline → 2 effects; 20/20 control →
  1 effect.

## What this experiment does not add

- No new idempotency mechanism is proposed (the epic's acceptance criteria
  are untouched; see #1893 for the `Idempotency-Key` proposal).
- No conceptual novelty is claimed for the underlying distributed-systems
  problem (lost-response ambiguity is textbook).
- No claim that the observed behavior is a protocol violation: `message/send`
  "MAY be idempotent" (§3.3.1) makes this OPTIONAL. Framing: the tested
  reference-server path does not exercise the optional `messageId`
  deduplication behavior.
