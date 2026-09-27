# A1 minimal reproduction — A2A `message/send` lost-response retry

Companion reproduction for
[a2aproject/A2A#1987](https://github.com/a2aproject/A2A/issues/1987)
("[Epic] Idempotency & safe retries"): on the tested A2A Python SDK path, a
`message/send` commits its external effect and then loses its response; the
client retries the same logical operation with the **same `messageId`**, the
server creates a **new task**, and the external effect occurs a second time —
**2 external effects for 1 logical operation**.

Built only on the real `a2a-sdk` 1.1.5 Python SDK (`BaseClient` →
in-process `ClientTransport` → `DefaultRequestHandler.on_message_send` →
`AgentExecutor.execute` → ledger). No other project code is imported.

## Setup

Python 3.12, then:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python a1_same_message_id_repro.py
python a1_positive_control.py
```

Each run takes about a second. No sleeps are used for synchronization; the
fault is deterministic: the attempt-1 response is dropped after the effect
commits, before the client receives it.

## Expected output (baseline)

```
environment: a2a-sdk 1.1.5, python 3.12.3
attempt 1: effect committed (effect_id=..., task_id=84ef3228-97ea-4413-916e-170c4e9002ec)
[fault] dropped attempt-1 message/send response AFTER effect commit, BEFORE the client received it
attempt 1: response lost -> client received NO taskId/contextId; client persists nothing about the server-side task
attempt 2: effect committed (effect_id=..., task_id=21bc4c8f-35ca-4558-bb5a-7be2c4e97e0c)
attempt 2: same message_id=msg-a1 -> server task_id=21bc4c8f-35ca-4558-bb5a-7be2c4e97e0c, context_id=..., state=TASK_STATE_COMPLETED
attempt-1 server task_id: 84ef3228-97ea-4413-916e-170c4e9002ec
attempt-2 server task_id: 21bc4c8f-35ca-4558-bb5a-7be2c4e97e0c
message_id on both sends: msg-a1
server tasks created: 2 (distinct: True)
authoritative effect count: 2
BASELINE: 2 external effects for 1 logical operation (duplicate execution observed)
```

## Expected output (positive control)

```
attempt 1: effect committed (effect_id=..., task_id=dc52441e-cb78-4dd9-8b97-8bc47d7ac3b6)
[fault] dropped attempt-1 message/send response AFTER effect commit, BEFORE the client received it
attempt 2: effect deduped (effect_id=..., task_id=dc52441e-cb78-4dd9-8b97-8bc47d7ac3b6)
attempt 2: same message_id=msg-a1 -> server task_id=ceea1a76-3c31-4cee-9d5a-6e432e695745, context_id=..., state=TASK_STATE_COMPLETED
server tasks created: 2 (A2A created a new task on retry: True)
authoritative effect count: 1
POSITIVE CONTROL: 1 external effect for 1 logical operation (retry absorbed by application-level dedup; A2A did not dedup)
```

The positive control demonstrates one application-level mitigation. It does
not show that A2A itself deduplicated the retry.

## Mechanism (5 lines)

1. The in-process transport replaces only the network: it calls the real
   `DefaultRequestHandler.on_message_send` for both attempts, then
   deterministically raises after the handler returns on attempt 1, which
   can only happen after the executor completed, so the external effect is
   committed before the response is lost.
2. The client perceives a transport failure and persists no taskId/contextId
   — exactly the ambiguity tracked by #1987.
3. The reproduction performs one explicit retry with the SAME
   `messageId` (`msg-a1`) and no taskId/contextId; the server generates a
   fresh UUID task ID per send, so it cannot correlate the retry with
   attempt 1 and runs the executor again.
4. An append-only JSONL ledger records one row per external-effect
   execution and is the authoritative effect count; the baseline records 2
   rows for 1 logical operation.
5. The positive control uses the identical protocol path, fault, and retry;
   the only change is effect-layer dedup on a stable logical-operation key,
   recording 1 row.

## Claim boundary

- The tested A2A reference-server path permits a retrying client to execute
  a mutating operation more than once after an ambiguous lost-response
  outcome — even with an identical `messageId` on the retry.
- The A2A specification (§3.3.1 Idempotency) says `message/send` "MAY be
  idempotent" and "Agents MAY utilize the messageId to detect duplicate
  messages": deduplication is **optional**. The tested reference-server path
  does not exercise the optional `messageId` deduplication behavior; the
  observed duplicate execution is **permitted** by the tested path, not a
  protocol violation.
- Source audit of the installed `a2a-sdk` 1.1.5 (no `idempot` string anywhere
  in the package; `default_request_handler.py` never reads `message_id` for
  duplicate detection; task IDs are generated as fresh UUIDs per send):
  there is no `messageId` dedup, idempotency cache, or retry handling on
  the tested path.
- Applications can mitigate using stable idempotency at the effect
  boundary. In the positive control, A2A did not dedup; the application
  effect layer did — one mitigation, not the only possible design.
