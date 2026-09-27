#!/usr/bin/env python3
"""A1 baseline: lost-response retry with the SAME messageId re-executes.

Real A2A SDK path only:
    BaseClient.send_message (a2a.client.base_client)
      -> ClientTransport (in-process; replaces only the network)
          -> DefaultRequestHandler.on_message_send (a2a.server.request_handlers)
              -> AgentExecutor.execute -> external effect commit -> ledger
          <- deterministic fault: response dropped AFTER effect commit,
             BEFORE the client receives it

Sequence:
  attempt 1: message/send with messageId "msg-a1" -> effect committed ->
             response lost -> client perceives a transport failure
  attempt 2: client retries the same logical operation with the SAME
             messageId "msg-a1" (it never received taskId/contextId) ->
             server creates a NEW task -> effect committed again

Expected: 2 server tasks, 2 external effects, same messageId on both sends.

The effect ledger (a1_baseline_effects.jsonl, truncated at run start) is the
authoritative count: one row is appended only when the simulated external
mutation actually executes. Each row records effect_id, message_id, task_id,
attempt, and timestamp.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone

from importlib.metadata import version as pkg_version

# Keep output deterministic and readable: the SDK logs benign shutdown
# warnings ("Dispatcher task was cancelled...") during handler.aclose().
logging.getLogger("a2a").setLevel(logging.ERROR)

from a2a.client.base_client import BaseClient
from a2a.client.client import ClientCallContext, ClientConfig
from a2a.client.transports.base import ClientTransport
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.context import ServerCallContext
from a2a.server.events.event_queue_v2 import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from a2a.server.tasks.task_updater import TaskUpdater
from a2a.types import a2a_pb2

MESSAGE_ID = "msg-a1"
LEDGER_PATH = os.path.join(os.getcwd(), "a1_baseline_effects.jsonl")


class ResponseLostError(RuntimeError):
    """The server fully processed message/send; the response never reached
    the client. The caller cannot tell whether the external effect
    committed. Models a lost response, not a rejected request."""


class EffectLedger:
    """Append-only JSONL ledger. A row exists iff the external mutation ran."""

    def __init__(self, path: str) -> None:
        self._path = path
        # Truncate at run start: this file is this run's authoritative count.
        open(self._path, "w").close()

    def commit(self, *, message_id: str, task_id: str, attempt: int) -> dict:
        effect = {
            "effect_id": uuid.uuid4().hex,
            "message_id": message_id,
            "task_id": task_id,
            "attempt": attempt,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        with open(self._path, "a") as f:
            f.write(json.dumps(effect) + "\n")
        return effect


class CreateOrderExecutor(AgentExecutor):
    """Real executor: commits one external effect, then completes the task."""

    def __init__(self, ledger: EffectLedger, attempts: dict) -> None:
        self._ledger = ledger
        self._attempts = attempts

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        attempt = self._attempts["n"]
        # SDK requires a Task event before status updates in task mode.
        await event_queue.enqueue_event(
            a2a_pb2.Task(
                id=context.task_id,
                context_id=context.context_id,
                status=a2a_pb2.TaskStatus(
                    state=a2a_pb2.TaskState.TASK_STATE_SUBMITTED
                ),
            )
        )
        # EXTERNAL EFFECT: the simulated mutation runs here, before any reply.
        effect = self._ledger.commit(
            message_id=context.message.message_id,
            task_id=context.task_id,
            attempt=attempt,
        )
        print(
            f"attempt {attempt}: effect committed "
            f"(effect_id={effect['effect_id']}, task_id={effect['task_id']})",
            flush=True,
        )
        updater = TaskUpdater(event_queue, context.task_id, context.context_id)
        await updater.start_work()
        reply = a2a_pb2.Message(
            message_id=f"reply-attempt-{attempt}", role=a2a_pb2.ROLE_AGENT
        )
        reply.parts.add().text = f"order recorded: {effect['effect_id']}"
        await updater.complete(message=reply)

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        updater = TaskUpdater(event_queue, context.task_id, context.context_id)
        await updater.cancel()


class FaultInjectingTransport(ClientTransport):
    """In-process transport: calls the real SDK request handler directly.

    The fault hook runs between `on_message_send` returning and the
    response being built. At that point the executor has run to completion,
    so the drop discards a reply for an effect that already committed.
    """

    def __init__(
        self,
        handler: DefaultRequestHandler,
        attempts: dict,
        drop_attempt1_response: bool,
    ) -> None:
        self._handler = handler
        self._attempts = attempts
        self._drop_attempt1_response = drop_attempt1_response

    async def send_message(
        self,
        request: a2a_pb2.SendMessageRequest,
        *,
        context: ClientCallContext | None = None,
    ) -> a2a_pb2.SendMessageResponse:
        self._attempts["n"] += 1
        attempt = self._attempts["n"]
        # Full server-side processing: validation, task setup, executor run
        # (effect commits inside execute()), event aggregation.
        result = await self._handler.on_message_send(request, ServerCallContext())

        if self._drop_attempt1_response and attempt == 1:
            print(
                "[fault] dropped attempt-1 message/send response AFTER "
                "effect commit, BEFORE the client received it",
                flush=True,
            )
            raise ResponseLostError(
                "attempt-1 response lost after effect commit; "
                "client received no taskId/contextId"
            )

        response = a2a_pb2.SendMessageResponse()
        if isinstance(result, a2a_pb2.Task):
            response.task.CopyFrom(result)
        else:
            response.message.CopyFrom(result)
        return response

    async def send_message_streaming(
        self,
        request: a2a_pb2.SendMessageRequest,
        *,
        context: ClientCallContext | None = None,
    ):
        raise NotImplementedError("streaming is not used by this reproduction")
        yield  # pragma: no cover

    async def get_task(self, request, *, context=None):
        raise NotImplementedError

    async def list_tasks(self, request, *, context=None):
        raise NotImplementedError

    async def cancel_task(self, request, *, context=None):
        raise NotImplementedError

    async def create_task_push_notification_config(self, request, *, context=None):
        raise NotImplementedError

    async def get_task_push_notification_config(self, request, *, context=None):
        raise NotImplementedError

    async def list_task_push_notification_configs(self, request, *, context=None):
        raise NotImplementedError

    async def delete_task_push_notification_config(self, request, *, context=None):
        raise NotImplementedError

    async def subscribe(self, request, *, context=None):
        raise NotImplementedError
        yield  # pragma: no cover

    async def get_extended_agent_card(self, request, *, context=None):
        raise NotImplementedError

    async def close(self) -> None:
        pass


def build_client_server() -> tuple[BaseClient, DefaultRequestHandler, FaultInjectingTransport, EffectLedger]:
    ledger = EffectLedger(LEDGER_PATH)
    attempts = {"n": 0}
    card = a2a_pb2.AgentCard(
        name="a1-repro-agent",
        version="1.0.0",
        capabilities=a2a_pb2.AgentCapabilities(streaming=False),
    )
    handler = DefaultRequestHandler(
        agent_executor=CreateOrderExecutor(ledger, attempts),
        task_store=InMemoryTaskStore(),
        agent_card=card,
    )
    transport = FaultInjectingTransport(
        handler=handler, attempts=attempts, drop_attempt1_response=True
    )
    client = BaseClient(
        card=card,
        config=ClientConfig(streaming=False),
        transport=transport,
        interceptors=[],
    )
    return client, handler, transport, ledger


async def send_once(client: BaseClient, message_id: str) -> dict:
    """One real client.send_message; returns observed task/message identifiers."""
    message = a2a_pb2.Message(message_id=message_id, role=a2a_pb2.ROLE_USER)
    message.parts.add().text = "order 1 x widget"
    request = a2a_pb2.SendMessageRequest(message=message)
    stream_response = None
    async for response in client.send_message(request):
        stream_response = response
    if stream_response is None:  # pragma: no cover - defensive
        raise RuntimeError("client.send_message yielded no response")
    observed = {"message_id": message_id}
    if stream_response.HasField("task"):
        task = stream_response.task
        observed.update(
            {
                "task_id": task.id,
                "context_id": task.context_id,
                "state": a2a_pb2.TaskState.Name(task.status.state),
            }
        )
    else:  # pragma: no cover - defensive
        observed.update(
            {"task_id": None, "context_id": None, "state": "message_only"}
        )
    return observed


async def main() -> int:
    print(f"environment: a2a-sdk {pkg_version('a2a-sdk')}, "
          f"python {sys.version.split()[0]}", flush=True)
    client, handler, _transport, ledger = build_client_server()

    # --- attempt 1: message/send with messageId "msg-a1" ---
    try:
        await send_once(client, MESSAGE_ID)
        print("attempt 1: response received (unexpected)", flush=True)
    except ResponseLostError:
        print(
            "attempt 1: response lost -> client received NO taskId/contextId; "
            "client persists nothing about the server-side task",
            flush=True,
        )

    # --- attempt 2: same logical operation, SAME messageId "msg-a1" ---
    # The client cannot supply the (lost) server-generated taskId/contextId,
    # so the server cannot correlate this retry with attempt 1.
    observed_attempt2 = await send_once(client, MESSAGE_ID)
    print(
        f"attempt 2: same message_id={MESSAGE_ID} -> "
        f"server task_id={observed_attempt2['task_id']}, "
        f"context_id={observed_attempt2['context_id']}, "
        f"state={observed_attempt2['state']}",
        flush=True,
    )

    await handler.aclose()

    # Server task IDs come from the ledger: the client never saw attempt-1's.
    with open(LEDGER_PATH) as f:
        rows = [json.loads(line) for line in f if line.strip()]
    attempt1_task = rows[0]["task_id"] if rows else "<none>"
    attempt2_task = rows[-1]["task_id"] if len(rows) > 1 else observed_attempt2["task_id"]
    effect_count = len(rows)

    print(f"attempt-1 server task_id: {attempt1_task}")
    print(f"attempt-2 server task_id: {attempt2_task}")
    print(f"message_id on both sends: {MESSAGE_ID}")
    print(
        f"server tasks created: {len(rows)} "
        f"(distinct: {attempt1_task != attempt2_task})"
    )
    print(f"authoritative effect count: {effect_count}")
    print(f"ledger: {LEDGER_PATH}")

    if effect_count == 2:
        print(
            "BASELINE: 2 external effects for 1 logical operation "
            "(duplicate execution observed)"
        )
        return 0
    print(f"BASELINE UNEXPECTED: effect_count={effect_count} (expected 2)")
    return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
