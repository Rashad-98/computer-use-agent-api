"""
Thin FastAPI shim that wraps sampling_loop and exposes:
  POST /task   — start the agent on a new task (full message history in body)
  GET  /stream — SSE stream of agent events
  GET  /health — liveness + running state
"""

import asyncio
import json
import os
import sys
from typing import Any

from anthropic.types.beta import BetaContentBlockParam
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

# upstream code lives at /home/computeruse/computer_use_demo/
sys.path.insert(0, "/home/computeruse")

from computer_use_demo.loop import APIProvider, sampling_loop
from computer_use_demo.tools import ToolVersion

app = FastAPI()

_SENTINEL = object()  # signals generator to stop


class AgentState:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[Any] = asyncio.Queue()
        self._seq = 0
        self.task: asyncio.Task | None = None

    def reset(self) -> None:
        self.queue = asyncio.Queue()
        self._seq = 0

    def next_seq(self) -> int:
        self._seq += 1
        return self._seq


state = AgentState()


class TaskRequest(BaseModel):
    messages: list[dict]
    model: str = "claude-sonnet-4-5-20250929"
    tool_version: ToolVersion = "computer_use_20250124"
    system_prompt_suffix: str = ""
    only_n_most_recent_images: int | None = 10
    max_tokens: int = 16384
    thinking_budget: int | None = None


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "task_running": state.task is not None and not state.task.done()}


@app.post("/task", status_code=202)
async def start_task(body: TaskRequest) -> dict:
    if state.task is not None and not state.task.done():
        raise HTTPException(status_code=409, detail="A task is already running")

    state.reset()
    state.task = asyncio.create_task(_run_agent_loop(body))
    return {"status": "started"}


@app.get("/stream")
async def stream() -> StreamingResponse:
    return StreamingResponse(_generate(), media_type="text/event-stream")


async def _generate():
    """Yield SSE-formatted events from the queue until the sentinel is received."""
    while True:
        try:
            event = await asyncio.wait_for(state.queue.get(), timeout=30)
        except asyncio.TimeoutError:
            # keepalive comment — prevents proxies from closing the connection
            yield ": keepalive\n\n"
            continue

        if event is _SENTINEL:
            break

        seq = state.next_seq()
        yield f"id: {seq}\ndata: {json.dumps(event)}\n\n"


async def _run_agent_loop(body: TaskRequest) -> None:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        await state.queue.put({"type": "error", "content": "ANTHROPIC_API_KEY not set"})
        await state.queue.put(_SENTINEL)
        return

    def output_callback(block: BetaContentBlockParam) -> None:
        asyncio.get_event_loop().call_soon_threadsafe(
            state.queue.put_nowait, _block_to_event(block)
        )

    def tool_output_callback(result: Any, tool_id: str) -> None:
        event: dict[str, Any] = {"type": "tool_result", "tool_use_id": tool_id}
        if hasattr(result, "output") and result.output:
            event["content"] = result.output
        if hasattr(result, "base64_image") and result.base64_image:
            event["is_image"] = True
            event["base64"] = result.base64_image
        if hasattr(result, "error") and result.error:
            event["error"] = result.error
        asyncio.get_event_loop().call_soon_threadsafe(state.queue.put_nowait, event)

    def api_response_callback(request: Any, response: Any, exc: Exception | None) -> None:
        if exc is not None:
            asyncio.get_event_loop().call_soon_threadsafe(
                state.queue.put_nowait,
                {"type": "error", "content": str(exc)},
            )

    try:
        messages = body.messages
        await sampling_loop(
            model=body.model,
            provider=APIProvider.ANTHROPIC,
            system_prompt_suffix=body.system_prompt_suffix,
            messages=messages,
            output_callback=output_callback,
            tool_output_callback=tool_output_callback,
            api_response_callback=api_response_callback,
            api_key=api_key,
            only_n_most_recent_images=body.only_n_most_recent_images,
            max_tokens=body.max_tokens,
            tool_version=body.tool_version,
            thinking_budget=body.thinking_budget,
        )
        await state.queue.put({"type": "done", "content": "Task complete."})
    except Exception as exc:  # noqa: BLE001
        await state.queue.put({"type": "error", "content": str(exc)})
    finally:
        await state.queue.put(_SENTINEL)


def _block_to_event(block: Any) -> dict[str, Any]:
    block_type = getattr(block, "type", None) or block.get("type", "unknown")

    if block_type == "text":
        text = getattr(block, "text", None) or block.get("text", "")
        return {"type": "text", "content": text}

    if block_type == "tool_use":
        return {
            "type": "tool_use",
            "name": getattr(block, "name", None) or block.get("name", ""),
            "id": getattr(block, "id", None) or block.get("id", ""),
            "input": getattr(block, "input", None) or block.get("input", {}),
        }

    if block_type == "thinking":
        return {
            "type": "thinking",
            "content": getattr(block, "thinking", None) or block.get("thinking", ""),
        }

    # fallback — pass through as raw so the client can decide what to do
    raw = block if isinstance(block, dict) else block.model_dump() if hasattr(block, "model_dump") else str(block)
    return {"type": "unknown", "raw": raw}
