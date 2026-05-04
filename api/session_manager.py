"""
Docker-based session lifecycle manager.

Responsibilities:
  - Allocate free OS ports atomically
  - Spawn / stop one agent container per session
  - Health-check newly spawned containers before accepting work
  - Consume + persist SSE stream from agent to session_events table
  - TTL reaper: stop containers idle > IDLE_TTL_SECONDS (default 1 hr)
"""

import asyncio
import json
import logging
import os
import socket
from datetime import UTC, datetime, timedelta

import docker
import docker.errors
import httpx
from sqlalchemy import func, select

logger = logging.getLogger(__name__)

# Registry of active background consume tasks: session_id → asyncio.Task
_consume_tasks: dict[str, asyncio.Task] = {}

AGENT_IMAGE = os.environ.get("AGENT_IMAGE", "computer-use-agent:local")
# Host/IP the API uses to reach agent containers' mapped ports.
# - "localhost"               → API running on the host (dev mode)
# - "host.docker.internal"   → API running in Docker on Linux
#   (requires --add-host=host.docker.internal:host-gateway on the API container)
AGENT_HOST = os.environ.get("AGENT_HOST", "localhost")
IDLE_TTL_SECONDS = int(os.environ.get("IDLE_TTL_SECONDS", "3600"))
HEALTH_TIMEOUT_SECONDS = float(os.environ.get("HEALTH_TIMEOUT_SECONDS", "60"))

_docker_client: docker.DockerClient | None = None


def _get_docker() -> docker.DockerClient:
    global _docker_client
    if _docker_client is None:
        _docker_client = docker.from_env()
    return _docker_client


def _allocate_port() -> int:
    """Atomically grab a free OS port. The OS won't reuse it until we release the socket."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# Sync helpers — run via asyncio.to_thread so they don't block the event loop
# ---------------------------------------------------------------------------

def _do_spawn(session_id: str, api_key: str) -> tuple[str, int, int, int]:
    vnc_port = _allocate_port()
    novnc_port = _allocate_port()
    agent_api_port = _allocate_port()

    client = _get_docker()
    container = client.containers.run(
        image=AGENT_IMAGE,
        name=f"cu-agent-{session_id[:8]}",
        detach=True,
        environment={"ANTHROPIC_API_KEY": api_key},
        ports={
            "5900/tcp": vnc_port,
            "6080/tcp": novnc_port,
            "8001/tcp": agent_api_port,
        },
    )
    return container.id, vnc_port, novnc_port, agent_api_port


def _do_stop(container_id: str) -> None:
    try:
        container = _get_docker().containers.get(container_id)
        container.stop(timeout=5)
        container.remove(force=True)
    except docker.errors.NotFound:
        pass
    except Exception:
        logger.exception("Error stopping container %s", container_id)


# ---------------------------------------------------------------------------
# Async public API
# ---------------------------------------------------------------------------

async def spawn_session_container(session_id: str, api_key: str) -> tuple[str, int, int, int]:
    """Return (container_id, vnc_port, novnc_port, agent_api_port)."""
    return await asyncio.to_thread(_do_spawn, session_id, api_key)


async def stop_session_container(container_id: str) -> None:
    await asyncio.to_thread(_do_stop, container_id)


async def wait_for_health(agent_api_port: int, timeout: float = HEALTH_TIMEOUT_SECONDS) -> None:
    """Poll the agent container's /health endpoint until it responds 200."""
    url = f"http://{AGENT_HOST}:{agent_api_port}/health"
    deadline = asyncio.get_running_loop().time() + timeout
    async with httpx.AsyncClient() as client:
        while asyncio.get_running_loop().time() < deadline:
            try:
                r = await client.get(url, timeout=2.0)
                if r.status_code == 200:
                    return
            except Exception:
                pass
            await asyncio.sleep(1.0)
    raise TimeoutError(f"Agent container not healthy after {timeout:.0f}s")


def agent_api_url(agent_api_port: int) -> str:
    return f"http://{AGENT_HOST}:{agent_api_port}"


# ---------------------------------------------------------------------------
# SSE consumer — eagerly reads agent stream, persists to session_events table
# ---------------------------------------------------------------------------

async def consume_and_persist(session_id: str, agent_api_port: int, db_factory) -> None:
    """Read the agent's SSE stream, persist every event to session_events, then update session status."""
    from models import Message, Session, SessionEvent  # local import avoids circular dependency

    url = f"http://{AGENT_HOST}:{agent_api_port}/stream"
    seq = 0
    text_blocks: list[str] = []
    final_status = "completed"

    try:
        async with httpx.AsyncClient() as client:
            async with client.stream("GET", url, timeout=None) as response:
                async for line in response.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    raw = line[len("data:"):].strip()
                    if not raw:
                        continue

                    seq += 1
                    async with db_factory() as db:
                        db.add(SessionEvent(session_id=session_id, seq=seq, data=raw))
                        await db.commit()

                    try:
                        event = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    event_type = event.get("type")
                    if event_type == "text":
                        text_blocks.append(event.get("content", ""))
                    elif event_type == "error":
                        final_status = "error"
                        break
                    elif event_type == "done":
                        break

        # Aggregate assistant text into the messages table for history
        if text_blocks:
            combined_text = "\n".join(t for t in text_blocks if t)
            content_blocks = [{"type": "text", "text": combined_text}]
            async with db_factory() as db:
                result = await db.execute(
                    select(func.coalesce(func.max(Message.seq), 0)).where(
                        Message.session_id == session_id
                    )
                )
                next_seq = (result.scalar() or 0) + 1
                db.add(
                    Message(
                        session_id=session_id,
                        role="assistant",
                        content=json.dumps(content_blocks),
                        seq=next_seq,
                    )
                )
                await db.commit()

    except asyncio.CancelledError:
        final_status = "error"
        raise
    except Exception:
        logger.exception("consume_and_persist error for session %s", session_id)
        final_status = "error"
    finally:
        _consume_tasks.pop(session_id, None)
        try:
            async with db_factory() as db:
                session = await db.get(Session, session_id)
                if session is not None:
                    session.status = final_status
                    session.updated_at = datetime.now(UTC)
                    await db.commit()
        except Exception:
            logger.exception("Failed to update session status for %s", session_id)


def start_consume_task(session_id: str, agent_api_port: int, db_factory) -> None:
    task = asyncio.create_task(consume_and_persist(session_id, agent_api_port, db_factory))
    _consume_tasks[session_id] = task


def cancel_consume_task(session_id: str) -> None:
    task = _consume_tasks.pop(session_id, None)
    if task is not None and not task.done():
        task.cancel()


# ---------------------------------------------------------------------------
# TTL reaper — started as a background task in main.py lifespan
# ---------------------------------------------------------------------------

async def reap_idle_sessions(db_factory) -> None:
    """Runs forever; every 60 s it stops containers idle > IDLE_TTL_SECONDS."""
    from models import Session  # local import avoids circular dependency at module load

    while True:
        await asyncio.sleep(60)
        try:
            cutoff = datetime.now(UTC) - timedelta(seconds=IDLE_TTL_SECONDS)
            async with db_factory() as db:
                result = await db.execute(
                    select(Session).where(
                        Session.status == "running",
                        Session.updated_at < cutoff,
                    )
                )
                idle = list(result.scalars().all())

            for session in idle:
                logger.info("Reaping idle session %s (last active %s)", session.id, session.updated_at)
                if session.container_id:
                    await stop_session_container(session.container_id)
                async with db_factory() as db:
                    s = await db.get(Session, session.id)
                    if s is not None:
                        s.status = "completed"
                        s.updated_at = datetime.now(UTC)
                        await db.commit()

        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("TTL reaper error")
