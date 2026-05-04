import json
from datetime import UTC, datetime

import httpx
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

import session_manager
from database import AsyncSessionFactory, get_db
from models import Message, Session
from schemas import MessageCreate, MessageResponse

router = APIRouter(prefix="/sessions/{session_id}/messages", tags=["messages"])


@router.get("", response_model=list[MessageResponse])
async def list_messages(session_id: str, db: AsyncSession = Depends(get_db)) -> list[Message]:
    session = await db.get(Session, session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found")
    result = await db.execute(
        select(Message).where(Message.session_id == session_id).order_by(Message.seq)
    )
    return list(result.scalars().all())


@router.post("", response_model=MessageResponse, status_code=202)
async def send_message(
    session_id: str, body: MessageCreate, db: AsyncSession = Depends(get_db)
) -> Message:
    session = await db.get(Session, session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found")
    if session.status == "error":
        raise HTTPException(status_code=409, detail="Session is in error state")
    if session.agent_api_port is None:
        raise HTTPException(status_code=409, detail="Session has no agent container")

    # Determine next sequence number
    result = await db.execute(
        select(func.coalesce(func.max(Message.seq), 0)).where(Message.session_id == session_id)
    )
    next_seq = (result.scalar() or 0) + 1

    # User messages use a plain string content block (matches Anthropic API format)
    content_blocks = [{"type": "text", "text": body.content}]

    msg = Message(
        session_id=session_id,
        role="user",
        content=json.dumps(content_blocks),
        seq=next_seq,
    )
    db.add(msg)

    # Set session title from the first user message
    if session.title is None:
        session.title = body.content[:120]

    # Bump updated_at so the TTL reaper doesn't consider this session idle.
    # If a prior task completed, reset to running for this new turn.
    if session.status == "completed":
        session.status = "running"
    session.updated_at = datetime.now(UTC)

    await db.commit()
    await db.refresh(msg)

    # Load full message history to send to agent
    history_result = await db.execute(
        select(Message).where(Message.session_id == session_id).order_by(Message.seq)
    )
    all_messages = [
        {"role": m.role, "content": json.loads(m.content)}
        for m in history_result.scalars().all()
    ]

    # Kick off the agent task (fire-and-forget — events arrive via SSE stream)
    agent_url = session_manager.agent_api_url(session.agent_api_port)
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{agent_url}/task",
                json={"messages": all_messages, "model": session.model},
                timeout=10.0,
            )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Agent container unreachable: {exc}") from exc

    # Start the background consumer that persists events and updates session status
    session_manager.start_consume_task(session_id, session.agent_api_port, AsyncSessionFactory)

    return msg
