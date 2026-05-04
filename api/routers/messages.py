import json
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
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
    if session.status in ("completed", "error"):
        raise HTTPException(status_code=409, detail=f"Session is {session.status}")

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

    # Bump updated_at so the TTL reaper doesn't consider this session idle
    session.updated_at = datetime.now(UTC)

    await db.commit()
    await db.refresh(msg)
    return msg
