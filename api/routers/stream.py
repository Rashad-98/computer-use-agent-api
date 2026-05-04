import asyncio

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionFactory, get_db
from models import Session, SessionEvent

router = APIRouter(prefix="/sessions/{session_id}/stream", tags=["stream"])

POLL_INTERVAL = 0.25  # seconds between DB polls while agent is running


@router.get("")
async def stream_session(
    session_id: str, request: Request, db: AsyncSession = Depends(get_db)
) -> StreamingResponse:
    session = await db.get(Session, session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found")

    last_event_id = request.headers.get("Last-Event-ID", "0")
    try:
        from_seq = int(last_event_id)
    except ValueError:
        from_seq = 0

    return StreamingResponse(
        _generate(session_id, from_seq),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def _generate(session_id: str, from_seq: int):
    """Yield stored SSE events, polling for new rows until the session finishes."""
    next_seq = from_seq + 1

    while True:
        async with AsyncSessionFactory() as db:
            result = await db.execute(
                select(SessionEvent)
                .where(
                    SessionEvent.session_id == session_id,
                    SessionEvent.seq >= next_seq,
                )
                .order_by(SessionEvent.seq)
            )
            events = list(result.scalars().all())
            session = await db.get(Session, session_id)
            is_done = session is None or session.status not in ("running", "pending")

        for event in events:
            yield f"id: {event.seq}\ndata: {event.data}\n\n"
            next_seq = event.seq + 1

        if is_done:
            # All events have been persisted; close the stream
            break

        # Agent still running — keepalive and poll again
        yield ": keepalive\n\n"
        await asyncio.sleep(POLL_INTERVAL)
