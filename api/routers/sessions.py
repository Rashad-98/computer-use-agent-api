import os
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import session_manager
from database import get_db
from models import Session
from schemas import SessionCreate, SessionDetailResponse, SessionResponse

router = APIRouter(prefix="/sessions", tags=["sessions"])


@router.post("", response_model=SessionResponse, status_code=201)
async def create_session(body: SessionCreate, db: AsyncSession = Depends(get_db)) -> Session:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY not configured on server")

    session = Session(model=body.model, status="pending")
    db.add(session)
    await db.commit()
    await db.refresh(session)

    try:
        container_id, vnc_port, novnc_port, agent_api_port = await session_manager.spawn_session_container(
            session.id, api_key
        )
        await session_manager.wait_for_health(agent_api_port)

        session.container_id = container_id
        session.vnc_port = vnc_port
        session.novnc_port = novnc_port
        session.agent_api_port = agent_api_port
        session.status = "running"
        session.updated_at = datetime.now(UTC)
        await db.commit()
        await db.refresh(session)

    except Exception as exc:
        session.status = "error"
        session.updated_at = datetime.now(UTC)
        await db.commit()
        raise HTTPException(status_code=503, detail=f"Failed to start agent container: {exc}") from exc

    return session


@router.get("", response_model=list[SessionResponse])
async def list_sessions(db: AsyncSession = Depends(get_db)) -> list[Session]:
    result = await db.execute(select(Session).order_by(Session.created_at.desc()))
    return list(result.scalars().all())


@router.get("/{session_id}", response_model=SessionDetailResponse)
async def get_session(session_id: str, db: AsyncSession = Depends(get_db)) -> Session:
    session = await db.get(Session, session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found")
    return session


@router.delete("/{session_id}", status_code=204)
async def delete_session(session_id: str, db: AsyncSession = Depends(get_db)) -> None:
    session = await db.get(Session, session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found")

    session_manager.cancel_consume_task(session_id)

    if session.container_id:
        await session_manager.stop_session_container(session.container_id)

    await db.delete(session)
    await db.commit()
