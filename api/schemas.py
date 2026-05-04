from datetime import datetime

from pydantic import BaseModel


class SessionCreate(BaseModel):
    model: str = "claude-sonnet-4-5-20250929"


class SessionResponse(BaseModel):
    id: str
    title: str | None
    status: str
    model: str
    container_id: str | None
    vnc_port: int | None
    novnc_port: int | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class MessageCreate(BaseModel):
    content: str


class MessageResponse(BaseModel):
    id: int
    session_id: str
    role: str
    content: str  # JSON string — caller parses content blocks if needed
    seq: int
    created_at: datetime

    model_config = {"from_attributes": True}


class SessionDetailResponse(SessionResponse):
    messages: list[MessageResponse] = []
