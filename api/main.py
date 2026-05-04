import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import session_manager
from database import AsyncSessionFactory, Base, engine
from routers import messages, sessions, stream


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    reaper = asyncio.create_task(session_manager.reap_idle_sessions(AsyncSessionFactory))
    try:
        yield
    finally:
        reaper.cancel()
        try:
            await reaper
        except asyncio.CancelledError:
            pass


app = FastAPI(title="Computer Use Agent API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(sessions.router)
app.include_router(messages.router)
app.include_router(stream.router)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
