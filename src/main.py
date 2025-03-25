from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
import asyncio
import json
from src.storage import CassandraStorage


app = FastAPI(
    title="Time-Series Event Service",
    description="A RESTful microservice for streaming and retrieving time-series events.",
    version="1.0.0"
)

storage = CassandraStorage()


@app.get(
    "/stream",
    response_class=StreamingResponse,
    summary="Stream real-time events",
    description="Provides a continuous stream of time-series events using Server-Sent Events (SSE).",
    responses={
        200: {
            "content": {"text/event-stream": {"example": "data: {\"timestamp\": \"2025-03-22T12:00:00Z\", \"metric\": \"temperature\", \"value\": 25.6}\n\n"}},
            "description": "Stream of events"
        }
    }
)
async def stream_events():
    return


@app.get(
    "/history",
    summary="Retrieve event history",
    description="Returns the last N events from the event history.",
    response_model=list[dict]
)
async def get_event_history(limit: int = 10):
    return


