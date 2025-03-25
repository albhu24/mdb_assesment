from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from collections import deque
import asyncio
import json
from src.storage import CassandraStorage


app = FastAPI(
    title="Time-Series Event Service",
    description="A RESTful microservice for streaming and retrieving time-series events.",
    version="1.0.0"
)

try:
    storage = CassandraStorage()
except Exception as e:
    raise RuntimeError(f"Failed to initialize Cassandra storage: {str(e)}")

async def event_stream():
    event_buffer = deque()
    last_timestamp = None  

    while True:
        try:
            if not event_buffer:
                new_events = storage.get_latest_events(last_timestamp=last_timestamp, limit=10)
                if new_events:
                    event_buffer.extend(new_events)
                    last_timestamp = new_events[-1]["timestamp"]
            
            if event_buffer:
                event = event_buffer.popleft()
                yield f"data: {json.dumps(event)}\n\n"
            else:
                yield "data: {}\n\n"
        except Exception as e:
            yield f"data: {{'error': 'Stream error: {str(e)}'}}\n\n"
        
        await asyncio.sleep(1)

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
    """Stream time-series events from Cassandra."""
    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get(
    "/history",
    summary="Retrieve event history",
    description="Returns the last N events from the event history.",
    response_model=list[dict]
)
async def get_event_history(limit: int = 10):
    if limit <= 0:
        raise HTTPException(status_code=400, detail="Limit must be positive")
    
    events = storage.get_events(limit=limit)
    if not events:
        return [] 
    
    return events



