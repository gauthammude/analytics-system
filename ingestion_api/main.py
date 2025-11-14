from fastapi import FastAPI
from pydantic import BaseModel
import redis

app = FastAPI(title="Ingestion API")

# Redis client
r = redis.Redis(host="redis", port=6379, decode_responses=True)

class Event(BaseModel):
    site_id: str
    event_type: str
    path: str
    user_id: str
    timestamp: str

@app.post("/event")
def ingest_event(event: Event):
    """
    Push event to Redis queue for asynchronous processing.
    """
    r.lpush("events_queue", event.json())
    return {"status": "ok"}
