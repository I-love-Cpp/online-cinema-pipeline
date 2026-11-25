import asyncio
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from avro_schema import MOVIE_EVENT_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

app = FastAPI(title="Movie Events Producer")

sr_client = SchemaRegistryClient({"url": "http://schema-registry:8081"})
avro_serializer = AvroSerializer(sr_client, MOVIE_EVENT_SCHEMA)

producer = Producer({
    "bootstrap.servers": "kafka:29092",
    "acks": "all",
    "retries": 5,
    "retry.backoff.ms": 200,
    "enable.idempotence": True,
})

TOPIC = "movie-events"

ALLOWED_EVENTS = {"VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED",
                  "VIEW_RESUMED", "LIKED", "SEARCHED"}
ALLOWED_DEVICES = {"MOBILE", "DESKTOP", "TV", "TABLET"}


class EventIn(BaseModel):
    event_id: Optional[str] = None
    user_id: str
    movie_id: str
    event_type: str
    timestamp: Optional[datetime] = None
    device_type: str
    session_id: str
    progress_seconds: Optional[int] = Field(default=None, ge=0)

    @field_validator("event_type")
    @classmethod
    def _check_event(cls, v):
        if v not in ALLOWED_EVENTS:
            raise ValueError(f"invalid event_type: {v}")
        return v

    @field_validator("device_type")
    @classmethod
    def _check_device(cls, v):
        if v not in ALLOWED_DEVICES:
            raise ValueError(f"invalid device_type: {v}")
        return v


def _on_delivery(err, msg):
    if err:
        log.error("Delivery failed: %s", err)
    else:
        log.info("Published event_id=%s type=%s ts=%s partition=%s offset=%s",
                 msg.key(), msg.topic(), msg.timestamp()[1], msg.partition(), msg.offset())


def _send_with_retry(event: dict, max_attempts: int = 5):
    key = event["user_id"].encode("utf-8")
    value = avro_serializer(event, SerializationContext(TOPIC, MessageField.VALUE))
    for attempt in range(1, max_attempts + 1):
        try:
            producer.produce(TOPIC, key=key, value=value, on_delivery=_on_delivery)
            producer.poll(0)
            return
        except BufferError:
            producer.poll(0.5)
        except Exception as e:
            log.warning("Publish attempt %d failed: %s", attempt, e)
            time.sleep(2 ** attempt)
    raise RuntimeError("Failed to publish event after retries")


@app.post("/events")
async def publish_event(ev: EventIn):
    event = ev.model_dump()
    event["event_id"] = event.get("event_id") or str(uuid.uuid4())
    if event.get("timestamp") is None:
        event["timestamp"] = int(datetime.now(timezone.utc).timestamp() * 1000)
    else:
        event["timestamp"] = int(event["timestamp"].timestamp() * 1000)
    try:
        _send_with_retry(event)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
    return {"event_id": event["event_id"]}


async def _gen_session(user_id: str, movie_id: str, device: str):
    session_id = str(uuid.uuid4())
    base_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    progress = 0

    def ev(etype, prog=None, offset=0):
        return {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "movie_id": movie_id,
            "event_type": etype,
            "timestamp": base_ts + offset,
            "device_type": device,
            "session_id": session_id,
            "progress_seconds": prog if prog is not None else progress,
        }

    _send_with_retry(ev("VIEW_STARTED", 0, 0))
    await asyncio.sleep(random.uniform(1, 2))
    progress += random.randint(60, 300)
    if random.random() < 0.3:
        _send_with_retry(ev("VIEW_PAUSED", progress, 5000))
        await asyncio.sleep(random.uniform(1, 2))
        _send_with_retry(ev("VIEW_RESUMED", progress, 10000))
    if random.random() < 0.8:
        progress = random.randint(3600, 7200)
        _send_with_retry(ev("VIEW_FINISHED", progress, 20000))
    if random.random() < 0.4:
        _send_with_retry(ev("LIKED", None, 25000))


@app.post("/generator/start")
async def start_generator(users: int = 10):
    movies = [f"movie_{i}" for i in range(1, 21)]
    devices = ["MOBILE", "DESKTOP", "TV", "TABLET"]
    for _ in range(users):
        asyncio.create_task(_gen_session(
            f"user_{uuid.uuid4().hex[:6]}",
            random.choice(movies),
            random.choice(devices),
        ))
    return {"status": "generator started", "users": users}
