import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, Response
from prometheus_client import Histogram, generate_latest, CONTENT_TYPE_LATEST

consume_latency = Histogram(
    "consume_latency_seconds",
    "Latency from event production to consumption",
    buckets=[0.01, 0.05, 0.1, 0.5, 1, 2, 5],
)

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9094")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "number-added")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "service-b-group")
SUM_FILE_PATH = os.environ.get("SUM_FILE_PATH", "sum_state.json")

# In-memory state, mirrored to disk after every update.
# This dict is only ever touched from the single asyncio event loop
# (the consumer loop and the /sum handler both run on it), so no
# separate locking is needed here.
state = {"sum": 0.0, "updated_at": None, "last_event_id": None}


def load_state_from_disk():
    if os.path.exists(SUM_FILE_PATH):
        with open(SUM_FILE_PATH, "r") as f:
            saved = json.load(f)
            state.update(saved)
            print(f"[state] resumed from {SUM_FILE_PATH}: {state}")


def save_state_to_disk():
    # Write to a temp file then rename, so a crash mid-write never leaves
    # a half-written, corrupt sum_state.json behind.
    tmp_path = SUM_FILE_PATH + ".tmp"
    with open(tmp_path, "w") as f:
        # Use json.dump instead of json.dumps to write directly to the file
        # We write the state to the temp file first, then rename it to the final path.
        json.dump(state, f)
    # Rename the temp file to the final path, replacing any existing file.
    os.replace(tmp_path, SUM_FILE_PATH)


async def consume_loop():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",  # if this is a brand-new consumer group, start from the beginning of the topic
    )
    await consumer.start()
    print(f"[kafka] consumer subscribed to '{KAFKA_TOPIC}' on {KAFKA_BROKER}")
    try:
        async for msg in consumer:
            event = json.loads(msg.value)
            
            # The produced_at field is in ISO 8601 format, but it ends with a "Z" to indicate UTC time. The datetime.fromisoformat method does not accept the "Z" suffix, so we replace it with "+00:00" to indicate UTC offset.
            produced_at = datetime.fromisoformat(event["produced_at"].replace("Z", "+00:00"))
            latency = (datetime.now(timezone.utc) - produced_at).total_seconds()
            consume_latency.observe(latency)

            state["sum"] += event["value"]
            state["updated_at"] = datetime.now(timezone.utc).isoformat()
            state["last_event_id"] = event.get("event_id")
            save_state_to_disk()
            print(f"[kafka] consumed event {event.get('event_id')}, running total = {state['sum']}")
    finally:
        await consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_state_from_disk()
    task = asyncio.create_task(consume_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(lifespan=lifespan)


@app.get("/sum")
def get_sum():
    """Returns the running total of every sum published by Service A so far."""
    return state

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/healthz")
def healthz():
    return {"status": "ok"}