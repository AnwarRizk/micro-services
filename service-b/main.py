import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI

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
        json.dump(state, f)
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


@app.get("/healthz")
def healthz():
    return {"status": "ok"}