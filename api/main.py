import asyncio
import logging
from contextlib import asynccontextmanager
from threading import Event, Lock, Thread
from uuid import uuid4

import pika
from fastapi import FastAPI, HTTPException, Response, status
from pydantic import BaseModel, Field

from messaging import RESULTS, TASKS, connect, consume, declare_queues, publish

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("pika").setLevel(logging.WARNING)
log = logging.getLogger(__name__)
jobs: dict[str, dict] = {}
jobs_lock = Lock()


class JobInput(BaseModel):
    text: str = Field(min_length=1, max_length=2000)


def apply_result(_channel, message, _redelivered):
    job_id = message["id"]
    with jobs_lock:
        job = jobs.get(job_id)
        if job is None:
            log.info("Ignoring result for unknown job %s", job_id)
        elif job["status"] == "DONE":
            log.info("Ignoring duplicate JobCompleted id=%s", job_id)
        else:
            jobs[job_id] = {
                "id": job_id,
                "status": "DONE",
                "result": message["result"],
                "completedAt": message["completedAt"],
            }
            log.info("Job is DONE id=%s", job_id)


@asynccontextmanager
async def lifespan(_app):
    stop = Event()
    listener = Thread(
        target=consume,
        args=(RESULTS, "JobCompleted", ("id", "result", "completedAt"), apply_result, stop),
        name="results-listener",
        daemon=True,
    )
    listener.start()
    try:
        yield
    finally:
        stop.set()
        await asyncio.to_thread(listener.join, 7)


app = FastAPI(title="Async Jobs", lifespan=lifespan)


@app.post("/jobs", status_code=status.HTTP_202_ACCEPTED)
def create_job(body: JobInput, response: Response):
    job_id = str(uuid4())
    accepted = {"id": job_id, "status": "ACCEPTED"}
    # Сначала сохраняем запись, чтобы слушатель не пропустил быстрый результат.
    with jobs_lock:
        jobs[job_id] = accepted

    try:
        with connect() as connection:
            channel = connection.channel()
            declare_queues(channel)
            channel.confirm_delivery()
            publish(channel, TASKS, "JobRequested", {"id": job_id, "text": body.text})
    except (pika.exceptions.AMQPError, OSError) as exc:
        with jobs_lock:
            jobs.pop(job_id, None)
        log.warning("Could not confirm submission %s: %s", job_id, exc)
        raise HTTPException(503, "Could not confirm job publication") from exc

    response.headers["Location"] = f"/jobs/{job_id}"
    return accepted


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)
    if job is None:
        raise HTTPException(404, "Job not found")
    return job
