import logging
import os
import time
from datetime import datetime, timezone
from functools import lru_cache

from messaging import RESULTS, TASKS, consume, publish

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("pika").setLevel(logging.WARNING)
log = logging.getLogger(__name__)


@lru_cache(maxsize=1024)
def compute(job_id, text):
    log.info("Processing job id=%s", job_id)
    time.sleep(2.5)
    return {
        "id": job_id,
        "result": text.upper(),
        "completedAt": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }


def process_job(channel, message, redelivered):
    job_id = message["id"]
    log.info("Received job id=%s redelivered=%s", job_id, redelivered)
    completed = compute(job_id, message["text"])
    publish(channel, RESULTS, "JobCompleted", completed)
    log.info("JobCompleted confirmed by broker id=%s", job_id)

    if os.environ.get("DEMO_CRASH_BEFORE_ACK") == "1" and not redelivered:
        log.warning("Demo crash after publishing and before ACK id=%s", job_id)
        os._exit(42)


if __name__ == "__main__":
    consume(TASKS, "JobRequested", ("id", "text"), process_job, confirms=True)
