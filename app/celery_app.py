import os
import multiprocessing
from celery import Celery
from app.config import settings

def get_cpu_limit():
    # cgroup v2 (modern systems)
    try:
        with open("/sys/fs/cgroup/cpu.max") as f:
            quota, period = f.read().strip().split()
            if quota != "max":
                return max(1, int(int(quota) / int(period)))
    except Exception:
        pass

    # fallback
    return multiprocessing.cpu_count()

def calculate_concurrency():
    cores = get_cpu_limit()
    return max(1, cores - 1)

celery_app = Celery(
    "ifrs9pro",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND
)

celery_app.conf.update(
    worker_send_task_events=True,
    task_send_sent_event=True,

    worker_enable_remote_control=True,
    worker_disable_remote_control=False,

    broker_connection_retry_on_startup=True,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
    task_time_limit=7200,  # 2 hour timeout as a safety buffer
)

# Dynamically set concurrency based on CPU limits
celery_app.conf.worker_concurrency = calculate_concurrency()

# We will create these modules next
celery_app.autodiscover_tasks(["app.tasks.ingestion", "app.tasks.calculation"])
