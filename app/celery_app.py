from celery import Celery
from app.config import settings

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
    task_time_limit=3600,  # 1 hour timeout
    worker_concurrency=2   # Adjust based on resources
)

# We will create these modules next
celery_app.autodiscover_tasks(["app.tasks.ingestion", "app.tasks.calculation"])
