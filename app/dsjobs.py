"""
authors: Kai Middlebrook
"""

import os

from celery import Celery
from celery.schedules import crontab  # noqa: F401
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

app = Celery(
    broker=os.getenv("CELERY_BROKER_URL", None),
    backend=os.getenv("CELERY_RESULT_BACKEND", None),
    include=["tracks", "utils"],
    timezone=os.getenv("CELERY_TIMEZONE", "UTC"),
)
# app.conf.beat_schedule = {
#     "playlists": {
#         "task": "spotify.flows.playlists_flow",
#         "schedule": crontab(hour=6, minute=30, day_of_week="tue,fri"),
#     },
#     "artists_discographies": {
#         "task": "spotify.flows.artists_discographies",
#         "schedule": crontab(hour=12, minute=30, day_of_week="fri"),
#     },
#     # "artists_infos": {
#     #     "task": "spotify.flows.artists_infos",
#     #     "schedule": crontab(hour=22, minute=0, day_of_week="wed"),
#     # },
#     "stream_counts": {
#         "task": "spotify.flows.stream_counts",
#         "schedule": crontab(hour=12, minute=30, day_of_week="tue,sat"),
#     },
# }
app.conf.broker_heartbeat = 0


if __name__ == "__main__":
    app.start()
