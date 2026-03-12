"""
Deployment configuration for course_raw_flow.
Scheduled to run at 2:00 AM ET daily.

Resource Requirements:
- CPU Request: 1000m (1 core)
- CPU Limit: 2000m (2 cores)
- Memory Request: 2Gi
- Memory Limit: 3Gi
"""
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from flows.course_raw_flow import course_raw_flow

deployment = Deployment.build_from_flow(
    flow=course_raw_flow,
    name="course-raw-daily",
    version="1.0",
    work_queue_name="default",
    schedule=CronSchedule(
        cron="0 2 * * *",
        timezone="America/New_York"
    ),
    tags=["raw", "course", "snaplogic"],
    description="Daily course data load from SnapLogic at 2:00 AM ET",
)

if __name__ == "__main__":
    deployment.apply()
