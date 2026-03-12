"""
Deployment configuration for person_raw_flow.
Scheduled to run at 3:00 AM ET daily.

Resource Requirements:
- CPU Request: 1000m (1 core)
- CPU Limit: 2000m (2 cores)
- Memory Request: 4Gi
- Memory Limit: 6Gi
Note: Person flow requires more memory than term/course flows due to large dataset processing
"""
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from flows.person_raw_flow import person_raw_flow

deployment = Deployment.build_from_flow(
    flow=person_raw_flow,
    name="person-raw-daily",
    version="1.0",
    work_queue_name="default",
    schedule=CronSchedule(
        cron="0 3 * * *",
        timezone="America/New_York"
    ),
    tags=["raw", "person", "snaplogic"],
    description="Daily person data load from SnapLogic at 3:00 AM ET",
)

if __name__ == "__main__":
    deployment.apply()
