"""
Deployment configuration for term_raw_flow.
Scheduled to run at 1:00 AM ET daily.

Resource Requirements:
- CPU Request: 1000m (1 core)
- CPU Limit: 2000m (2 cores)
- Memory Request: 2Gi
- Memory Limit: 3Gi
"""
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from flows.term_raw_flow import term_raw_flow

# Create schedules: 1am term, 2am course, 3am person (ET)
deployment = Deployment.build_from_flow(
    flow=term_raw_flow,
    name="term-raw-daily",
    version="1.0",
    work_queue_name="default",
    schedule=CronSchedule(
        cron="0 1 * * *",
        timezone="America/New_York"
    ),
    tags=["raw", "term", "peoplesoft"],
    description="Daily term data load from PeopleSoft at 1:00 AM ET",
)

if __name__ == "__main__":
    deployment.apply()
