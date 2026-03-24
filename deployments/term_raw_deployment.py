from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from flows.term.term_flow import term_raw_flow

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
