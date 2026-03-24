from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from flows.person.person_flow import person_raw_flow

deployment = Deployment.build_from_flow(
    flow=person_raw_flow,
    name="person-raw-daily",
    version="1.0",
    work_queue_name="default",
    schedule=CronSchedule(
        cron="0 3 * * *",
        timezone="America/New_York"
    ),
    tags=["raw", "person", "de-person-api"],
    description="Daily person data load from Data Engineering Person API at 3:00 AM ET",
)

if __name__ == "__main__":
    deployment.apply()
