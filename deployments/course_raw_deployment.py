from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from flows.course.course_flow import course_raw_flow

deployment = Deployment.build_from_flow(
    flow=course_raw_flow,
    name="course-raw-daily",
    version="1.0",
    work_queue_name="default",
    schedule=CronSchedule(
        cron="0 2 * * *",
        timezone="America/New_York"
    ),
    tags=["raw", "course"],
    description="Daily course data load at 2:00 AM ET",
)

if __name__ == "__main__":
    deployment.apply()
