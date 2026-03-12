from dagster import Definitions, ScheduleDefinition
from PersonDataLoad.defs.assets import term_raw_op, course_raw_op, person_raw_op
from PersonDataLoad.defs.resources import (
    PostgresResource,
    SnapLogicPersonApiResource,
    SnapLogicCourseApiResource,
    PsQueryResource,
    AsyncpgPoolResource,
    VDSApiResource,
    SAPApiResource
)
import dagster as dg

#TODO: Rename PersonDataLoad to DataLakeDataLoad


@dg.job(name="term_raw_job",
        tags={"dagster-k8s/config": {
                "container_config": {
                    "resources": {
                        "requests": {"cpu": "1000m", "memory": "2Gi"},
                        "limits": {"cpu": "2000m", "memory": "3Gi"}
                    }
                },
            }
        }
)
def term_raw_job():
    term_raw_op()()


@dg.job(name="course_raw_job",
        tags={"dagster-k8s/config": {
                "container_config": {
                    "resources": {
                        "requests": {"cpu": "1000m", "memory": "2Gi"},
                        "limits": {"cpu": "2000m", "memory": "3Gi"}
                    }
                },
            }
        }
)
def course_raw_job():
    course_raw_op()()


@dg.job(name="person_raw_job",
        tags={"dagster-k8s/config": {
                "container_config": {
                    "resources": {
                        "requests": {"cpu": "1000m", "memory": "4Gi"},
                        "limits": {"cpu": "2000m", "memory": "6Gi"}
                    }
                },
            }
        }
)
def person_raw_job():
    person_raw_op()()

# Create schedules: 1am term, 2am course, 3am person (ET)
schedule_term = ScheduleDefinition(
    job=term_raw_job,
    cron_schedule="0 1 * * *",
    execution_timezone="America/New_York"
)

schedule_course = ScheduleDefinition(
    job=course_raw_job,
    cron_schedule="0 2 * * *",
    execution_timezone="America/New_York"
)

schedule_person = ScheduleDefinition(
    job=person_raw_job,
    cron_schedule="0 3 * * *",
    execution_timezone="America/New_York"
)

# Define the Dagster Definitions object including assets, resources, jobs, and schedules
defs = Definitions(
    #TODO: Define this in the resources file
    resources={
        "postgres": PostgresResource(),                       # Asynchronous SQLAlchemy resource for PostgreSQL
        "snaplogic_person_api": SnapLogicPersonApiResource(), # Resource for SnapLogic Person API
        "snaplogic_course_api": SnapLogicCourseApiResource(), # Resource for SnapLogic Course API
        "ps_query": PsQueryResource(),                        # Resource for PeopleSoft API
        "asyncpg_pool": AsyncpgPoolResource(),                # Resource for asyncpg connection pool (needed for COPY)
        "vds_api": VDSApiResource(),                          # Resource for VDS API
        "sap_api": SAPApiResource()                           # Resource for SAP API
    },
    jobs=[term_raw_job, course_raw_job, person_raw_job],
    schedules=[schedule_term, schedule_course, schedule_person]
    #executor=dg.multiprocess_executor.configured({"max_concurrent": 1}) # Limit to 1 concurrent asset
)
