"""
Course data loading flow for Prefect.
"""
import asyncio
import json
import httpx
import logging
from typing import List
from prefect import flow
from prefect.logging import get_run_logger
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy import text
import asyncpg
from config.resources import (
    PostgresResource,
    SnapLogicCourseApiResource,
    AsyncpgPoolResource
)

logging.getLogger("httpx").setLevel(logging.WARNING)


"""
    Create a Prefect flow that retrieves course data from the SnapLogic API
    and prepares it for insertion into the Postgres database.

    Steps:
    1. Extract relevant term codes using the BU Term Query resource.
    2. For each term code, call the SnapLogic Course API to get course details.
    3. Flatten the course data and log the number of records retrieved.
    4. Prepare the course data for insertion into the Postgres database.
"""
@flow(
    name="course-raw-flow",
    description="Retrieves course data from the SnapLogic API and prepares it for insertion into the Postgres database",
    retries=1,
    retry_delay_seconds=300,
    log_prints=True
)
async def course_raw_flow():
    """
    A Prefect flow that retrieves course data from the SnapLogic API
    and prepares it for insertion into the Postgres database.

    Steps:
    1. Extract relevant term codes using the BU Term Query resource.
    2. For each term code, call the SnapLogic Course API to get course details.
    3. Flatten the course data and log the number of records retrieved.
    4. Prepare the course data for insertion into the Postgres database.
    """
    logger = get_run_logger()

    INSERT_BATCH_SIZE = 50
    INSERT_SEMAPHORE_LIMIT = 4

    # Get resources
    postgres_engine = PostgresResource.get_engine()
    snaplogic_config = SnapLogicCourseApiResource.get_config()
    asyncpg_pool_config = AsyncpgPoolResource.get_pool_config()

    session_factory = async_sessionmaker(postgres_engine, expire_on_commit=False)
    asyncpg_pool = await asyncpg.create_pool(**asyncpg_pool_config)

    terms = []

    # Retrieve STRM codes for the current term, its adjacent terms, and a conditional fourth term based on whether the current or previous term is the Summer
    async with session_factory() as session:
        async with session.begin():
            terms = (await session.execute(
                text("SELECT strm FROM term_curated.term_data_by_service WHERE service='active_terms'"),
            )).scalars().all()

    logger.info(f"Fetching course details for terms: {terms}")

    #TODO: Call Get Course Offerings with term first, and merge with course details here

    # Get course details for each term asynchronously
    async def get_course_details(term: str) -> List[str]:
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    snaplogic_config["url"],
                    params={"term": term, "csEnv": snaplogic_config["cs_env"]},
                    headers=snaplogic_config["headers"],
                    timeout=36000,
                )
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.error(f"Course request failed for term {term}: {e}")
                return []

    courses_list = await asyncio.gather(*[get_course_details(term) for term in terms])

    courses = []
    for term_group in courses_list:
        for term in term_group:
            term_details = term.get("termDetails", {})
            academic_career = term_details.get("academicCareer", "")
            term_code = term_details.get("term", {}).get("code", "")
            for course in term.get("courses", []):
                courses.append({
                    "academic_career": academic_career,
                    "term_code": term_code,
                    "course_id": course.get("v2", {}).get("courseId", ""),
                    "session_code": course.get("v2", {}).get("sessionCode", ""),
                    "course_data": json.dumps({**course, "termDetails": term_details}),
                })

    insert_sem = asyncio.Semaphore(INSERT_SEMAPHORE_LIMIT)
    metrics = {"insert_success": 0, "errors": 0, "type_skipped": 0}

    async def batch_insert(batch, max_retries: int = 3, base_delay: float = 2.0):
        async with insert_sem:
            records = []
            for r in batch:
                try:
                    term_code_int = int(r["term_code"]) if r.get("term_code") not in (None, "") else None
                    course_id_int = int(r["course_id"]) if r.get("course_id") not in (None, "") else None
                    if term_code_int is None or course_id_int is None:
                        raise ValueError("Missing numeric term_code or course_id")
                    records.append((
                        r["academic_career"],
                        term_code_int,
                        course_id_int,
                        r["session_code"],
                        r["course_data"],
                    ))
                except Exception as e:
                    metrics["type_skipped"] += 1
                    logger.warning(f"Skipping record due to type conversion issue: {e} | r={ {k: r.get(k) for k in ('academic_career','term_code','course_id','session_code')} }")
            query = (
                """
                INSERT INTO course_raw.course_data
                (academic_career, term_code, course_id, session_code, course_data)
                VALUES ($1, $2, $3, $4, $5::jsonb)
                """
            )
            for attempt in range(1, max_retries + 1):
                try:
                    async with asyncpg_pool.acquire() as conn:
                        await conn.executemany(query, records)
                    metrics["insert_success"] += len(batch)
                    return
                except Exception as e:
                    if attempt < max_retries:
                        delay = base_delay * (2 ** (attempt - 1))
                        logger.warning(f"Batch insert retry {attempt}/{max_retries} after error: {e}. Waiting {delay:.1f}s")
                        await asyncio.sleep(delay)
                    else:
                        metrics["errors"] += len(batch)
                        logger.error(f"Batch insert failed after retries: {e}")

    tasks = []
    for i in range(0, len(courses), INSERT_BATCH_SIZE):
        batch = courses[i:i + INSERT_BATCH_SIZE]
        tasks.append(asyncio.create_task(batch_insert(batch)))

    await asyncio.gather(*tasks)

    await asyncpg_pool.close()
    await postgres_engine.dispose()

    if metrics["errors"] == 0:
        logger.info(f"{metrics['insert_success']} records inserted successfully.")
    else:
        logger.warning(f"Inserted {metrics['insert_success']}/{len(courses)} records. Errors: {metrics['errors']}.")

    return {
        "status": "success" if metrics["errors"] == 0 else "partial_success",
        "records_inserted": metrics["insert_success"],
        "errors": metrics["errors"],
        "type_skipped": metrics["type_skipped"]
    }


if __name__ == "__main__":
    import asyncio
    asyncio.run(course_raw_flow())
