"""
Course data loading flow for Prefect.
"""
import asyncio
import json
import httpx
import logging
from datetime import datetime
from typing import List, Dict
from prefect import flow, task
from prefect.cache_policies import NONE as NO_CACHE
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


@task(name="fetch-active-terms", retries=2, retry_delay_seconds=30, tags=["fetch-terms"])
async def fetch_active_terms_task(postgres_engine) -> List[str]:
    """
    Fetch active term codes from the database.
    
    Args:
        postgres_engine: SQLAlchemy async engine.
        
    Returns:
        List[str]: List of active term codes (STRM).
    """
    logger = get_run_logger()
    session_factory = async_sessionmaker(postgres_engine, expire_on_commit=False)
    
    async with session_factory() as session:
        async with session.begin():
            terms = (await session.execute(
                text("SELECT strm FROM term_curated.term_data_by_service WHERE service='active_terms'"),
            )).scalars().all()
    
    logger.info(f"✅ Retrieved {len(terms)} active terms from database")
    return terms


@task(
    name="fetch-course-details-for-term",
    retries=2,
    retry_delay_seconds=30,
    task_run_name="fetch-courses-{term}",
    cache_policy=NO_CACHE,
    tags=["fetch-courses"]
)
async def fetch_course_details_for_term_task(
    term: str,
    snaplogic_config: dict
) -> List[dict]:
    """
    Fetch course details for a specific term from SnapLogic API.
    
    Args:
        term (str): The term code to fetch courses for.
        snaplogic_config (dict): SnapLogic API configuration.
        
    Returns:
        List[dict]: List of course groups for the term.
    """
    logger = get_run_logger()
    
    async with httpx.AsyncClient() as client:
        try:
            logger.info(f"📡 Fetching courses for term {term}...")
            fetch_start = datetime.now()
            resp = await client.get(
                snaplogic_config["url"],
                params={"term": term, "csEnv": snaplogic_config["cs_env"]},
                headers=snaplogic_config["headers"],
                timeout=36000,
            )
            resp.raise_for_status()
            result = resp.json()
            fetch_duration = (datetime.now() - fetch_start).total_seconds()
            course_count = len(result) if isinstance(result, list) else 0
            logger.info(f"✅ Term {term}: Retrieved {course_count} course groups in {fetch_duration:.2f}s")
            return result
        except Exception as e:
            logger.error(f"❌ Course request failed for term {term}: {type(e).__name__}: {e}")
            raise


@task(name="flatten-course-data", cache_policy=NO_CACHE, tags=["transform"])
async def flatten_course_data_task(courses_list: List[List[dict]]) -> List[Dict]:
    """
    Flatten nested course data into a flat list of records.
    
    Args:
        courses_list (List[List[dict]]): List of course groups from multiple terms.
        
    Returns:
        List[Dict]: Flattened list of course records ready for insertion.
    """
    logger = get_run_logger()
    logger.info(f"🔄 Flattening course data...")
    
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
    
    logger.info(f"✅ Flattened {len(courses)} total course records")
    return courses


@task(
    name="insert-courses-batch",
    retries=3,
    retry_delay_seconds=10,
    task_run_name="insert-batch-{batch_num}",
    cache_policy=NO_CACHE,
    tags=["insert-courses"]
)
async def insert_courses_batch_task(
    batch: List[Dict],
    batch_num: int,
    total_batches: int,
    asyncpg_pool,
    insert_sem: asyncio.Semaphore,
    metrics: dict
) -> dict:
    """
    Insert a batch of course records into the database.
    
    Args:
        batch (List[Dict]): Course records to insert.
        batch_num (int): Batch number for logging.
        total_batches (int): Total number of batches.
        asyncpg_pool: Database connection pool.
        insert_sem (asyncio.Semaphore): Semaphore to limit concurrent inserts.
        metrics (dict): Shared metrics dictionary.
        
    Returns:
        dict: Summary with inserted count, skipped count, and error count.
    """
    logger = get_run_logger()
    batch_start = datetime.now()
    
    async with insert_sem:
        records = []
        skipped = 0
        
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
                skipped += 1
                metrics["type_skipped"] += 1
                logger.warning(f"⚠️  Batch {batch_num}/{total_batches}: Skipping record - {type(e).__name__}: {e}")
        
        if not records:
            logger.warning(f"⚠️  Batch {batch_num}/{total_batches}: No valid records after type conversion")
            return {"batch_num": batch_num, "inserted": 0, "skipped": skipped, "errors": len(batch)}
        
        query = (
            """
            INSERT INTO course_raw.course_data
            (academic_career, term_code, course_id, session_code, course_data)
            VALUES ($1, $2, $3, $4, $5::jsonb)
            """
        )
        
        max_retries = 3
        base_delay = 2.0
        
        for attempt in range(1, max_retries + 1):
            try:
                async with asyncpg_pool.acquire() as conn:
                    await conn.executemany(query, records)
                batch_duration = (datetime.now() - batch_start).total_seconds()
                metrics["insert_success"] += len(records)
                metrics["batches_completed"] += 1
                logger.info(f"✅ Batch {batch_num}/{total_batches}: Inserted {len(records)} records in {batch_duration:.2f}s ({metrics['batches_completed']}/{total_batches} batches complete)")
                return {"batch_num": batch_num, "inserted": len(records), "skipped": skipped, "errors": 0}
            except Exception as e:
                if attempt < max_retries:
                    delay = base_delay * (2 ** (attempt - 1))
                    logger.warning(f"🔄 Batch {batch_num}/{total_batches}: Retry {attempt}/{max_retries} - {type(e).__name__}: {e}. Waiting {delay:.1f}s...")
                    await asyncio.sleep(delay)
                else:
                    metrics["errors"] += len(batch)
                    batch_duration = (datetime.now() - batch_start).total_seconds()
                    logger.error(f"❌ Batch {batch_num}/{total_batches}: Failed after {max_retries} attempts ({batch_duration:.2f}s) - {type(e).__name__}: {e}")
                    return {"batch_num": batch_num, "inserted": 0, "skipped": skipped, "errors": len(batch)}


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

    asyncpg_pool = await asyncpg.create_pool(**asyncpg_pool_config)

    # Step 1: Fetch active terms
    logger.info("\n📡 Step 1: Fetching active terms...")
    terms = await fetch_active_terms_task(postgres_engine)
    
    logger.info(f"╔════════════════════════════════════════════╗")
    logger.info(f"║ FETCHING COURSE DATA FOR {len(terms)} TERMS        ║")
    logger.info(f"╠════════════════════════════════════════════╣")
    logger.info(f"║ Terms: {', '.join(terms):<33} ║")
    logger.info(f"╚════════════════════════════════════════════╝")

    #TODO: Call Get Course Offerings with term first, and merge with course details here

    # Step 2: Fetch course details for each term
    logger.info(f"\n⏳ Step 2: Starting parallel fetch for {len(terms)} terms...")
    courses_list = await asyncio.gather(
        *[fetch_course_details_for_term_task(term, snaplogic_config) for term in terms],
        return_exceptions=True
    )
    
    # Filter out any failed fetches (exceptions)
    successful_courses = [c for c in courses_list if not isinstance(c, Exception)]
    failed_count = len(courses_list) - len(successful_courses)
    if failed_count > 0:
        logger.warning(f"⚠️  {failed_count} term(s) failed to fetch")
    logger.info(f"✅ Completed {len(successful_courses)} term fetches")

    # Step 3: Flatten course data
    logger.info(f"\n🔄 Step 3: Flattening course data...")
    courses = await flatten_course_data_task(successful_courses)
    
    # Step 4: Insert courses in batches
    insert_sem = asyncio.Semaphore(INSERT_SEMAPHORE_LIMIT)
    metrics = {"insert_success": 0, "errors": 0, "type_skipped": 0, "batches_completed": 0, "batches_total": 0}
    
    # Calculate total batches
    total_batches = (len(courses) + INSERT_BATCH_SIZE - 1) // INSERT_BATCH_SIZE
    metrics["batches_total"] = total_batches
    
    logger.info(f"\n💾 Step 4: Starting database insert operations")
    logger.info(f"   • Total records: {len(courses)}")
    logger.info(f"   • Batch size: {INSERT_BATCH_SIZE}")
    logger.info(f"   • Total batches: {total_batches}")
    logger.info(f"   • Concurrent inserts: {INSERT_SEMAPHORE_LIMIT}")

    tasks = []
    batch_num = 0
    for i in range(0, len(courses), INSERT_BATCH_SIZE):
        batch_num += 1
        batch = courses[i:i + INSERT_BATCH_SIZE]
        tasks.append(
            insert_courses_batch_task(
                batch=batch,
                batch_num=batch_num,
                total_batches=total_batches,
                asyncpg_pool=asyncpg_pool,
                insert_sem=insert_sem,
                metrics=metrics
            )
        )

    logger.info(f"⏳ Executing {len(tasks)} batch insert tasks...")
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info(f"✅ All batch operations completed")

    await asyncpg_pool.close()
    await postgres_engine.dispose()

    # Final summary
    logger.info(f"\n╔════════════════════════════════════════════╗")
    logger.info(f"║      COURSE_RAW_FLOW RUN SUMMARY       ║")
    logger.info(f"╠════════════════════════════════════════════╣")
    logger.info(f"║ Total Records:          {len(courses):>6,}          ║")
    logger.info(f"║ Successfully Inserted:  {metrics['insert_success']:>6,}          ║")
    logger.info(f"║ Type Conversion Errors: {metrics['type_skipped']:>6,}          ║")
    logger.info(f"║ Insert Errors:          {metrics['errors']:>6,}          ║")
    logger.info(f"║ Batches Completed:      {metrics['batches_completed']:>3}/{metrics['batches_total']:<3}          ║")
    logger.info(f"╚════════════════════════════════════════════╝")
    
    if metrics["errors"] == 0 and metrics["type_skipped"] == 0:
        logger.info(f"✅ All {metrics['insert_success']} records inserted successfully")
    elif metrics["errors"] == 0:
        logger.warning(f"⚠️  Inserted {metrics['insert_success']}/{len(courses)} records. {metrics['type_skipped']} skipped due to type conversion")
    else:
        logger.error(f"❌ Inserted {metrics['insert_success']}/{len(courses)} records. Errors: {metrics['errors']}, Skipped: {metrics['type_skipped']}")

    return {
        "status": "success" if metrics["errors"] == 0 else "partial_success",
        "records_inserted": metrics["insert_success"],
        "errors": metrics["errors"],
        "type_skipped": metrics["type_skipped"],
        "batches_completed": metrics["batches_completed"]
    }


if __name__ == "__main__":
    import asyncio
    asyncio.run(course_raw_flow())
