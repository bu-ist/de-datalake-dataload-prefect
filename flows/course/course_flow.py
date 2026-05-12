import asyncio
import json
from datetime import datetime
from prefect import flow
from prefect.logging import get_run_logger
from config.resources import PostgresResource, CourseApiResource
from flows.course.course_tasks import fetch_active_terms_task, fetch_course_details_for_term_task, insert_courses_batch_task


@flow(name="course-raw-flow", description="Retrieves course data from the course API and prepares it for insertion into the Postgres database", retries=1, retry_delay_seconds=300, log_prints=True)
async def course_raw_flow(test_run: bool = False):
    logger = get_run_logger()
    if test_run:
        logger.warning("TEST RUN MODE: database writes skipped")
    INSERT_BATCH_SIZE = 50
    INSERT_SEMAPHORE_LIMIT = 4

    asyncpg_pool = await PostgresResource.get_pool()
    course_api_config = CourseApiResource.get_config()

    terms = await fetch_active_terms_task(asyncpg_pool)
    logger.info(f"📚 Fetching course data for {len(terms)} terms: {', '.join(terms)}")

    #TODO: Call Get Course Offerings with term first, and merge with course details here

    courses_list = await asyncio.gather(*[fetch_course_details_for_term_task(term, course_api_config) for term in terms], return_exceptions=True)
    
    successful_courses = [c for c in courses_list if not isinstance(c, Exception)]
    if len(successful_courses) < len(courses_list):
        logger.warning(f"⚠️  {len(courses_list) - len(successful_courses)} term(s) failed")

    courses = []
    for term_group in successful_courses:
        for term in term_group:
            term_details = term.get("termDetails", {})
            academic_career = term_details.get("academicCareer", "")
            term_code = term_details.get("term", {}).get("code", "")
            for course in term.get("courses", []):
                courses.append({"academic_career": academic_career, "term_code": term_code, "course_id": course.get("v2", {}).get("courseId", ""), "session_code": course.get("v2", {}).get("sessionCode", ""), "course_data": json.dumps({**course, "termDetails": term_details})})
    
    metrics = {"insert_success": 0, "errors": 0, "type_skipped": 0, "batches_completed": 0, "batches_total": (len(courses) + INSERT_BATCH_SIZE - 1) // INSERT_BATCH_SIZE}

    if not test_run:
        insert_sem = asyncio.Semaphore(INSERT_SEMAPHORE_LIMIT)
        logger.info(f"💾 Inserting {len(courses)} records in {metrics['batches_total']} batches (batch_size={INSERT_BATCH_SIZE}, concurrency={INSERT_SEMAPHORE_LIMIT})")
        tasks = [insert_courses_batch_task(batch=courses[i:i + INSERT_BATCH_SIZE], batch_num=batch_num + 1, total_batches=metrics['batches_total'], asyncpg_pool=asyncpg_pool, insert_sem=insert_sem, metrics=metrics) for batch_num, i in enumerate(range(0, len(courses), INSERT_BATCH_SIZE))]
        await asyncio.gather(*tasks, return_exceptions=True)
    else:
        logger.info(f"TEST RUN: would insert {len(courses)} courses — skipping")

    await asyncpg_pool.close()

    logger.info(f"✅ COURSE_RAW_FLOW COMPLETE - Inserted: {metrics['insert_success']:,}/{len(courses):,} | Errors: {metrics['errors']} | Skipped: {metrics['type_skipped']} | Batches: {metrics['batches_completed']}/{metrics['batches_total']}")

    return {"status": "success" if metrics["errors"] == 0 else "partial_success", "records_inserted": metrics["insert_success"], "errors": metrics["errors"], "type_skipped": metrics["type_skipped"], "batches_completed": metrics["batches_completed"]}


if __name__ == "__main__":
    import asyncio
    asyncio.run(course_raw_flow())
