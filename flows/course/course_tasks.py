import asyncio
import json
import httpx
from datetime import datetime
from typing import List, Dict
from prefect import task
from prefect.cache_policies import NONE as NO_CACHE
from prefect.logging import get_run_logger
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy import text
from flows.utils.db import batch_insert_with_retry


@task(name="fetch-active-terms", retries=2, retry_delay_seconds=30, tags=["fetch-terms"])
async def fetch_active_terms_task(postgres_engine) -> List[str]:
    logger = get_run_logger()
    session_factory = async_sessionmaker(postgres_engine, expire_on_commit=False)
    
    async with session_factory() as session:
        async with session.begin():
            terms = (await session.execute(text("SELECT strm FROM term_curated.term_data_by_service WHERE service='active_terms'"))).scalars().all()
    
    logger.info(f"✅ Retrieved {len(terms)} active terms")
    return terms


@task(name="fetch-course-details-for-term", retries=2, retry_delay_seconds=30, task_run_name="fetch-courses-{term}", cache_policy=NO_CACHE, tags=["fetch-courses"])
async def fetch_course_details_for_term_task(term: str, snaplogic_config: dict) -> List[dict]:
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
            course_count = len(result) if isinstance(result, list) else 0
            logger.info(f"✅ Term {term}: {course_count} groups in {(datetime.now() - fetch_start).total_seconds():.1f}s")
            return result
        except Exception as e:
            logger.error(f"❌ Term {term} failed: {type(e).__name__}: {e}")
            raise


@task(name="insert-courses-batch", retries=3, retry_delay_seconds=10, task_run_name="insert-batch-{batch_num}", cache_policy=NO_CACHE, tags=["insert-courses"])
async def insert_courses_batch_task(batch: List[Dict], batch_num: int, total_batches: int, asyncpg_pool, insert_sem: asyncio.Semaphore, metrics: dict) -> dict:
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
                records.append((r["academic_career"], term_code_int, course_id_int, r["session_code"], r["course_data"]))
            except Exception as e:
                skipped += 1
                metrics["type_skipped"] += 1
                logger.warning(f"⚠️  Batch {batch_num}/{total_batches}: Skipping record - {type(e).__name__}: {e}")
        
        if not records:
            logger.warning(f"⚠️  Batch {batch_num}/{total_batches}: No valid records after type conversion")
            return {"batch_num": batch_num, "inserted": 0, "skipped": skipped, "errors": len(batch)}
        
        query = "INSERT INTO course_raw.course_data (academic_career, term_code, course_id, session_code, course_data) VALUES ($1, $2, $3, $4, $5::jsonb)"
        
        try:
            await batch_insert_with_retry(asyncpg_pool, query, records, max_retries=3)
            metrics["insert_success"] += len(records)
            metrics["batches_completed"] += 1
            logger.info(f"✅ Batch {batch_num}/{total_batches}: {len(records)} records")
            return {"batch_num": batch_num, "inserted": len(records), "skipped": skipped, "errors": 0}
        except Exception as e:
            metrics["errors"] += len(batch)
            logger.error(f"❌ Batch {batch_num}/{total_batches}: {type(e).__name__}: {e}")
            return {"batch_num": batch_num, "inserted": 0, "skipped": skipped, "errors": len(batch)}
