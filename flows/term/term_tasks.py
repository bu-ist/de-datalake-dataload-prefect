import json
import httpx
from datetime import datetime
from typing import List
from prefect import task
from prefect.cache_policies import NONE as NO_CACHE
from prefect.logging import get_run_logger


@task(name="fetch-terms-from-cs-tools", retries=2, retry_delay_seconds=30, tags=["fetch-terms"])
async def fetch_terms_from_cs_tools_task(cstools_config: dict) -> List[dict]:
    logger = get_run_logger()
    logger.info("📡 Fetching term data from CS Tools BU_TERM_QRY...")
    logger.info(f"   Endpoint: {cstools_config['url']}")
    fetch_start = datetime.now()

    try:
        async with httpx.AsyncClient(verify=False, follow_redirects=True) as client:
            resp = await client.post(
                cstools_config["url"],
                json={"query_name": "BU_TERM_QRY"},
                headers=cstools_config["headers"],
                timeout=30,
            )
            resp.raise_for_status()
            rows = resp.json().get("data", [])

        logger.info(f"✅ Retrieved {len(rows)} rows in {(datetime.now() - fetch_start).total_seconds():.1f}s")
        return rows
    except Exception as e:
        logger.error(f"❌ Fetch failed: {type(e).__name__}: {e}")
        raise


@task(name="insert-term-data", retries=2, retry_delay_seconds=10, cache_policy=NO_CACHE, tags=["insert-terms"])
async def insert_term_data_task(rows: List[dict], asyncpg_pool) -> int:
    logger = get_run_logger()
    logger.info(f"💾 Database operations (Truncate + Insert)...")
    db_start = datetime.now()

    records = [(t.get("ACAD_CAREER", ""), t.get("STRM", ""), json.dumps(t)) for t in rows]

    async with asyncpg_pool.acquire() as conn:
        async with conn.transaction():
            try:
                await conn.execute("TRUNCATE term_raw.term_data;")
                await conn.executemany(
                    "INSERT INTO term_raw.term_data (acad_career, strm, term_data) VALUES ($1, $2, $3)",
                    records
                )
                await conn.execute("SELECT term_raw.refresh_current_term_data();")
            except Exception as e:
                logger.error(f"❌ DB operation failed: {type(e).__name__}: {e}")
                raise

    logger.info(f"✅ Inserted {len(records)} records in {(datetime.now() - db_start).total_seconds():.2f}s")
    return len(records)
