import asyncio
from typing import List, Tuple
from prefect.logging import get_run_logger


async def batch_insert_with_retry(asyncpg_pool, query: str, records: List[Tuple], max_retries: int = 3, base_delay: float = 2.0) -> None:
    logger = get_run_logger()

    for attempt in range(1, max_retries + 1):
        try:
            async with asyncpg_pool.acquire() as conn:
                await conn.executemany(query, records)
            return
        except Exception as e:
            if attempt < max_retries:
                delay = base_delay * (2 ** (attempt - 1))
                logger.warning(f"🔄 Retry {attempt}/{max_retries} - {type(e).__name__}: {e}. Waiting {delay:.1f}s...")
                await asyncio.sleep(delay)
            else:
                logger.error(f"❌ Failed after {max_retries} attempts - {type(e).__name__}: {e}")
                raise
