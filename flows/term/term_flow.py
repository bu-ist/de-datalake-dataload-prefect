from datetime import datetime
from prefect import flow
from prefect.logging import get_run_logger
from config.resources import PostgresResource, CsToolsResource
from flows.term.term_tasks import fetch_terms_from_cs_tools_task, insert_term_data_task
from flows.utils.logging_helpers import log_step_header


@flow(name="term-raw-flow", description="Retrieves terms from BU_TERM_QRY via Campus Solutions Tools API and prepares it for insertion into the Postgres database", retries=1, retry_delay_seconds=300, log_prints=True)
async def term_raw_flow():
    logger = get_run_logger()
    flow_start = datetime.now()
    logger.info("🚀 TERM_RAW_FLOW STARTING")

    asyncpg_pool = await PostgresResource.get_pool()
    cstools_config = CsToolsResource.get_config()

    log_step_header(logger, 1, "Fetching term data from Campus Solutions Tools")
    rows = await fetch_terms_from_cs_tools_task(cstools_config)

    log_step_header(logger, 2, "Inserting data into database")
    records_inserted = await insert_term_data_task(rows, asyncpg_pool)

    await asyncpg_pool.close()
    
    flow_duration = (datetime.now() - flow_start).total_seconds()
    logger.info(f"\n✅ TERM_RAW_FLOW COMPLETE - Records: {records_inserted:,} | Duration: {flow_duration:.2f}s | Status: SUCCESS")

    return {"status": "success", "records_inserted": records_inserted, "duration_seconds": flow_duration}


if __name__ == "__main__":
    import asyncio
    asyncio.run(term_raw_flow())
