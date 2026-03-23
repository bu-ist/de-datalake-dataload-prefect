"""
Term data loading flow for Prefect.
"""
import json
import httpx
from datetime import datetime
from typing import List, Dict
from prefect import flow, task
from prefect.cache_policies import NONE as NO_CACHE
from prefect.logging import get_run_logger
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy import text
from config.resources import PostgresResource, PsQueryResource, ps_url


@task(name="fetch-terms-from-peoplesoft", retries=2, retry_delay_seconds=30, tags=["fetch-terms"])
async def fetch_terms_from_peoplesoft_task(ps_query_config: dict) -> List[dict]:
    """
    Fetch term data from PeopleSoft BU_TERM_QRY API.
    
    Args:
        ps_query_config (dict): PeopleSoft query configuration.
        
    Returns:
        List[dict]: List of term records from PeopleSoft.
    """
    logger = get_run_logger()
    logger.info("📡 Fetching term data from PeopleSoft BU_TERM_QRY...")
    fetch_start = datetime.now()
    
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                ps_url(ps_query_config["csEnv"], "BU_TERM_QRY"),
                params={"isconnectedquery": "N", "maxrows": 0, "json_resp": "true"},
                headers=ps_query_config["headers"],
                timeout=30,
            )
            resp.raise_for_status()
            rows = resp.json().get("data", {}).get("query", {}).get("rows", [])
        
        fetch_duration = (datetime.now() - fetch_start).total_seconds()
        logger.info(f"✅ Retrieved {len(rows)} term rows in {fetch_duration:.2f}s")
        return rows
    except httpx.HTTPStatusError as e:
        logger.error(f"❌ HTTP error fetching terms: {e.response.status_code} - {e.response.text}")
        raise
    except httpx.TimeoutException as e:
        logger.error(f"❌ Timeout fetching terms after 30s: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Failed BU Term Query request: {type(e).__name__}: {e}")
        raise


@task(name="transform-term-data", cache_policy=NO_CACHE, tags=["transform"])
async def transform_term_data_task(rows: List[dict]) -> List[Dict]:
    """
    Transform term data into records with JSONB format.
    
    Args:
        rows (List[dict]): Raw term records from PeopleSoft.
        
    Returns:
        List[Dict]: Transformed term records ready for insertion.
    """
    logger = get_run_logger()
    logger.info("🔄 Transforming term data to JSONB format...")
    transform_start = datetime.now()
    
    records = [
        {
            "acad_career": t.get("ACAD_CAREER", ""),
            "strm": t.get("STRM", ""),
            "term_data": json.dumps(t),
        }
        for t in rows
    ]
    
    transform_duration = (datetime.now() - transform_start).total_seconds()
    logger.info(f"✅ Transformed {len(records)} records in {transform_duration:.3f}s")
    return records


@task(name="insert-term-data", retries=2, retry_delay_seconds=10, cache_policy=NO_CACHE, tags=["insert-terms"])
async def insert_term_data_task(records: List[Dict], postgres_engine) -> int:
    """
    Truncate and insert term data into the database, then refresh the view.
    
    Args:
        records (List[Dict]): Term records to insert.
        postgres_engine: SQLAlchemy async engine.
        
    Returns:
        int: Number of records inserted.
    """
    logger = get_run_logger()
    logger.info(f"💾 Database operations (Truncate + Insert)...")
    db_start = datetime.now()
    
    session_factory = async_sessionmaker(postgres_engine, expire_on_commit=False)
    async with session_factory() as session, session.begin():
        try:
            # Truncate table
            logger.info("   🗑️  Truncating term_raw.term_data table...")
            truncate_start = datetime.now()
            await session.execute(text("TRUNCATE term_raw.term_data;"))
            truncate_duration = (datetime.now() - truncate_start).total_seconds()
            logger.info(f"   ✅ Table truncated in {truncate_duration:.3f}s")

            # Insert JSONB records
            logger.info(f"   📥 Inserting {len(records)} JSONB records...")
            insert_start = datetime.now()
            await session.execute(
                text("""
                    INSERT INTO term_raw.term_data (acad_career, strm, term_data)
                    VALUES (:acad_career, :strm, :term_data)
                """),
                records,
            )
            insert_duration = (datetime.now() - insert_start).total_seconds()
            logger.info(f"   ✅ Inserted {len(records)} JSONB rows in {insert_duration:.2f}s ({len(records)/insert_duration:.1f} records/sec)")
        except Exception as e:
            logger.error(f"   ❌ JSONB insert failed: {type(e).__name__}: {e}")
            raise

        try:
            # Refresh current term data
            logger.info("   🔄 Refreshing current_term_data view...")
            refresh_start = datetime.now()
            await session.execute(text("SELECT term_raw.refresh_current_term_data();"))
            refresh_duration = (datetime.now() - refresh_start).total_seconds()
            logger.info(f"   ✅ View refreshed in {refresh_duration:.3f}s")
        except Exception as e:
            logger.error(f"   ❌ Failed to refresh current term data: {type(e).__name__}: {e}")
            raise
    
    db_duration = (datetime.now() - db_start).total_seconds()
    logger.info(f"✅ All database operations completed in {db_duration:.2f}s")
    return len(records)


"""
    A Prefect flow that retrieves terms from BU_TERM_QRY
    and prepares it for insertion into the Postgres database.
"""
@flow(
    name="term-raw-flow",
    description="Retrieves terms from BU_TERM_QRY and prepares it for insertion into the Postgres database",
    retries=1,
    retry_delay_seconds=300,
    log_prints=True
)
async def term_raw_flow():
    """
    A Prefect flow that retrieves terms from BU_TERM_QRY
    and prepares it for insertion into the Postgres database.

    Steps:
    1. Fetch term data from PeopleSoft BU_TERM_QRY API
    2. Transform data into records with JSONB format
    3. Truncate and insert into term_raw.term_data table
    4. Refresh current_term_data view
    """
    logger = get_run_logger()
    flow_start = datetime.now()

    logger.info("\n╔══════════════════════════════════════════╗")
    logger.info("║     TERM_RAW_FLOW STARTING            ║")
    logger.info("╚══════════════════════════════════════════╝")

    # Get resources
    postgres_engine = PostgresResource.get_engine()
    ps_query_config = PsQueryResource.get_config()

    # Step 1: Fetch term data from PeopleSoft
    logger.info("\n📡 Step 1: Fetching term data from PeopleSoft...")
    rows = await fetch_terms_from_peoplesoft_task(ps_query_config)

    # Step 2: Transform data into records
    logger.info("\n🔄 Step 2: Transforming term data...")
    records = await transform_term_data_task(rows)
    
    # Step 3: Insert data into database
    logger.info(f"\n💾 Step 3: Inserting data into database...")
    records_inserted = await insert_term_data_task(records, postgres_engine)

    # Cleanup
    await postgres_engine.dispose()
    
    # Final summary
    flow_duration = (datetime.now() - flow_start).total_seconds()
    logger.info(f"\n╔══════════════════════════════════════════╗")
    logger.info(f"║     TERM_RAW_FLOW RUN SUMMARY         ║")
    logger.info(f"╠══════════════════════════════════════════╣")
    logger.info(f"║ Total Records:      {records_inserted:>6,}              ║")
    logger.info(f"║ Status:             SUCCESS            ║")
    logger.info(f"║ Total Duration:     {flow_duration:>6.2f}s            ║")
    logger.info(f"╚══════════════════════════════════════════╝")

    return {
        "status": "success",
        "records_inserted": records_inserted,
        "duration_seconds": flow_duration
    }


if __name__ == "__main__":
    import asyncio
    asyncio.run(term_raw_flow())
