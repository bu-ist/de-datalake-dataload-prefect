"""
Term data loading flow for Prefect.
"""
import json
import httpx
from prefect import flow
from prefect.logging import get_run_logger
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy import text
from config.resources import PostgresResource, PsQueryResource, ps_url


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

    # Get resources
    postgres_engine = PostgresResource.get_engine()
    ps_query_config = PsQueryResource.get_config()

    try:
        # Fetch term data from PeopleSoft
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                ps_url(ps_query_config["csEnv"], "BU_TERM_QRY"),
                params={"isconnectedquery": "N", "maxrows": 0, "json_resp": "true"},
                headers=ps_query_config["headers"],
                timeout=30,
            )
            resp.raise_for_status()
            rows = resp.json().get("data", {}).get("query", {}).get("rows", [])
        logger.info(f"Retrieved {len(rows)} term rows.")
    except Exception as e:
        logger.error(f"Failed BU Term Query HTTP request: {e}")
        raise

    # Transform data into records
    records = [
        {
            "acad_career": t.get("ACAD_CAREER", ""),
            "strm": t.get("STRM", ""),
            "term_data": json.dumps(t),
        }
        for t in rows
    ]

    # Insert data into database
    session_factory = async_sessionmaker(postgres_engine, expire_on_commit=False)
    async with session_factory() as session, session.begin():
        try:
            # Truncate table
            await session.execute(text("TRUNCATE term_raw.term_data;"))

            # Insert JSONB records
            await session.execute(
                text("""
                    INSERT INTO term_raw.term_data (acad_career, strm, term_data)
                    VALUES (:acad_career, :strm, :term_data)
                """),
                records,
            )

            logger.info(f"Inserted {len(records)} JSONB rows into term_raw.term_data.")
        except Exception as e:
            logger.error(f"JSONB insert failed: {e}")
            raise

        try:
            # Refresh current term data
            await session.execute(text("SELECT term_raw.refresh_current_term_data();"))
            logger.info("Refreshed current_term_data from JSONB.")
        except Exception as e:
            logger.error(f"Failed to refresh current term data: {e}")
            raise

    # Cleanup
    await postgres_engine.dispose()

    return {
        "status": "success",
        "records_inserted": len(records)
    }


if __name__ == "__main__":
    import asyncio
    asyncio.run(term_raw_flow())
