"""
Person data loading flow for Prefect.
"""
import asyncio
import json
import httpx
import math
import traceback
import logging
from datetime import datetime
from typing import Optional, List
from prefect import flow
from prefect.logging import get_run_logger
import asyncpg
from config.resources import (
    AsyncpgPoolResource,
    SnapLogicPersonApiResource,
    PsQueryResource,
    VDSApiResource,
    SAPApiResource,
    ps_url
)

logging.getLogger("httpx").setLevel(logging.WARNING)


"""
    Create a Prefect flow that retrieves person data from the SnapLogic API
    and prepares it for insertion into the Postgres database.

    steps:
    1. Extract BUIDs.
    2. For each BUID, query PeopleSoft to get uidCarTerm data.
    3. Batch uidCarTerm data and send to SnapLogic Person API to get person details.
    4. Insert person data in the Postgres database.
"""
@flow(
    name="person-raw-flow",
    description="Retrieves person data from SnapLogic API and inserts into Postgres database",
    retries=1,
    retry_delay_seconds=300,
    log_prints=True
)
async def person_raw_flow():
    """
    A Prefect flow that retrieves person data from the SnapLogic API
    and prepares it for insertion into the Postgres database.

    Steps:
    1. Extract BUIDs from PeopleSoft, VDS, and SAP APIs.
    2. For each BUID, query PeopleSoft to get uidCarTerm data.
    3. Batch uidCarTerm data and send to SnapLogic Person API to get person details.
    4. Insert person data in the Postgres database.
    """
    logger = get_run_logger()

    UIDCARTERM_GROUP_SIZE = 1000 #1900
    PSQUERY_SEMAPHORE_LIMIT = 10 #10
    SNAPLOGIC_SEMAPHORE_LIMIT = 8 #8
    INSERT_SEMAPHORE_LIMIT = 100 #25

    # Get resource configurations
    asyncpg_pool_config = AsyncpgPoolResource.get_pool_config()
    snaplogic_person_config = SnapLogicPersonApiResource.get_config()
    ps_query_config = PsQueryResource.get_config()
    vds_api_config = VDSApiResource.get_config()
    sap_api_config = SAPApiResource.get_config()

    asyncpg_pool = await asyncpg.create_pool(**asyncpg_pool_config)

    metrics = {
        "ps_queried": 0, "ps_success": 0, "ps_empty": 0,
        "uidcarterm_total": 0, "uidcarterm_estimated": 0,
        "snaplogic_batches_started": 0, "snaplogic_batches_completed": 0, "snaplogic_batches_estimated": 0,
        "persons_received": 0, "insert_success": 0, "insert_skipped": 0,
        "errors": {"ps": 0, "snap": 0, "db": 0},
        "start_time": datetime.now(), "done": False
    }

    buids = []

    # Fetch BUIDs from BU_PARM_0216_QRY query
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                ps_url(ps_query_config["csEnv"], "BU_PARM_0216_QRY"),
                params={"isconnectedquery": "N", "maxrows": 0, "json_resp": "true"},
                headers=ps_query_config["headers"],
                timeout=30,
            )
            resp.raise_for_status()
            rows = resp.json().get("data", {}).get("query", {}).get("rows", [])
        buids.extend([row.get("CAMPUS_ID") for row in rows if row.get("CAMPUS_ID")])
        logger.info(f"Retrieved {len(buids)} BUIDs from BU_PARM_0216_QRY.")
    except Exception as e:
        logger.error(f"Failed to fetch BUIDs from PeopleSoft BU_PARM_0216_QRY: {e}")
        raise

    # TODO: Fetch ENS population Too, or call EVERY Population.

    # Fetch BUIDs from VDS API
    # TODO: Re-enable VDS BUID fetch after new credentials are set up
    # try:
    #     async with httpx.AsyncClient() as client:
    #         resp = await client.get(
    #             vds_api_config["url"],
    #             params={"sizeLimit": "25000", "filter": "(%26(AffiliateAssignmentEndDate=*)(%26(!(PersonPrimaryAffiliation=staff))(!(PersonPrimaryAffiliation=faculty))))"},
    #             headers=vds_api_config["headers"],
    #             timeout=30,
    #         )
    #         resp.raise_for_status()
    #         rows = resp.json().get("entity", {}).get("resources", [])
    #     newBuids = [row.get("attributes").get("buid") for row in rows if row.get("attributes")]
    #     buids.extend(newBuids)
    #     logger.info(f"Retrieved {len(newBuids)} BUIDs from VDS.")
    # except Exception as e:
    #     logger.error(f"Failed to fetch BUIDs from VDS: {e}")
    #     return

    # Fetch BUIDs from SAP API
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                sap_api_config["url"],
                params={"BAPIName": "Z_HR_EMPLOYEE_OBJ_LIST", "account": "HR"},
                json={},
                headers=sap_api_config["headers"],
                timeout=30,
            )
            resp.raise_for_status()
            rows = resp.json().get("ET_EMP_LIST", [])
        newBuids = [row.get("BUID") for row in rows if row.get("EMP_STATUS") == "3 - Active" and row.get("BUID")]
        buids.extend(newBuids)
        logger.info(f"Retrieved {len(newBuids)} BUIDs from SAP Z_HR_EMPLOYEE_OBJ_LIST.")
    except Exception as e:
        logger.error(f"Failed to fetch BUIDs from SAP: {e}")
        raise

    buids = list(set(buids))  # Deduplicate BUIDs
    logger.info(f"Total unique BUIDs to process: {len(buids)}")

    #TODO: Any failed BUIDs will go into the person_live_update queue for reprocessing

    ps_sem = asyncio.Semaphore(PSQUERY_SEMAPHORE_LIMIT)
    snap_sem = asyncio.Semaphore(SNAPLOGIC_SEMAPHORE_LIMIT)
    insert_sem = asyncio.Semaphore(INSERT_SEMAPHORE_LIMIT)

    uidCarTerms, running_snap = [], []
    threshold_event, all_ps_done = asyncio.Event(), asyncio.Event()
    # Set max queue size to prevent memory overflow when inserts are slower than snaplogic returns
    insert_queue: asyncio.Queue = asyncio.Queue(maxsize=20000)
    worker_tasks = []

    async def query_ps(buid: str, ps_client: httpx.AsyncClient, max_retries: int = 5, base_delay: int = 10) -> Optional[Exception]:
        """
        Query the PeopleSoft API for all uidCarTerm data associated with a given BUID,
        and append it to function-level list uidCarTerms.

        Args:
            buid (str): The unique BUID to fetch data for.
            ps_client (httpx.AsyncClient): HTTP client for making the API request.
            max_retries (int, optional): Maximum number of retry attempts for API or network errors.
            base_delay (int, optional): Base delay in seconds between retries, exponentially increased.

        Returns:
            Optional[Exception]: Returns an exception if all retries fail, else None.

        Raises:
            httpx.HTTPError: If the PeopleSoft API request fails after all retries.
        """
        async with ps_sem:
            metrics["ps_queried"] += 1
            for attempt in range(1, max_retries + 1):
                try:
                    req = {"isconnectedquery": "N", "maxrows": 0, "prompt_uniquepromptname": "BUID", "prompt_fieldvalue": buid, "json_resp": "true"}
                    resp = await ps_client.get(ps_url(ps_query_config["csEnv"], "BU_TERM_STD_FULL_TERM"), params=req, timeout=30)
                    resp.raise_for_status()
                    uidCarTerm = resp.json()['data']['query']['rows']

                    #TODO: Add logic for Faculty Terms. Consider if buid is a student and faculty for Terms
                    # req = {"isconnectedquery": "N", "maxrows": 0, "prompt_uniquepromptname": "EMPLID", "prompt_fieldvalue": buid, "json_resp": "true"}
                    # resp = await ps_client.get(ps_url(ps_query_config["csEnv"], "BU_FACULTY_GET"), params=req, timeout=30)
                    # resp.raise_for_status()
                    # facultyTerms = resp.json()['data']['query']['rows']

                    if uidCarTerm:
                        metrics["ps_success"] += 1
                        metrics["uidcarterm_total"] += len(uidCarTerm)
                    else:
                        uidCarTerm = [{"CAMPUS_ID": buid}]
                        metrics["ps_empty"] += 1
                    uidCarTerms.append(uidCarTerm)
                    if len([item for row in uidCarTerms for item in row]) >= UIDCARTERM_GROUP_SIZE:
                        threshold_event.set()
                    break
                except Exception as e:
                    if attempt < max_retries:
                        await asyncio.sleep(base_delay * 3 ** (attempt - 1))
                    else:
                        metrics["errors"]["ps"] += 1
                        logger.error(f"PSQuery error for BUID {buid}: {e}")
                        return e

    async def query_snap_and_insert(uidCarTerms: str, snap_client: httpx.AsyncClient, max_retries: int = 5, base_delay: int = 10) -> Optional[int]:
        """
        Query SnapLogic personBatch with a batch of uidCarTerm records, then enqueue insert_person tasks.

        Args:
            uidCarTerms (str): JSON-like string representation of uidCarTerm data batch.
            snap_client (httpx.AsyncClient): HTTP client for making the SnapLogic request.
            max_retries (int, optional): Maximum number of retry attempts for API or network errors.
            base_delay (int, optional): Base delay in seconds between retries, exponentially increased.

        Returns:
            Optional[int]: Number of person records retrieved, or an exception if failed after retries.

        Raises:
            httpx.HTTPError: If the SnapLogic API request fails after all retries.
            json.JSONDecodeError: If the response is not a valid JSON.
        """
        async with snap_sem:
            metrics["snaplogic_batches_started"] += 1
            for attempt in range(1, max_retries + 1):
                try:
                    #TODO: Snap takes about 5 minutes to finish. Timeout after 11 minutes. Log any buids that failed or insert them into queue to redo (live updates queue)
                    resp = await snap_client.post(
                        snaplogic_person_config["url"],
                        json={"uidCarTerm": str(uidCarTerms)},
                        params={"objects": "['student','affiliate','faculty','employee']", "csEnv": snaplogic_person_config["cs_env"]},
                        timeout=10000
                    )
                    resp.raise_for_status()
                    persons = resp.json()
                    persons = [p for p in persons if p.get("personid")] #TODO: Temp fix for glitch in SnapLogic returning empty objects
                    metrics["persons_received"] += len(persons)
                    metrics["snaplogic_batches_completed"] += 1

                    # Enqueue each person for single-record insertion
                    for p in persons:
                        await insert_queue.put(p)
                    return len(persons)
                except Exception as e:
                    if attempt < max_retries:
                        await asyncio.sleep(base_delay * 3 ** (attempt - 1))
                    else:
                        metrics["errors"]["snap"] += 1
                        logger.error(f"SnapLogic error: {e}\n{traceback.format_exc()}")
                        return e

    async def insert_worker() -> None:
        """
        Worker that pulls single person records from a queue and inserts
        them individually, respecting the insert semaphore.
        """
        while True:
            p = await insert_queue.get()
            if p is None:
                insert_queue.task_done()
                break
            try:
                uid = p.get("personid")
                if not uid:
                    metrics["insert_skipped"] += 1
                    insert_queue.task_done()
                    continue

                for k in ("ssn", "socialSecurityNumber", "sexualOrientation"):
                    p.get("personBasic", {}).pop(k, None)
                for k in ("finAid", "finAidReceived"):
                    p.get("studentInfo", {}).pop(k, None)

                async with insert_sem:
                    async with asyncpg_pool.acquire() as conn:
                        await conn.execute(
                            """
                            INSERT INTO person_raw.person_data (bu_uid, person_data)
                            VALUES ($1, $2::jsonb)
                            """,
                            uid,
                            json.dumps(p),
                        )
                metrics["insert_success"] += 1
            except Exception:
                metrics["errors"]["db"] += 1
                logger.error("Single insert failed", exc_info=True)
            finally:
                insert_queue.task_done()

    async def monitor_progress(interval: int = 15) -> None:
        """
        Monitor the ETL process and periodically log status metrics and estimated completion time.

        Args:
            interval (int, optional): Interval in seconds between each status log.

        Returns:
            None

        Logs:
            Current progress, resource usage, and time estimates.
        """
        total_buids = len(buids)
        start = metrics["start_time"]
        last_valid_total_runtime = None
        try:
            while not metrics["done"]:
                elapsed = (datetime.now() - start).total_seconds()
                ps_done = metrics["ps_queried"]
                progress = ps_done / total_buids if total_buids else 0
                total_estimated_runtime = None
                if metrics["snaplogic_batches_completed"] > 0:
                    est_batches_total = metrics["snaplogic_batches_estimated"]
                    done_batches = metrics["snaplogic_batches_completed"]
                    effective_done_batches = max(done_batches, 8)
                    remaining_batches = max(est_batches_total - done_batches, 0)
                    avg_batch_time = elapsed / effective_done_batches
                    eta = avg_batch_time * remaining_batches
                    total_estimated_runtime = elapsed + eta
                    last_valid_total_runtime = total_estimated_runtime
                if total_estimated_runtime is None and last_valid_total_runtime is not None:
                    total_estimated_runtime = last_valid_total_runtime
                eta_str = (
                    f"{int(total_estimated_runtime//3600):02}:"
                    f"{int((total_estimated_runtime%3600)//60):02}:"
                    f"{int(total_estimated_runtime%60):02}"
                    if total_estimated_runtime is not None else "N/A"
                )
                elapsed_str = (
                    f"{int(elapsed//3600):02}:"
                    f"{int((elapsed%3600)//60):02}:"
                    f"{int(elapsed%60):02}"
                )
                if ps_done > 0:
                    avg_uid = metrics["uidcarterm_total"] / ps_done
                    est_uid = int(avg_uid * total_buids)
                    metrics["uidcarterm_estimated"] = est_uid
                    metrics["snaplogic_batches_estimated"] = math.ceil(est_uid / UIDCARTERM_GROUP_SIZE)
                ps_active = PSQUERY_SEMAPHORE_LIMIT - ps_sem._value
                snap_active = SNAPLOGIC_SEMAPHORE_LIMIT - snap_sem._value
                insert_active = INSERT_SEMAPHORE_LIMIT - insert_sem._value
                logger.info(
                    f"[HEARTBEAT {elapsed_str} | ETA: {eta_str}]"
                    f"\n  PS Queries:        {ps_done:,}/{total_buids:,} ({progress*100:.1f}%) — success: {metrics['ps_success']:,} | empty: {metrics['ps_empty']:,}"
                    f"\n  uidCarTerms:       {metrics['uidcarterm_total']:,} collected (est. {metrics['uidcarterm_estimated'] or '?'} total)"
                    f"\n  SnapLogic Batches: {metrics['snaplogic_batches_started']:,} started / {metrics['snaplogic_batches_completed']:,} completed / est {metrics['snaplogic_batches_estimated'] or '?'} total"
                    f"\n  Persons Returned:  {metrics['persons_received']:,}"
                    f"\n  Inserts:           {metrics['insert_success']:,} new | {metrics['insert_skipped']:,} skipped"
                    f"\n  Semaphores:        PS={ps_active}/{PSQUERY_SEMAPHORE_LIMIT} | Snap={snap_active}/{SNAPLOGIC_SEMAPHORE_LIMIT} | Inserts={insert_active}/{INSERT_SEMAPHORE_LIMIT}"
                    f"\n  Errors:            PS={metrics['errors']['ps']} | Snap={metrics['errors']['snap']} | DB={metrics['errors']['db']}"
                    f"\n  Elapsed:           {elapsed_str} | ETA: {eta_str}"
                )
                await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"⚠️ Heartbeat error: {e}")

    async def process_uidCarTerms_batch(batch: List[List[dict]], snap_client: httpx.AsyncClient) -> None:
        """
        Process a single batch of at most UIDCARTERM_GROUP_SIZE uidCarTerm records
        by flattening and sending them to the SnapLogic personBatch task.

        Args:
            batch (List[List[dict]]): A list of uidCarTerm result sets.
            snap_client (httpx.AsyncClient): HTTP client for SnapLogic requests.

        Returns:
            None

        Raises:
            json.JSONDecodeError: If JSON serialization fails.
        """
        flattened = [item for row in batch for item in row]
        if not flattened:
            return
        uidCarTerms_str = "[" + ",".join("{" + ",".join(
                f'{("BUID" if k=="CAMPUS_ID" else k)}:\"{v}\"' for k, v in item.items() if k != "attr:rownumber"
            ) + "}" for item in flattened) + "]"
        task = asyncio.create_task(
            query_snap_and_insert(uidCarTerms_str, snap_client)
        )
        running_snap.append(task)

    async def monitor_uidCarTerms(snap_client: httpx.AsyncClient) -> None:
        """
        Monitor and submit uidCarTerm data batches to SnapLogic as they become ready,
        waiting for either threshold_event or the completion of PS queries.

        Args:
            snap_client (httpx.AsyncClient): HTTP client for SnapLogic requests.

        Returns:
            None
        """
        while True:
            done, _ = await asyncio.wait(
                [asyncio.create_task(threshold_event.wait()), asyncio.create_task(all_ps_done.wait())],
                return_when=asyncio.FIRST_COMPLETED)
            if threshold_event.is_set():
                threshold_event.clear()
                snapshot = uidCarTerms.copy(); uidCarTerms.clear()
                await process_uidCarTerms_batch(snapshot, snap_client)
            if all_ps_done.is_set():
                #TODO: run uidcarterms evenly across all semaphores if psqueries are small. Will be important once we implement live updates
                if uidCarTerms:
                    snapshot = uidCarTerms.copy(); uidCarTerms.clear()
                    await process_uidCarTerms_batch(snapshot, snap_client)
                break

    heartbeat = asyncio.create_task(monitor_progress())
    # Start insert workers
    worker_tasks = [asyncio.create_task(insert_worker()) for _ in range(INSERT_SEMAPHORE_LIMIT)]
    async with httpx.AsyncClient(timeout=60, follow_redirects=True, headers=snaplogic_person_config["headers"]) as snap_client:
        async with httpx.AsyncClient(timeout=60, follow_redirects=True, headers=ps_query_config["headers"]) as ps_client:
            monitor_task = asyncio.create_task(monitor_uidCarTerms(snap_client))
            await asyncio.gather(
                *(query_ps(b, ps_client) for b in buids), return_exceptions=True)
            all_ps_done.set()
            await monitor_task
            if running_snap:
                await asyncio.gather(*running_snap, return_exceptions=True)
            # Wait for queue to drain, then signal workers to stop
            await insert_queue.join()
            for _ in worker_tasks:
                await insert_queue.put(None)
            await asyncio.gather(*worker_tasks, return_exceptions=True)

    #TODO: Review this redundant logic (maybe replace with asyncio.TaskGroup())
    if running_snap:
        await asyncio.gather(*running_snap, return_exceptions=True)

    metrics["done"] = True
    await heartbeat

    await asyncpg_pool.close()

    elapsed = (datetime.now() - metrics["start_time"]).total_seconds()
    logger.info("\n───────────────────────────────────────────────")
    logger.info("PERSON_RAW_FLOW RUN SUMMARY")
    logger.info("───────────────────────────────────────────────")
    logger.info(f"BUIDs:                 {len(buids):,}")
    logger.info(f"PS Queries:            {metrics['ps_queried']:,} (success {metrics['ps_success']:,}, empty {metrics['ps_empty']:,})")
    logger.info(f"uidCarTerms:           {metrics['uidcarterm_total']:,}")
    logger.info(f"SnapLogic Batches:     {metrics['snaplogic_batches_completed']:,}/{metrics['snaplogic_batches_started'] or '?'}")
    logger.info(f"Persons Returned:      {metrics['persons_received']:,}")
    logger.info(f"Inserts:               {metrics['insert_success']:,} new | {metrics['insert_skipped']:,} skipped")
    logger.info(f"Errors:                PS={metrics['errors']['ps']} | Snap={metrics['errors']['snap']} | DB={metrics['errors']['db']}")
    logger.info(f"Duration:              {int(elapsed//3600)}h {int((elapsed%3600)//60)}m {int(elapsed%60)}s")
    logger.info("───────────────────────────────────────────────")

    return {
        "status": "success",
        "buids_processed": len(buids),
        "records_inserted": metrics["insert_success"],
        "errors": metrics["errors"]
    }


if __name__ == "__main__":
    import asyncio
    asyncio.run(person_raw_flow())
