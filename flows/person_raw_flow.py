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
    DEPersonApiResource,
    PsQueryResource,
    VDSApiResource,
    SAPApiResource,
    ps_url
)

logging.getLogger("httpx").setLevel(logging.WARNING)


"""
    Create a Prefect flow that retrieves person data from the Data Engineering Person API
    and prepares it for insertion into the Postgres database.

    steps:
    1. Extract BUIDs.
    2. For each BUID, query PeopleSoft to get uidCarTerm data.
    3. Batch uidCarTerm data and send to Data Engineering Person API to get person details.
    4. Insert person data in the Postgres database.
"""
@flow(
    name="person-raw-flow",
    description="Retrieves person data from Data Engineering Person API and inserts into Postgres database",
    retries=1,
    retry_delay_seconds=300,
    log_prints=True
)
async def person_raw_flow():
    """
    A Prefect flow that retrieves person data from the Data Engineering Person API
    and prepares it for insertion into the Postgres database.

    Steps:
    1. Extract BUIDs from PeopleSoft, VDS, and SAP APIs.
    2. For each BUID, query PeopleSoft to get uidCarTerm data.
    3. Batch uidCarTerm data and send to Data Engineering Person API to get person details.
    4. Insert person data in the Postgres database.
    """
    logger = get_run_logger()

    UIDCARTERM_BATCH_SIZE = 400
    BUID_BATCH_SIZE = 100
    PSQUERY_SEMAPHORE_LIMIT = 10 #10
    PERSON_API_SEMAPHORE_LIMIT = 5 #8
    INSERT_SEMAPHORE_LIMIT = 100 #25

    # Get resource configurations
    asyncpg_pool_config = AsyncpgPoolResource.get_pool_config()
    person_api_config = DEPersonApiResource.get_config()
    ps_query_config = PsQueryResource.get_config()
    vds_api_config = VDSApiResource.get_config()
    sap_api_config = SAPApiResource.get_config()

    asyncpg_pool = await asyncpg.create_pool(**asyncpg_pool_config)

    metrics = {
        "ps_queried": 0, "ps_success": 0, "ps_empty": 0,
        "students_unique": 0, "uidcarterm_total": 0, "uidcarterm_estimated": 0,
        "buids_only_count": 0, "buids_only_estimated": 0,
        "uidcarterm_batches_sent": 0, "uidcarterm_batches_completed": 0,
        "buid_batches_sent": 0, "buid_batches_completed": 0,
        "persons_received": 0, "insert_success": 0, "insert_skipped": 0,
        "errors": {"ps": 0, "person_api": 0, "db": 0},
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
    person_api_sem = asyncio.Semaphore(PERSON_API_SEMAPHORE_LIMIT)
    insert_sem = asyncio.Semaphore(INSERT_SEMAPHORE_LIMIT)

    uidCarTerms, buids_only, running_person_api = [], [], []
    uidCarTerms_threshold_event, buids_threshold_event, all_ps_done = asyncio.Event(), asyncio.Event(), asyncio.Event()
    # Set max queue size to prevent memory overflow when inserts are slower than Person API returns
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
                        # Track unique students (BUIDs with term data)
                        unique_buids_in_terms = set(term.get("CAMPUS_ID") for term in uidCarTerm if term.get("CAMPUS_ID"))
                        metrics["students_unique"] += len(unique_buids_in_terms)
                        uidCarTerms.append(uidCarTerm)
                        if len([item for row in uidCarTerms for item in row]) >= UIDCARTERM_BATCH_SIZE:
                            uidCarTerms_threshold_event.set()
                    else:
                        metrics["ps_empty"] += 1
                        metrics["buids_only_count"] += 1
                        buids_only.append(buid)
                        if len(buids_only) >= BUID_BATCH_SIZE:
                            buids_threshold_event.set()
                    break
                except Exception as e:
                    if attempt < max_retries:
                        await asyncio.sleep(base_delay * 3 ** (attempt - 1))
                    else:
                        metrics["errors"]["ps"] += 1
                        logger.error(f"PSQuery error for BUID {buid}: {e}")
                        return e

    async def query_person_api_and_insert(data_payload: dict, person_api_client: httpx.AsyncClient, max_retries: int = 5, base_delay: int = 10) -> Optional[int]:
        """
        Query Data Engineering Person API with a batch of buids or uidCarTerms, then enqueue insert_person tasks.

        Args:
            data_payload (dict): Dictionary containing the API request payload.
            person_api_client (httpx.AsyncClient): HTTP client for making the Person API request.
            max_retries (int, optional): Maximum number of retry attempts for API or network errors.
            base_delay (int, optional): Base delay in seconds between retries, exponentially increased.

        Returns:
            Optional[int]: Number of person records retrieved, or an exception if failed after retries.

        Raises:
            httpx.HTTPError: If the Person API request fails after all retries.
            json.JSONDecodeError: If the response is not a valid JSON.
        """
        async with person_api_sem:
            metrics["uidcarterm_batches_sent"] += 1 if "student" in data_payload else 0
            metrics["buid_batches_sent"] += 1 if "buids" in data_payload else 0
            for attempt in range(1, max_retries + 1):
                try:
                    #TODO: Person API takes about 5 minutes to finish. Timeout after 11 minutes. Log any buids that failed or insert them into queue to redo (live updates queue)
                    resp = await person_api_client.post(
                        person_api_config["url"],
                        json=data_payload,
                        timeout=10000
                    )
                    resp.raise_for_status()
                    response_obj = resp.json()
                    persons = response_obj.get("data", [])
                    persons = [p for p in persons if p.get("personid")] #TODO: Temp fix for glitch in Person API returning empty objects
                    metrics["persons_received"] += len(persons)
                    # Track completed batches by type
                    if "student" in data_payload:
                        metrics["uidcarterm_batches_completed"] += 1
                    elif "buids" in data_payload:
                        metrics["buid_batches_completed"] += 1

                    # Enqueue each person for single-record insertion
                    for p in persons:
                        await insert_queue.put(p)
                    return len(persons)
                except Exception as e:
                    if attempt < max_retries:
                        await asyncio.sleep(base_delay * 3 ** (attempt - 1))
                    else:
                        metrics["errors"]["person_api"] += 1
                        logger.error(f"Person API error: {e}\n{traceback.format_exc()}")
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

                # Safely remove sensitive fields if the parent objects exist and are dicts
                person_basic = p.get("personBasic")
                if person_basic and isinstance(person_basic, dict):
                    for k in ("ssn", "socialSecurityNumber", "sexualOrientation"):
                        person_basic.pop(k, None)
                
                student_info = p.get("studentInfo")
                if student_info and isinstance(student_info, dict):
                    for k in ("finAid", "finAidReceived"):
                        student_info.pop(k, None)

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
                
                # Calculate estimates and ETA
                total_batches_completed = metrics["uidcarterm_batches_completed"] + metrics["buid_batches_completed"]
                if total_batches_completed > 0:
                    # Estimate total batches needed
                    if ps_done > 0:
                        avg_terms_per_student = metrics["uidcarterm_total"] / metrics["ps_success"] if metrics["ps_success"] > 0 else 0
                        est_total_terms = int(avg_terms_per_student * metrics["ps_success"] + (total_buids - ps_done) * avg_terms_per_student)
                        est_term_batches = math.ceil(est_total_terms / UIDCARTERM_BATCH_SIZE)
                        est_buid_batches = math.ceil((total_buids - metrics["students_unique"]) / BUID_BATCH_SIZE)
                        est_batches_total = est_term_batches + est_buid_batches
                    else:
                        est_batches_total = 1
                    
                    effective_done_batches = max(total_batches_completed, 1)
                    remaining_batches = max(est_batches_total - total_batches_completed, 0)
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
                
                # Calculate queue size
                queue_size = insert_queue.qsize()
                
                # Calculate estimated totals
                if ps_done > 0:
                    avg_terms = metrics["uidcarterm_total"] / metrics["ps_success"] if metrics["ps_success"] > 0 else 0
                    metrics["uidcarterm_estimated"] = int(avg_terms * total_buids)
                    metrics["buids_only_estimated"] = total_buids - metrics["students_unique"] if ps_done >= total_buids else int((metrics["ps_empty"] / ps_done) * total_buids)
                    
                    # Calculate estimated batch counts for display
                    avg_terms_per_student = metrics["uidcarterm_total"] / metrics["ps_success"] if metrics["ps_success"] > 0 else 0
                    remaining_buids = total_buids - ps_done
                    frac_with_terms = metrics["ps_success"] / ps_done
                    est_remaining_terms = int(remaining_buids * frac_with_terms * avg_terms_per_student)
                    est_total_terms = metrics["uidcarterm_total"] + est_remaining_terms
                    est_term_batches = math.ceil(est_total_terms / UIDCARTERM_BATCH_SIZE) if est_total_terms > 0 else 0
                    est_total_buids_without_terms = int((metrics["ps_empty"] / ps_done) * total_buids)
                    est_buid_batches = math.ceil(est_total_buids_without_terms / BUID_BATCH_SIZE) if est_total_buids_without_terms > 0 else 0
                    est_batches_total = est_term_batches + est_buid_batches
                else:
                    est_term_batches = 0
                    est_buid_batches = 0
                    est_batches_total = 0
                
                # Format totals for batches
                total_batches_sent = metrics["uidcarterm_batches_sent"] + metrics["buid_batches_sent"]
                total_batches_completed = metrics["uidcarterm_batches_completed"] + metrics["buid_batches_completed"]
                ps_active = PSQUERY_SEMAPHORE_LIMIT - ps_sem._value
                person_api_active = PERSON_API_SEMAPHORE_LIMIT - person_api_sem._value
                insert_active = INSERT_SEMAPHORE_LIMIT - insert_sem._value
                logger.info(
                    f"╔═══════════════════════════════════════════════════════════════════╗"
                    f"\n║ HEARTBEAT [{elapsed_str}] — ETA: {eta_str}               "
                    f"\n╠═══════════════════════════════════════════════════════════════════╣"
                    f"\n║ DATA COLLECTION                                                   "
                    f"\n║   PS Queries:         {ps_done:>6,} / {total_buids:<6,} ({progress*100:>5.1f}%)              "
                    f"\n║     └─ With Terms:    {metrics['ps_success']:>6,} students → {metrics['uidcarterm_total']:>6,} term records   "
                    f"\n║     └─ Without Terms: {metrics['ps_empty']:>6,} people (BUID only)               "
                    f"\n╠═══════════════════════════════════════════════════════════════════╣"
                    f"\n║ API BATCHES (Person API)                                          "
                    f"\n║   Student Batches:    {metrics['uidcarterm_batches_completed']:>3} / {est_term_batches:<3} completed                "
                    f"\n║   BUID Batches:       {metrics['buid_batches_completed']:>3} / {est_buid_batches:<3} completed                "
                    f"\n║   Total:              {total_batches_completed:>3} / {est_batches_total:<3} completed                "
                    f"\n╠═══════════════════════════════════════════════════════════════════╣"
                    f"\n║ DATABASE OPERATIONS                                               "
                    f"\n║   Persons Received:   {metrics['persons_received']:>6,}                                   "
                    f"\n║   Insert Queue:       {queue_size:>6,} pending                              "
                    f"\n║   Inserted:           {metrics['insert_success']:>6,} records                            "
                    f"\n║   Skipped:            {metrics['insert_skipped']:>6,} records                            "
                    f"\n╠═══════════════════════════════════════════════════════════════════╣"
                    f"\n║ RESOURCE USAGE                                                    "
                    f"\n║   Semaphores Active:  PS={ps_active}/{PSQUERY_SEMAPHORE_LIMIT}  API={person_api_active}/{PERSON_API_SEMAPHORE_LIMIT}  Insert={insert_active}/{INSERT_SEMAPHORE_LIMIT}     "
                    f"\n║   Errors:             PS={metrics['errors']['ps']}  API={metrics['errors']['person_api']}  DB={metrics['errors']['db']}                  "
                    f"\n╚═══════════════════════════════════════════════════════════════════╝"
                )
                await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"⚠️ Heartbeat error: {e}")

    async def process_uidCarTerms_batch(batch: List[List[dict]], person_api_client: httpx.AsyncClient) -> None:
        """
        Process a single batch of at most UIDCARTERM_BATCH_SIZE uidCarTerm records
        by flattening and sending them to the Data Engineering Person API.

        Args:
            batch (List[List[dict]]): A list of uidCarTerm result sets.
            person_api_client (httpx.AsyncClient): HTTP client for Person API requests.

        Returns:
            None

        Raises:
            json.JSONDecodeError: If JSON serialization fails.
        """
        flattened = [item for row in batch for item in row]
        if not flattened:
            return
        # Convert to API format: lowercase keys and rename CAMPUS_ID to buid
        uid_car_term_data = [
            {("buid" if k=="CAMPUS_ID" else k.lower()): v for k, v in item.items() if k != "attr:rownumber"}
            for item in flattened
        ]
        payload = {
            "objects": ["student", "affiliate", "faculty", "employee"],
            "student": {"uid_car_term": uid_car_term_data}
        }
        task = asyncio.create_task(
            query_person_api_and_insert(payload, person_api_client)
        )
        running_person_api.append(task)

    async def process_buids_batch(batch: List[str], person_api_client: httpx.AsyncClient) -> None:
        """
        Process a single batch of BUIDs (without term data) and send to the Data Engineering Person API.

        Args:
            batch (List[str]): A list of BUIDs.
            person_api_client (httpx.AsyncClient): HTTP client for Person API requests.

        Returns:
            None
        """
        if not batch:
            return
        payload = {
            "buids": batch,
            "objects": ["student", "affiliate", "faculty", "employee"]
        }
        task = asyncio.create_task(
            query_person_api_and_insert(payload, person_api_client)
        )
        running_person_api.append(task)

    async def monitor_uidCarTerms(person_api_client: httpx.AsyncClient) -> None:
        """
        Monitor and submit uidCarTerm data batches to Data Engineering Person API as they become ready,
        waiting for either uidCarTerms_threshold_event or buids_threshold_event or the completion of PS queries.

        Args:
            person_api_client (httpx.AsyncClient): HTTP client for Person API requests.

        Returns:
            None
        """
        while True:
            done, _ = await asyncio.wait(
                [asyncio.create_task(uidCarTerms_threshold_event.wait()), asyncio.create_task(buids_threshold_event.wait()), asyncio.create_task(all_ps_done.wait())],
                return_when=asyncio.FIRST_COMPLETED)
            if uidCarTerms_threshold_event.is_set():
                uidCarTerms_threshold_event.clear()
                snapshot = uidCarTerms.copy(); uidCarTerms.clear()
                await process_uidCarTerms_batch(snapshot, person_api_client)
            if buids_threshold_event.is_set():
                buids_threshold_event.clear()
                snapshot = buids_only.copy(); buids_only.clear()
                await process_buids_batch(snapshot, person_api_client)
            if all_ps_done.is_set():
                #TODO: run uidcarterms evenly across all semaphores if psqueries are small. Will be important once we implement live updates
                if uidCarTerms:
                    snapshot = uidCarTerms.copy(); uidCarTerms.clear()
                    await process_uidCarTerms_batch(snapshot, person_api_client)
                if buids_only:
                    snapshot = buids_only.copy(); buids_only.clear()
                    await process_buids_batch(snapshot, person_api_client)
                break

    heartbeat = asyncio.create_task(monitor_progress())
    # Start insert workers
    worker_tasks = [asyncio.create_task(insert_worker()) for _ in range(INSERT_SEMAPHORE_LIMIT)]
    async with httpx.AsyncClient(timeout=60, follow_redirects=True, headers=person_api_config["headers"]) as person_api_client:
        async with httpx.AsyncClient(timeout=60, follow_redirects=True, headers=ps_query_config["headers"]) as ps_client:
            monitor_task = asyncio.create_task(monitor_uidCarTerms(person_api_client))
            await asyncio.gather(
                *(query_ps(b, ps_client) for b in buids), return_exceptions=True)
            all_ps_done.set()
            await monitor_task
            if running_person_api:
                await asyncio.gather(*running_person_api, return_exceptions=True)
            # Wait for queue to drain, then signal workers to stop
            await insert_queue.join()
            for _ in worker_tasks:
                await insert_queue.put(None)
            await asyncio.gather(*worker_tasks, return_exceptions=True)

    #TODO: Review this redundant logic (maybe replace with asyncio.TaskGroup())
    if running_person_api:
        await asyncio.gather(*running_person_api, return_exceptions=True)

    metrics["done"] = True
    await heartbeat

    await asyncpg_pool.close()

    elapsed = (datetime.now() - metrics["start_time"]).total_seconds()
    total_batches_sent = metrics["uidcarterm_batches_sent"] + metrics["buid_batches_sent"]
    total_batches_completed = metrics["uidcarterm_batches_completed"] + metrics["buid_batches_completed"]
    
    logger.info("\n╔═══════════════════════════════════════════════════════╗")
    logger.info("║           PERSON_RAW_FLOW RUN SUMMARY                ║")
    logger.info("╠═══════════════════════════════════════════════════════╣")
    logger.info(f"║ Total BUIDs Processed:     {len(buids):>6,}                     ║")
    logger.info(f"║   ├─ Students (w/ terms):  {metrics['students_unique']:>6,}                     ║")
    logger.info(f"║   │    └─ Term records:    {metrics['uidcarterm_total']:>6,}                     ║")
    logger.info(f"║   └─ BUIDs only:           {metrics['buids_only_count']:>6,}                     ║")
    logger.info("╠═══════════════════════════════════════════════════════╣")
    logger.info(f"║ API Batches:               {total_batches_completed:>3} / {total_batches_sent:<3} completed         ║")
    logger.info(f"║   ├─ Student batches:      {metrics['uidcarterm_batches_completed']:>3} / {metrics['uidcarterm_batches_sent']:<3} completed         ║")
    logger.info(f"║   └─ BUID batches:         {metrics['buid_batches_completed']:>3} / {metrics['buid_batches_sent']:<3} completed         ║")
    logger.info("╠═══════════════════════════════════════════════════════╣")
    logger.info(f"║ Persons Returned:          {metrics['persons_received']:>6,}                     ║")
    logger.info(f"║ Records Inserted:          {metrics['insert_success']:>6,}                     ║")
    logger.info(f"║ Records Skipped:           {metrics['insert_skipped']:>6,}                     ║")
    logger.info("╠═══════════════════════════════════════════════════════╣")
    logger.info(f"║ Errors: PS={metrics['errors']['ps']}  API={metrics['errors']['person_api']}  DB={metrics['errors']['db']}                        ║")
    logger.info(f"║ Duration: {int(elapsed//3600)}h {int((elapsed%3600)//60)}m {int(elapsed%60)}s                              ║")
    logger.info("╚═══════════════════════════════════════════════════════╝")

    return {
        "status": "success",
        "buids_processed": len(buids),
        "records_inserted": metrics["insert_success"],
        "errors": metrics["errors"]
    }


if __name__ == "__main__":
    import asyncio
    asyncio.run(person_raw_flow())
