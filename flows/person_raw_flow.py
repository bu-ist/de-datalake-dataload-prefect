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
from prefect import flow, task
from prefect.cache_policies import NONE as NO_CACHE
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


@task(name="fetch-buids-from-peoplesoft", retries=2, retry_delay_seconds=30, tags=["fetch-buids"])
async def fetch_buids_from_peoplesoft_task(ps_query_config: dict) -> List[str]:
    """
    Fetch BUIDs from PeopleSoft BU_PARM_0216_QRY query.
    
    Args:
        ps_query_config (dict): PeopleSoft query configuration.
        
    Returns:
        List[str]: List of BUIDs retrieved from PeopleSoft.
    """
    logger = get_run_logger()
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            ps_url(ps_query_config["csEnv"], "BU_PARM_0216_QRY"),
            params={"isconnectedquery": "N", "maxrows": 0, "json_resp": "true"},
            headers=ps_query_config["headers"],
            timeout=30,
        )
        resp.raise_for_status()
        rows = resp.json().get("data", {}).get("query", {}).get("rows", [])
    buids = [row.get("CAMPUS_ID") for row in rows if row.get("CAMPUS_ID")]
    logger.info(f"Retrieved {len(buids)} BUIDs from BU_PARM_0216_QRY.")
    return buids


@task(name="fetch-buids-from-sap", retries=2, retry_delay_seconds=30, tags=["fetch-buids"])
async def fetch_buids_from_sap_task(sap_api_config: dict) -> List[str]:
    """
    Fetch BUIDs from SAP Z_HR_EMPLOYEE_OBJ_LIST API.
    
    Args:
        sap_api_config (dict): SAP API configuration.
        
    Returns:
        List[str]: List of active employee BUIDs from SAP.
    """
    logger = get_run_logger()
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
    buids = [row.get("BUID") for row in rows if row.get("EMP_STATUS") == "3 - Active" and row.get("BUID")]
    logger.info(f"Retrieved {len(buids)} BUIDs from SAP Z_HR_EMPLOYEE_OBJ_LIST.")
    return buids


async def query_ps_single(
    buid: str,
    ps_client: httpx.AsyncClient,
    ps_query_config: dict,
    ps_sem: asyncio.Semaphore,
    metrics: dict,
    uidCarTerms: list,
    buids_only: list,
    uidCarTerms_threshold_event: asyncio.Event,
    buids_threshold_event: asyncio.Event,
    UIDCARTERM_BATCH_SIZE: int,
    BUID_BATCH_SIZE: int,
    logger
) -> Optional[Exception]:
    """
    Query the PeopleSoft API for all uidCarTerm data associated with a given BUID.

    Args:
        buid (str): The unique BUID to fetch data for.
        ps_client (httpx.AsyncClient): HTTP client for making the API request.
        ps_query_config (dict): PeopleSoft query configuration.
        ps_sem (asyncio.Semaphore): Semaphore to limit concurrent PS queries.
        metrics (dict): Shared metrics dictionary.
        uidCarTerms (list): Shared list to append term data.
        buids_only (list): Shared list to append BUIDs without term data.
        uidCarTerms_threshold_event (asyncio.Event): Event to signal batch threshold.
        buids_threshold_event (asyncio.Event): Event to signal BUID batch threshold.
        UIDCARTERM_BATCH_SIZE (int): Batch size for uidCarTerms.
        BUID_BATCH_SIZE (int): Batch size for BUIDs only.
        logger: Logger instance.

    Returns:
        Optional[Exception]: Returns an exception if all retries fail, else None.
    """
    async with ps_sem:
        metrics["ps_queried"] += 1
        for attempt in range(1, 6):  # 5 retries
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
                return None
            except Exception as e:
                if attempt < 5:
                    await asyncio.sleep(10 * 3 ** (attempt - 1))
                else:
                    metrics["errors"]["ps"] += 1
                    logger.error(f"PSQuery error for BUID {buid}: {e}")
                    return e


@task(name="query-all-buids-from-peoplesoft", retries=1, retry_delay_seconds=30, cache_policy=NO_CACHE, tags=["query-buid-terms"])
async def query_all_buids_task(
    buids: List[str],
    ps_client: httpx.AsyncClient,
    ps_query_config: dict,
    ps_sem: asyncio.Semaphore,
    metrics: dict,
    uidCarTerms: list,
    buids_only: list,
    uidCarTerms_threshold_event: asyncio.Event,
    buids_threshold_event: asyncio.Event,
    UIDCARTERM_BATCH_SIZE: int,
    BUID_BATCH_SIZE: int
) -> None:
    """
    Query PeopleSoft for all BUIDs concurrently, respecting semaphore limits.

    Args:
        buids (List[str]): List of all BUIDs to query.
        ps_client (httpx.AsyncClient): HTTP client for making the API request.
        ps_query_config (dict): PeopleSoft query configuration.
        ps_sem (asyncio.Semaphore): Semaphore to limit concurrent PS queries.
        metrics (dict): Shared metrics dictionary.
        uidCarTerms (list): Shared list to append term data.
        buids_only (list): Shared list to append BUIDs without term data.
        uidCarTerms_threshold_event (asyncio.Event): Event to signal batch threshold.
        buids_threshold_event (asyncio.Event): Event to signal BUID batch threshold.
        UIDCARTERM_BATCH_SIZE (int): Batch size for uidCarTerms.
        BUID_BATCH_SIZE (int): Batch size for BUIDs only.
    """
    logger = get_run_logger()
    logger.info(f"Querying PeopleSoft for {len(buids)} BUIDs...")
    await asyncio.gather(
        *(query_ps_single(
            buid=buid,
            ps_client=ps_client,
            ps_query_config=ps_query_config,
            ps_sem=ps_sem,
            metrics=metrics,
            uidCarTerms=uidCarTerms,
            buids_only=buids_only,
            uidCarTerms_threshold_event=uidCarTerms_threshold_event,
            buids_threshold_event=buids_threshold_event,
            UIDCARTERM_BATCH_SIZE=UIDCARTERM_BATCH_SIZE,
            BUID_BATCH_SIZE=BUID_BATCH_SIZE,
            logger=logger
        ) for buid in buids),
        return_exceptions=True
    )
    logger.info(f"Completed querying {len(buids)} BUIDs from PeopleSoft")


@task(
    name="process-uidcarterms-batch",
    retries=5,
    retry_delay_seconds=10,
    task_run_name="uidCarTerms-batch-{batch_id}",
    cache_policy=NO_CACHE,
    tags=["get-person-batch"]
)
async def process_uidCarTerms_batch_task(
    batch: List[List[dict]],
    batch_id: int,
    person_api_client: httpx.AsyncClient,
    person_api_config: dict,
    person_api_sem: asyncio.Semaphore,
    metrics: dict
) -> List[dict]:
    """
    Process a single batch of uidCarTerm records by sending to Person API.

    Args:
        batch (List[List[dict]]): A list of uidCarTerm result sets.
        batch_id (int): Unique identifier for this batch.
        person_api_client (httpx.AsyncClient): HTTP client for Person API requests.
        person_api_config (dict): Person API configuration.
        person_api_sem (asyncio.Semaphore): Semaphore to limit concurrent API calls.
        metrics (dict): Shared metrics dictionary.

    Returns:
        List[dict]: List of person records retrieved from API.
    """
    logger = get_run_logger()
    flattened = [item for row in batch for item in row]
    if not flattened:
        logger.info("📦 Empty batch - skipping")
        return []
    
    # Extract unique BUIDs and count terms
    unique_buids = set(item.get("CAMPUS_ID") for item in flattened if item.get("CAMPUS_ID"))
    num_terms = len(flattened)
    num_students = len(unique_buids)
    
    logger.info(f"📦 Processing batch {batch_id}: {num_students} students, {num_terms} term records")
    
    # Convert to API format: lowercase keys and rename CAMPUS_ID to buid
    uid_car_term_data = [
        {("buid" if k=="CAMPUS_ID" else k.lower()): v for k, v in item.items() if k != "attr:rownumber"}
        for item in flattened
    ]
    payload = {
        "objects": ["student", "affiliate", "faculty", "employee"],
        "student": {"uid_car_term": uid_car_term_data}
    }
    
    async with person_api_sem:
        metrics["uidcarterm_batches_sent"] += 1
        #TODO: Person API takes about 5 minutes to finish. Timeout after 11 minutes. Log any buids that failed or insert them into queue to redo (live updates queue)
        resp = await person_api_client.post(
            person_api_config["url"],
            json=payload,
            timeout=10000
        )
        resp.raise_for_status()
        response_obj = resp.json()
        persons = response_obj.get("data", [])
        persons = [p for p in persons if p.get("personid")]
        metrics["persons_received"] += len(persons)
        metrics["uidcarterm_batches_completed"] += 1
        
        logger.info(f"✅ Batch {batch_id} complete: Retrieved {len(persons)} person records from {num_students} students with {num_terms} terms")
        return persons


@task(
    name="process-buids-batch",
    retries=5,
    retry_delay_seconds=10,
    task_run_name="buids-batch-{batch_id}",
    cache_policy=NO_CACHE,
    tags=["get-person-batch"]
)
async def process_buids_batch_task(
    batch: List[str],
    batch_id: int,
    person_api_client: httpx.AsyncClient,
    person_api_config: dict,
    person_api_sem: asyncio.Semaphore,
    metrics: dict
) -> List[dict]:
    """
    Process a single batch of BUIDs (without term data) by sending to Person API.

    Args:
        batch (List[str]): A list of BUIDs.
        batch_id (int): Unique identifier for this batch.
        person_api_client (httpx.AsyncClient): HTTP client for Person API requests.
        person_api_config (dict): Person API configuration.
        person_api_sem (asyncio.Semaphore): Semaphore to limit concurrent API calls.
        metrics (dict): Shared metrics dictionary.

    Returns:
        List[dict]: List of person records retrieved from API.
    """
    logger = get_run_logger()
    if not batch:
        logger.info("📦 Empty batch - skipping")
        return []
    
    num_buids = len(batch)
    logger.info(f"📦 Processing batch {batch_id}: {num_buids} BUIDs (no term data)")
    
    payload = {
        "buids": batch,
        "objects": ["student", "affiliate", "faculty", "employee"]
    }
    
    async with person_api_sem:
        metrics["buid_batches_sent"] += 1
        #TODO: Person API takes about 5 minutes to finish. Timeout after 11 minutes. Log any buids that failed or insert them into queue to redo (live updates queue)
        resp = await person_api_client.post(
            person_api_config["url"],
            json=payload,
            timeout=10000
        )
        resp.raise_for_status()
        response_obj = resp.json()
        persons = response_obj.get("data", [])
        persons = [p for p in persons if p.get("personid")]
        metrics["persons_received"] += len(persons)
        metrics["buid_batches_completed"] += 1
        
        logger.info(f"✅ Batch {batch_id} complete: Retrieved {len(persons)} person records from {num_buids} BUIDs")
        return persons


@task(
    name="insert-persons-batch",
    retries=3,
    retry_delay_seconds=10,
    task_run_name="insert-{batch_type}-batch-{batch_id}",
    cache_policy=NO_CACHE,
    tags=["insert-persons"]
)
async def insert_persons_batch_task(
    persons: List[dict],
    batch_id: int,
    batch_type: str,
    asyncpg_pool,
    insert_sem: asyncio.Semaphore,
    metrics: dict
) -> dict:
    """
    Insert a batch of person records into the database.

    Args:
        persons (List[dict]): List of person records to insert.
        batch_id (int): Unique identifier for this batch.
        batch_type (str): Type of batch ("uidcarterms" or "buids").
        asyncpg_pool: Database connection pool.
        insert_sem (asyncio.Semaphore): Semaphore to limit concurrent inserts.
        metrics (dict): Shared metrics dictionary.

    Returns:
        dict: Summary with inserted count, skipped count, and error count.
    """
    logger = get_run_logger()
    inserted_count = 0
    skipped_count = 0
    error_count = 0
    
    num_persons = len(persons)
    logger.info(f"💾 Starting insert for batch {batch_id} ({batch_type}): {num_persons} person records")
    
    for p in persons:
        uid = p.get("personid")
        if not uid:
            skipped_count += 1
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

        try:
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
            inserted_count += 1
        except Exception as e:
            metrics["errors"]["db"] += 1
            error_count += 1
            logger.error(f"❌ Insert failed for person {uid}: {e}")
    
    metrics["insert_success"] += inserted_count
    metrics["insert_skipped"] += skipped_count
    
    # Log summary for this insert batch
    if error_count > 0:
        logger.warning(f"⚠️ Insert batch {batch_id} ({batch_type}): {inserted_count} inserted, {skipped_count} skipped, {error_count} errors")
    else:
        logger.info(f"✅ Insert batch {batch_id} ({batch_type}) complete: {inserted_count} inserted, {skipped_count} skipped")
    
    return {
        "batch_id": batch_id,
        "batch_type": batch_type,
        "inserted": inserted_count,
        "skipped": skipped_count,
        "errors": error_count,
        "total_attempted": num_persons
    }


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

    UIDCARTERM_BATCH_SIZE = 6000
    BUID_BATCH_SIZE = 1000
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
        ps_buids_task = await fetch_buids_from_peoplesoft_task(ps_query_config)
        buids.extend(ps_buids_task)
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
        sap_buids_task = await fetch_buids_from_sap_task(sap_api_config)
        buids.extend(sap_buids_task)
    except Exception as e:
        logger.error(f"Failed to fetch BUIDs from SAP: {e}")
        raise

    buids = list(set(buids))  # Deduplicate BUIDs
    logger.info(f"Total unique BUIDs to process: {len(buids)}")

    #TODO: Any failed BUIDs will go into the person_live_update queue for reprocessing

    ps_sem = asyncio.Semaphore(PSQUERY_SEMAPHORE_LIMIT)
    person_api_sem = asyncio.Semaphore(PERSON_API_SEMAPHORE_LIMIT)
    insert_sem = asyncio.Semaphore(INSERT_SEMAPHORE_LIMIT)

    uidCarTerms, buids_only = [], []
    uidCarTerms_threshold_event, buids_threshold_event, all_ps_done = asyncio.Event(), asyncio.Event(), asyncio.Event()
    # Queue for batches waiting to be processed by Person API
    person_api_batch_queue: asyncio.Queue = asyncio.Queue()
    
    # Batch counter for UI identification
    batch_counter = {"uidcarterms": 0, "buids": 0}
    
    # Track all insert tasks so we can wait for them at the end
    insert_tasks = []
    insert_tasks_lock = asyncio.Lock()

    async def person_api_worker(person_api_client: httpx.AsyncClient) -> None:
        """
        Worker that processes batches from the queue, one at a time.
        Only PERSON_API_SEMAPHORE_LIMIT workers run concurrently.
        Insert tasks are created but not awaited, allowing the worker to process
        the next batch while inserts run concurrently (up to INSERT_SEMAPHORE_LIMIT).
        """
        while True:
            batch_item = await person_api_batch_queue.get()
            if batch_item is None:
                person_api_batch_queue.task_done()
                break
            
            batch_type, batch_data, batch_id = batch_item
            try:
                persons = []
                if batch_type == "uidcarterms":
                    persons = await process_uidCarTerms_batch_task(
                        batch=batch_data,
                        batch_id=batch_id,
                        person_api_client=person_api_client,
                        person_api_config=person_api_config,
                        person_api_sem=person_api_sem,
                        metrics=metrics
                    )
                elif batch_type == "buids":
                    persons = await process_buids_batch_task(
                        batch=batch_data,
                        batch_id=batch_id,
                        person_api_client=person_api_client,
                        person_api_config=person_api_config,
                        person_api_sem=person_api_sem,
                        metrics=metrics
                    )
                
                # Create insert task but DON'T await it - this allows the worker
                # to immediately process the next batch while inserts run concurrently
                if persons:
                    # Add small delay to help Prefect UI show tasks in sequential lanes
                    await asyncio.sleep(0.05)
                    insert_task = asyncio.create_task(
                        insert_persons_batch_task(
                            persons=persons,
                            batch_id=batch_id,
                            batch_type=batch_type,
                            asyncpg_pool=asyncpg_pool,
                            insert_sem=insert_sem,
                            metrics=metrics
                        )
                    )
                    async with insert_tasks_lock:
                        insert_tasks.append(insert_task)
            except Exception as e:
                logger.error(f"Worker error processing {batch_type} batch {batch_id}: {e}")
                metrics["errors"]["person_api"] += 1
            finally:
                person_api_batch_queue.task_done()

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
                
                # Calculate batch queue size
                batch_queue_size = person_api_batch_queue.qsize()
                
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
                    f"\n║   Batch Queue:        {batch_queue_size:>3} pending                             "
                    f"\n╠═══════════════════════════════════════════════════════════════════╣"
                    f"\n║ DATABASE OPERATIONS                                               "
                    f"\n║   Persons Received:   {metrics['persons_received']:>6,}                                   "
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
                if snapshot:
                    batch_counter["uidcarterms"] += 1
                    await person_api_batch_queue.put(("uidcarterms", snapshot, batch_counter["uidcarterms"]))
            if buids_threshold_event.is_set():
                buids_threshold_event.clear()
                snapshot = buids_only.copy(); buids_only.clear()
                if snapshot:
                    batch_counter["buids"] += 1
                    await person_api_batch_queue.put(("buids", snapshot, batch_counter["buids"]))
            if all_ps_done.is_set():
                #TODO: run uidcarterms evenly across all semaphores if psqueries are small. Will be important once we implement live updates
                if uidCarTerms:
                    snapshot = uidCarTerms.copy(); uidCarTerms.clear()
                    batch_counter["uidcarterms"] += 1
                    await person_api_batch_queue.put(("uidcarterms", snapshot, batch_counter["uidcarterms"]))
                if buids_only:
                    snapshot = buids_only.copy(); buids_only.clear()
                    batch_counter["buids"] += 1
                    await person_api_batch_queue.put(("buids", snapshot, batch_counter["buids"]))
                break

    heartbeat = asyncio.create_task(monitor_progress())
    
    async with httpx.AsyncClient(timeout=60, follow_redirects=True, headers=person_api_config["headers"]) as person_api_client:
        # Start Person API workers (only PERSON_API_SEMAPHORE_LIMIT will run concurrently)
        person_api_workers = [asyncio.create_task(person_api_worker(person_api_client)) for _ in range(PERSON_API_SEMAPHORE_LIMIT)]
        
        async with httpx.AsyncClient(timeout=60, follow_redirects=True, headers=ps_query_config["headers"]) as ps_client:
            monitor_task = asyncio.create_task(monitor_uidCarTerms(person_api_client))
            # Query all BUIDs from PeopleSoft as a single task
            await query_all_buids_task(
                buids=buids,
                ps_client=ps_client,
                ps_query_config=ps_query_config,
                ps_sem=ps_sem,
                metrics=metrics,
                uidCarTerms=uidCarTerms,
                buids_only=buids_only,
                uidCarTerms_threshold_event=uidCarTerms_threshold_event,
                buids_threshold_event=buids_threshold_event,
                UIDCARTERM_BATCH_SIZE=UIDCARTERM_BATCH_SIZE,
                BUID_BATCH_SIZE=BUID_BATCH_SIZE
            )
            all_ps_done.set()
            await monitor_task
            
            # Wait for all Person API batches to be processed
            await person_api_batch_queue.join()
            
            # Signal Person API workers to stop
            for _ in person_api_workers:
                await person_api_batch_queue.put(None)
            await asyncio.gather(*person_api_workers, return_exceptions=True)
            
            # Now wait for all insert tasks to complete
            logger.info(f"Waiting for {len(insert_tasks)} insert tasks to complete...")
            await asyncio.gather(*insert_tasks, return_exceptions=True)
            logger.info("All insert tasks completed.")

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
