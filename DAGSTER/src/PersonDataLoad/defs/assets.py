import dagster as dg
import asyncio, json, httpx, math
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy import text
from dagster import AssetKey
from typing import Optional, List, Dict
import traceback
from zoneinfo import ZoneInfo
import logging
from deepdiff import DeepDiff
import asyncpg

logging.getLogger("httpx").setLevel(logging.WARNING)

ps_url = lambda env,qry: f"https://cs{env}.bu.edu/PSIGW/RESTListeningConnector/PSFT_CS/ExecuteQuery.v1/PUBLIC/{qry}/JSON/NONFILE"

"""
    A Dagster asset that retrieves terms from BU_TERM_QRY
    and prepares it for insertion into the Postgres database.
"""
def term_raw_op() -> dg.OpDefinition:
    @dg.op(
        name="term_raw_op",
        required_resource_keys={"postgres", "ps_query"},
        tags={"kind": "sql", "source": "PeopleSoft"},
    )
    async def op(context: dg.OpExecutionContext):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    ps_url(context.resources.ps_query["csEnv"], "BU_TERM_QRY"),
                    params={"isconnectedquery": "N", "maxrows": 0, "json_resp": "true"},
                    headers=context.resources.ps_query["headers"],
                    timeout=30,
                )
                resp.raise_for_status()
                rows = resp.json().get("data", {}).get("query", {}).get("rows", [])
            context.log.info(f"Retrieved {len(rows)} term rows.")
        except Exception as e:
            context.log.error(f"Failed BU Term Query HTTP request: {e}")
            return
        records = [
            {
                "acad_career": t.get("ACAD_CAREER", ""),
                "strm": t.get("STRM", ""),
                "term_data": json.dumps(t),
            }
            for t in rows
        ]
        session_factory = async_sessionmaker(context.resources.postgres)
        async with session_factory() as session, session.begin():
            try:
                await session.execute(text("TRUNCATE term_raw.term_data;"))

                await session.execute(
                    text("""
                        INSERT INTO term_raw.term_data (acad_career, strm, term_data)
                        VALUES (:acad_career, :strm, :term_data)
                    """),
                    records,
                )

                context.log.info(f"Inserted {len(records)} JSONB rows into term_raw.term_data.")
            except Exception as e:
                context.log.error(f"JSONB insert failed: {e}")
                return
            try:
                await session.execute(text("SELECT term_raw.refresh_current_term_data();"))
                context.log.info("Refreshed current_term_data from JSONB.")
            except Exception as e:
                context.log.error(f"Failed to refresh current term data: {e}")
                return

    return op




"""
    Create a Dagster asset that retrieves course data from the SnapLogic API
    and prepares it for insertion into the Postgres database.

    Steps:
    1. Extract relevant term codes using the BU Term Query resource.
    2. For each term code, call the SnapLogic Course API to get course details.
    3. Flatten the course data and log the number of records retrieved.
    4. Prepare the course data for insertion into the Postgres database.
"""
def course_raw_op() -> dg.OpDefinition:
    @dg.op(
        name="course_raw_op",
        required_resource_keys={"postgres", "snaplogic_course_api", "asyncpg_pool"},
        tags={"kind": "sql", "source": "PeopleSoft,snaplogic"},
    )
    async def op(context: dg.OpExecutionContext):
        INSERT_BATCH_SIZE = 50
        INSERT_SEMAPHORE_LIMIT = 4
        session_factory = async_sessionmaker(context.resources.postgres, expire_on_commit=False)
        asyncpg_pool = await asyncpg.create_pool(**context.resources.asyncpg_pool)

        terms = []

        # Retrieve STRM codes for the current term, its adjacent terms, and a conditional fourth term based on whether the current or previous term is the Summer
        async with session_factory() as session:
            async with session.begin():
                terms = (await session.execute(
                    text(f"SELECT strm FROM term_curated.term_data_by_service WHERE service='active_terms'"),
                )).scalars().all()

        context.log.info(f"Fetching course details for terms: {terms}")

        #TODO: Call Get Course Offerings with term first, and merge with course details here

        # Get course details for each term asynchronously
        async def get_course_details(term: str) -> List[str]:
            async with httpx.AsyncClient() as client:
                try:
                    resp = await client.get(
                        context.resources.snaplogic_course_api["url"],
                        params={"term": term, "csEnv": context.resources.snaplogic_course_api["cs_env"]},
                        headers=context.resources.snaplogic_course_api["headers"],
                        timeout=36000,
                    )
                    resp.raise_for_status()
                    return resp.json()
                except Exception as e:
                    context.log.error(f"Course request failed for term {term}: {e}")
                    return []
        courses_list = await asyncio.gather(*[get_course_details(term) for term in terms])

        courses = []
        for term_group in courses_list:
            for term in term_group:
                term_details = term.get("termDetails", {})
                academic_career = term_details.get("academicCareer", "")
                term_code = term_details.get("term", {}).get("code", "")
                for course in term.get("courses", []):
                    courses.append({
                        "academic_career": academic_career,
                        "term_code": term_code,
                        "course_id": course.get("v2", {}).get("courseId", ""),
                        "session_code": course.get("v2", {}).get("sessionCode", ""),
                        "course_data": json.dumps({**course, "termDetails": term_details}),
                    })

        insert_sem = asyncio.Semaphore(INSERT_SEMAPHORE_LIMIT)
        metrics = {"insert_success": 0, "errors": 0, "type_skipped": 0}

        async def batch_insert(batch, max_retries: int = 3, base_delay: float = 2.0):
            async with insert_sem:
                records = []
                for r in batch:
                    try:
                        term_code_int = int(r["term_code"]) if r.get("term_code") not in (None, "") else None
                        course_id_int = int(r["course_id"]) if r.get("course_id") not in (None, "") else None
                        if term_code_int is None or course_id_int is None:
                            raise ValueError("Missing numeric term_code or course_id")
                        records.append((
                            r["academic_career"],
                            term_code_int,
                            course_id_int,
                            r["session_code"],
                            r["course_data"],
                        ))
                    except Exception as e:
                        metrics["type_skipped"] += 1
                        context.log.warning(f"Skipping record due to type conversion issue: {e} | r={ {k: r.get(k) for k in ('academic_career','term_code','course_id','session_code')} }")
                query = (
                    """
                    INSERT INTO course_raw.course_data
                    (academic_career, term_code, course_id, session_code, course_data)
                    VALUES ($1, $2, $3, $4, $5::jsonb)
                    """
                )
                for attempt in range(1, max_retries + 1):
                    try:
                        async with asyncpg_pool.acquire() as conn:
                            await conn.executemany(query, records)
                        metrics["insert_success"] += len(batch)
                        return
                    except Exception as e:
                        if attempt < max_retries:
                            delay = base_delay * (2 ** (attempt - 1))
                            context.log.warning(f"Batch insert retry {attempt}/{max_retries} after error: {e}. Waiting {delay:.1f}s")
                            await asyncio.sleep(delay)
                        else:
                            metrics["errors"] += len(batch)
                            context.log.error(f"Batch insert failed after retries: {e}")


        tasks = []
        for i in range(0, len(courses), INSERT_BATCH_SIZE):
            batch = courses[i:i + INSERT_BATCH_SIZE]
            tasks.append(asyncio.create_task(batch_insert(batch)))

        await asyncio.gather(*tasks)

        await asyncpg_pool.close()

        if metrics["errors"] == 0:
            context.log.info(f"{metrics['insert_success']} records inserted successfully.")
        else:
            context.log.warning(f"Inserted {metrics['insert_success']}/{len(courses)} records. Errors: {metrics['errors']}.")

    return op

"""
    Create a Dagster asset that retrieves person data from the SnapLogic API
    and prepares it for insertion into the Postgres database.

    steps:
    1. Extract BUIDs.
    2. For each BUID, query PeopleSoft to get uidCarTerm data.
    3. Batch uidCarTerm data and send to SnapLogic Person API to get person details.
    4. Insert person data in the Postgres database.
"""
def person_raw_op() -> dg.OpDefinition:
    
    @dg.op(
        name="person_raw_op",
        required_resource_keys={"asyncpg_pool", "snaplogic_person_api", "ps_query", "vds_api", "sap_api"},
        tags={"kind": "sql", "source": "PeopleSoft,snaplogic"},
    )
    async def op(context: dg.OpExecutionContext):

        UIDCARTERM_GROUP_SIZE = 1000 #1900
        PSQUERY_SEMAPHORE_LIMIT = 10 #10
        SNAPLOGIC_SEMAPHORE_LIMIT = 8 #8
        INSERT_SEMAPHORE_LIMIT = 100 #25

        asyncpg_pool = await asyncpg.create_pool(**context.resources.asyncpg_pool)

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
                    ps_url(context.resources.ps_query["csEnv"], "BU_PARM_0216_QRY"),
                    params={"isconnectedquery": "N", "maxrows": 0, "json_resp": "true"},
                    headers=context.resources.ps_query["headers"],
                    timeout=30,
                )
                resp.raise_for_status()
                rows = resp.json().get("data", {}).get("query", {}).get("rows", [])
            buids.extend([row.get("CAMPUS_ID") for row in rows if row.get("CAMPUS_ID")])
            context.log.info(f"Retrieved {len(buids)} BUIDs from BU_PARM_0216_QRY.")
        except Exception as e:
            context.log.error(f"Failed to fetch BUIDs from PeopleSoft BU_PARM_0216_QRY: {e}")
            return

        # TODO: Fetch ENS population Too, or call EVERY Population.

        # Fetch BUIDs from VDS API
        # TODO: Re-enable VDS BUID fetch after new credentials are set up
        # try:
        #     async with httpx.AsyncClient() as client:
        #         resp = await client.get(
        #             context.resources.vds_api["url"],
        #             params={"sizeLimit": "25000", "filter": "(%26(AffiliateAssignmentEndDate=*)(%26(!(PersonPrimaryAffiliation=staff))(!(PersonPrimaryAffiliation=faculty))))"},
        #             headers=context.resources.vds_api["headers"],
        #             timeout=30,
        #         )
        #         resp.raise_for_status()
        #         rows = resp.json().get("entity", {}).get("resources", [])
        #     newBuids = [row.get("attributes").get("buid") for row in rows if row.get("attributes")]
        #     buids.extend(newBuids)
        #     context.log.info(f"Retrieved {len(newBuids)} BUIDs from VDS.")
        # except Exception as e:
        #     context.log.error(f"Failed to fetch BUIDs from VDS: {e}")
        #     return

        # Fetch BUIDs from SAP API
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    context.resources.sap_api["url"],
                    params={"BAPIName": "Z_HR_EMPLOYEE_OBJ_LIST", "account": "HR"},
                    json={},
                    headers=context.resources.sap_api["headers"],
                    timeout=30,
                )
                resp.raise_for_status()
                rows = resp.json().get("ET_EMP_LIST", [])
            newBuids = [row.get("BUID") for row in rows if row.get("EMP_STATUS") == "3 - Active" and row.get("BUID")]
            buids.extend(newBuids)
            context.log.info(f"Retrieved {len(newBuids)} BUIDs from SAP Z_HR_EMPLOYEE_OBJ_LIST.")
        except Exception as e:
            context.log.error(f"Failed to fetch BUIDs from SAP: {e}")
            return
        
        buids = list(set(buids))  # Deduplicate BUIDs
        context.log.info(f"Total unique BUIDs to process: {len(buids)}")

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
                        resp = await ps_client.get(ps_url(context.resources.ps_query["csEnv"], "BU_TERM_STD_FULL_TERM"), params=req, timeout=30)
                        resp.raise_for_status()
                        uidCarTerm = resp.json()['data']['query']['rows']

                        #TODO: Add logic for Faculty Terms. Consider if buid is a student and faculty for Terms
                        # req = {"isconnectedquery": "N", "maxrows": 0, "prompt_uniquepromptname": "EMPLID", "prompt_fieldvalue": buid, "json_resp": "true"}
                        # resp = await ps_client.get(ps_url(context.resources.ps_query["csEnv"], "BU_FACULTY_GET"), params=req, timeout=30)
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
                            context.log.error(f"PSQuery error for BUID {buid}: {e}")
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
                            context.resources.snaplogic_person_api["url"],
                            json={"uidCarTerm": str(uidCarTerms)},
                            params={"objects": "['student','affiliate','faculty','employee']", "csEnv": context.resources.snaplogic_person_api["cs_env"]},
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
                            context.log.error(f"SnapLogic error: {e}\n{traceback.format_exc()}")
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
                    context.log.error("Single insert failed", exc_info=True)
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
                    context.log.info(
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
                context.log.error(f"⚠️ Heartbeat error: {e}")


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
        async with httpx.AsyncClient(timeout=60, follow_redirects=True, headers=context.resources.snaplogic_person_api["headers"]) as snap_client:
            async with httpx.AsyncClient(timeout=60, follow_redirects=True, headers=context.resources.ps_query["headers"]) as ps_client:
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
        context.log.info("\n───────────────────────────────────────────────")
        context.log.info("PERSON_RAW_OP RUN SUMMARY")
        context.log.info("───────────────────────────────────────────────")
        context.log.info(f"BUIDs:                 {len(buids):,}")
        context.log.info(f"PS Queries:            {metrics['ps_queried']:,} (success {metrics['ps_success']:,}, empty {metrics['ps_empty']:,})")
        context.log.info(f"uidCarTerms:           {metrics['uidcarterm_total']:,}")
        context.log.info(f"SnapLogic Batches:     {metrics['snaplogic_batches_completed']:,}/{metrics['snaplogic_batches_started'] or '?'}")
        context.log.info(f"Persons Returned:      {metrics['persons_received']:,}")
        context.log.info(f"Inserts:               {metrics['insert_success']:,} new | {metrics['insert_skipped']:,} skipped")
        context.log.info(f"Errors:                PS={metrics['errors']['ps']} | Snap={metrics['errors']['snap']} | DB={metrics['errors']['db']}")
        context.log.info(f"Duration:              {int(elapsed//3600)}h {int((elapsed%3600)//60)}m {int(elapsed%60)}s")
        context.log.info("───────────────────────────────────────────────")

    return op


