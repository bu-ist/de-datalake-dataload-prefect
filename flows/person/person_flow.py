import asyncio
import math
import tomllib
import httpx
from datetime import datetime
from prefect import flow
from prefect.logging import get_run_logger
from config.resources import PostgresResource, DEPersonApiResource, CsToolsResource, SAPApiResource, VDSApiResource
from flows.person.person_tasks import fetch_ps_buid_query_task, fetch_vds_buid_query_task, fetch_sap_buid_query_task, query_all_buids_task, process_uidCarTerms_batch_task, process_buids_batch_task, insert_persons_batch_task
from flows.person.person_tasks import _QUERIES_PATH

# Non-serializable shared state for inline subflows (same process/event loop).
# Populated by person_raw_flow before the subflow starts.
_batch_context: dict = {}


@flow(name="fetch-buids", log_prints=True)
async def fetch_buids_subflow(
    cstools_config: dict,
    sap_api_config: dict,
    vds_api_config: dict,
) -> list:
    logger = get_run_logger()
    with open(_QUERIES_PATH, "rb") as f:
        queries = tomllib.load(f)
    ps_queries = queries.get("PSQueries", [])
    sap_queries = queries.get("SAPQueries", [])
    vds_queries = queries.get("VDSQueries", [])

    ps_sem = asyncio.Semaphore(5)
    sap_sem = asyncio.Semaphore(5)
    vds_sem = asyncio.Semaphore(5)

    async def run_ps(query):
        async with ps_sem:
            return await fetch_ps_buid_query_task(query, cstools_config, query["json"].get("query_name", "unknown"))

    async def run_sap(query):
        async with sap_sem:
            return await fetch_sap_buid_query_task(query, sap_api_config, query["params"].get("BAPIName", "unknown"))

    async def run_vds(query, idx):
        async with vds_sem:
            return await fetch_vds_buid_query_task(query, vds_api_config, f"VDS-{idx}")

    results = await asyncio.gather(
        *[run_ps(q) for q in ps_queries],
        *[run_sap(q) for q in sap_queries],
        *[run_vds(q, i) for i, q in enumerate(vds_queries)],
        return_exceptions=True,
    )

    all_buids = []
    total = len(ps_queries) + len(sap_queries) + len(vds_queries)
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            if i < len(ps_queries):
                label = f"PSQuery {i}"
            elif i < len(ps_queries) + len(sap_queries):
                label = f"SAP query {i - len(ps_queries)}"
            else:
                label = f"VDS query {i - len(ps_queries) - len(sap_queries)}"
            logger.error(f"❌ {label} failed: {type(result).__name__}: {result}")
        else:
            all_buids.extend(result)

    buids = list(set(all_buids))
    logger.info(f"✅ {len(buids):,} unique BUIDs: [{', '.join(sorted(buids))}]")
    return buids


@flow(name="person-api-batches", log_prints=True)
async def person_batches_subflow(
    n_workers: int,
    person_api_config: dict,
) -> None:
    """
    Consumes person API batches from the shared queue as they are produced by the
    CS Tools query pipeline, concurrently calling the Person API.
    Runs alongside query_all_buids_task — starts immediately and waits for work.
    Results are pushed to the insert_queue for person_inserts_subflow.
    """
    logger = get_run_logger()
    queue: asyncio.Queue = _batch_context["queue"]
    insert_queue: asyncio.Queue = _batch_context["insert_queue"]
    person_api_sem: asyncio.Semaphore = _batch_context["person_api_sem"]
    metrics: dict = _batch_context["metrics"]

    state = {"inserts_started": False}

    async def worker(client: httpx.AsyncClient) -> None:
        while True:
            batch_item = await queue.get()
            if batch_item is None:
                queue.task_done()
                break
            batch_type, batch_data, batch_id = batch_item
            try:
                persons = []
                if batch_type == "uidcarterms":
                    persons = await process_uidCarTerms_batch_task(
                        batch=batch_data,
                        batch_id=batch_id,
                        person_api_client=client,
                        person_api_config=person_api_config,
                        person_api_sem=person_api_sem,
                        metrics=metrics,
                    )
                elif batch_type == "buids":
                    persons = await process_buids_batch_task(
                        batch=batch_data,
                        batch_id=batch_id,
                        person_api_client=client,
                        person_api_config=person_api_config,
                        person_api_sem=person_api_sem,
                        metrics=metrics,
                    )
                if persons:
                    if not state["inserts_started"]:
                        state["inserts_started"] = True
                        _batch_context["first_insert_event"].set()
                    await insert_queue.put((persons, batch_id, batch_type))
            except Exception as e:
                logger.error(f"❌ {batch_type} batch {batch_id} failed permanently: {type(e).__name__}: {e}")
                metrics["errors"]["person_api"] += 1
            finally:
                queue.task_done()

    async with httpx.AsyncClient(timeout=60, follow_redirects=True, headers=person_api_config["headers"]) as client:
        workers = [asyncio.create_task(worker(client)) for _ in range(n_workers)]
        await asyncio.gather(*workers)


@flow(name="person-inserts", log_prints=True)
async def person_inserts_subflow(n_workers: int) -> None:
    """
    Consumes person records from the insert_queue as they are produced by
    person_batches_subflow, concurrently inserting into the database.
    Runs alongside person_batches_subflow — starts immediately and waits for work.
    """
    logger = get_run_logger()
    insert_queue: asyncio.Queue = _batch_context["insert_queue"]
    insert_sem: asyncio.Semaphore = _batch_context["insert_sem"]
    asyncpg_pool = _batch_context["asyncpg_pool"]
    metrics: dict = _batch_context["metrics"]

    async def worker() -> None:
        while True:
            item = await insert_queue.get()
            if item is None:
                insert_queue.task_done()
                break
            persons, batch_id, batch_type = item
            insert_queue.task_done()
            try:
                await insert_persons_batch_task(
                    persons=persons,
                    batch_id=batch_id,
                    batch_type=batch_type,
                    asyncpg_pool=asyncpg_pool,
                    insert_sem=insert_sem,
                    metrics=metrics,
                )
            except Exception as e:
                logger.error(f"❌ An insert task failed permanently: {e}")

    workers = [asyncio.create_task(worker()) for _ in range(n_workers)]
    await asyncio.gather(*workers)


@flow(name="person-raw-flow", description="Retrieves person data from Data Engineering Person API and inserts into Postgres database", retries=1, retry_delay_seconds=300, log_prints=True)
async def person_raw_flow(
    cstools_semaphore_limit: int = 10,
    person_api_semaphore_limit: int = 5,
    insert_semaphore_limit: int = 100,
    uidcarterm_batch_size: int = 600,
    buid_batch_size: int = 100,
):
    logger = get_run_logger()

    person_api_config = DEPersonApiResource.get_config()
    cstools_config = CsToolsResource.get_config()
    sap_api_config = SAPApiResource.get_config()
    vds_api_config = VDSApiResource.get_config()

    start_time = datetime.now()
    metrics = {
        "cs_queried": 0,
        "cs_success": 0,
        "cs_empty": 0,
        "uidcarterm_total": 0,
        "buids_only_count": 0,
        "uidcarterm_batches_sent": 0,
        "uidcarterm_batches_completed": 0,
        "buid_batches_sent": 0,
        "buid_batches_completed": 0,
        "persons_received": 0,
        "insert_success": 0,
        "insert_skipped": 0,
        "errors": {"cs": 0, "person_api": 0, "db": 0},
        "done": False,
    }
    phase_info = {"total_buids": 0}

    async def monitor_progress(interval: int = 15) -> None:
        last_valid_total_runtime = None
        try:
            while not metrics["done"]:
                elapsed = (datetime.now() - start_time).total_seconds()
                elapsed_str = f"{int(elapsed//3600):02}:{int((elapsed%3600)//60):02}:{int(elapsed%60):02}"
                cs_done = metrics["cs_queried"]
                total_buids_known = phase_info["total_buids"]
                progress = cs_done / total_buids_known if total_buids_known else 0
                total_batches_completed = metrics["uidcarterm_batches_completed"] + metrics["buid_batches_completed"]

                if cs_done >= total_buids_known and total_buids_known > 0:
                    est_term_batches = batch_counter["uidcarterms"]
                    est_buid_batches = batch_counter["buids"]
                    est_batches_total = est_term_batches + est_buid_batches
                elif cs_done > 0:
                    avg_terms_per_student = metrics["uidcarterm_total"] / metrics["cs_success"] if metrics["cs_success"] > 0 else 0
                    frac_with_terms = metrics["cs_success"] / cs_done
                    remaining_buids = total_buids_known - cs_done
                    est_total_terms = metrics["uidcarterm_total"] + int(remaining_buids * frac_with_terms * avg_terms_per_student)
                    est_term_batches = math.ceil(est_total_terms / uidcarterm_batch_size) if est_total_terms > 0 else 0
                    est_total_buids_without_terms = int((metrics["cs_empty"] / cs_done) * total_buids_known)
                    est_buid_batches = math.ceil(est_total_buids_without_terms / buid_batch_size) if est_total_buids_without_terms > 0 else 0
                    est_batches_total = est_term_batches + est_buid_batches
                else:
                    est_term_batches = 0
                    est_buid_batches = 0
                    est_batches_total = 1

                total_estimated_runtime = None
                if total_batches_completed > 0:
                    remaining_batches = max(est_batches_total - total_batches_completed, 0)
                    avg_batch_time = elapsed / max(total_batches_completed, 1)
                    total_estimated_runtime = elapsed + avg_batch_time * remaining_batches
                    last_valid_total_runtime = total_estimated_runtime

                if total_estimated_runtime is None and last_valid_total_runtime is not None:
                    total_estimated_runtime = last_valid_total_runtime

                eta_str = f"{int(total_estimated_runtime//3600):02}:{int((total_estimated_runtime%3600)//60):02}:{int(total_estimated_runtime%60):02}" if total_estimated_runtime is not None else "N/A"
                logger.info(
                    f"╔═══════════════════════════════════════════════════════════════════╗"
                    f"\n║ HEARTBEAT [{elapsed_str}] — ETA: {eta_str}"
                    f"\n╠═══════════════════════════════════════════════════════════════════╣"
                    f"\n║ DATA COLLECTION                                                   "
                    f"\n║   CS Tools Queries: {cs_done:>6,} / {total_buids_known:<6,} ({progress*100:>5.1f}%)"
                    f"\n║     └─ With Terms:    {metrics['cs_success']:>6,} students → {metrics['uidcarterm_total']:>6,} term records"
                    f"\n║     └─ Without Terms: {metrics['cs_empty']:>6,} people (BUID only)"
                    f"\n╠═══════════════════════════════════════════════════════════════════╣"
                    f"\n║ API BATCHES (Person API)                                          "
                    f"\n║   Student Batches:    {metrics['uidcarterm_batches_completed']:>3} / {est_term_batches:<3} completed"
                    f"\n║   BUID Batches:       {metrics['buid_batches_completed']:>3} / {est_buid_batches:<3} completed"
                    f"\n║   Total:              {total_batches_completed:>3} / {est_batches_total:<3} completed"
                    f"\n╠═══════════════════════════════════════════════════════════════════╣"
                    f"\n║ DATABASE OPERATIONS                                               "
                    f"\n║   Persons Received:   {metrics['persons_received']:>6,}"
                    f"\n║   Inserted:           {metrics['insert_success']:>6,} records"
                    f"\n║   Skipped:            {metrics['insert_skipped']:>6,} records"
                    f"\n╠═══════════════════════════════════════════════════════════════════╣"
                    f"\n║   Errors:             CS={metrics['errors']['cs']}  API={metrics['errors']['person_api']}  DB={metrics['errors']['db']}"
                    f"\n╚═══════════════════════════════════════════════════════════════════╝"
                )
                await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"⚠️  Heartbeat error: {e}")

    heartbeat = asyncio.create_task(monitor_progress())

    # Phase 1: Fetch BUIDs (concurrent)
    #TODO: Fetch ENS population too, or call EVERY Population.
    buids = await fetch_buids_subflow(cstools_config, sap_api_config, vds_api_config)
    phase_info["total_buids"] = len(buids)

    #TODO: Any failed BUIDs will go into the person_live_update queue for reprocessing

    # Set up pipeline shared state (non-serializable — accessible to the inline subflows
    # because they run in the same process/event loop via asyncio.create_task)
    asyncpg_pool = await PostgresResource.get_pool()
    queue: asyncio.Queue = asyncio.Queue()
    insert_queue: asyncio.Queue = asyncio.Queue()
    first_insert_event = asyncio.Event()
    _batch_context.update({
        "queue": queue,
        "insert_queue": insert_queue,
        "person_api_sem": asyncio.Semaphore(person_api_semaphore_limit),
        "insert_sem": asyncio.Semaphore(insert_semaphore_limit),
        "insert_semaphore_limit": insert_semaphore_limit,
        "first_insert_event": first_insert_event,
        "asyncpg_pool": asyncpg_pool,
        "metrics": metrics,
    })

    uidCarTerms: list = []
    buids_only: list = []
    uidCarTerms_threshold_event = asyncio.Event()
    buids_threshold_event = asyncio.Event()
    all_cs_done = asyncio.Event()
    batch_counter = {"uidcarterms": 0, "buids": 0}
    cs_sem = asyncio.Semaphore(cstools_semaphore_limit)

    async def monitor_batches() -> None:
        """Watches for batch thresholds and pushes ready batches into the queue.
        Starts person_batches_subflow just before the first batch is pushed."""
        while True:
            done, _ = await asyncio.wait(
                [
                    asyncio.create_task(uidCarTerms_threshold_event.wait()),
                    asyncio.create_task(buids_threshold_event.wait()),
                    asyncio.create_task(all_cs_done.wait()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if uidCarTerms_threshold_event.is_set():
                uidCarTerms_threshold_event.clear()
                snapshot = uidCarTerms.copy()
                uidCarTerms.clear()
                if snapshot:
                    if "batches_task" not in _batch_context:
                        _batch_context["batches_task"] = asyncio.create_task(person_batches_subflow(
                            n_workers=person_api_semaphore_limit,
                            person_api_config=person_api_config,
                        ))
                    batch_counter["uidcarterms"] += 1
                    await queue.put(("uidcarterms", snapshot, batch_counter["uidcarterms"]))
            if buids_threshold_event.is_set():
                buids_threshold_event.clear()
                snapshot = buids_only.copy()
                buids_only.clear()
                if snapshot:
                    if "batches_task" not in _batch_context:
                        _batch_context["batches_task"] = asyncio.create_task(person_batches_subflow(
                            n_workers=person_api_semaphore_limit,
                            person_api_config=person_api_config,
                        ))
                    batch_counter["buids"] += 1
                    await queue.put(("buids", snapshot, batch_counter["buids"]))
            if all_cs_done.is_set():
                if "batches_task" not in _batch_context:
                    _batch_context["batches_task"] = asyncio.create_task(person_batches_subflow(
                        n_workers=person_api_semaphore_limit,
                        person_api_config=person_api_config,
                    ))
                if uidCarTerms:
                    snapshot = uidCarTerms.copy()
                    uidCarTerms.clear()
                    batch_counter["uidcarterms"] += 1
                    await queue.put(("uidcarterms", snapshot, batch_counter["uidcarterms"]))
                if buids_only:
                    snapshot = buids_only.copy()
                    buids_only.clear()
                    batch_counter["buids"] += 1
                    await queue.put(("buids", snapshot, batch_counter["buids"]))
                break

    async def start_inserts_when_ready() -> None:
        """Waits for the first insert to be ready, then starts person_inserts_subflow.
        Running as a task in person_raw_flow's context keeps the inserts subflow
        as a sibling (not a child) of person_batches_subflow in the Prefect UI."""
        await first_insert_event.wait()
        _batch_context["inserts_task"] = asyncio.create_task(
            person_inserts_subflow(n_workers=insert_semaphore_limit)
        )

    inserts_starter = asyncio.create_task(start_inserts_when_ready())

    async with httpx.AsyncClient(timeout=60, follow_redirects=True, verify=False, headers=cstools_config["headers"]) as cs_client:
        monitor_task = asyncio.create_task(monitor_batches())
        await query_all_buids_task(
            buids=buids,
            cs_client=cs_client,
            cstools_config=cstools_config,
            cs_sem=cs_sem,
            metrics=metrics,
            uidCarTerms=uidCarTerms,
            buids_only=buids_only,
            uidCarTerms_threshold_event=uidCarTerms_threshold_event,
            buids_threshold_event=buids_threshold_event,
            uidcarterm_batch_size=uidcarterm_batch_size,
            buid_batch_size=buid_batch_size,
        )
        all_cs_done.set()
        await monitor_task

    # Drain person_api queue → signal person_batches_subflow workers → await it
    await queue.join()
    if batches_task := _batch_context.get("batches_task"):
        for _ in range(person_api_semaphore_limit):
            await queue.put(None)
        await batches_task

    # Ensure inserts_starter exits (no-op if it already fired, cancels if no persons found)
    if not inserts_starter.done():
        inserts_starter.cancel()
    await asyncio.gather(inserts_starter, return_exceptions=True)

    # All Person API calls done; drain insert_queue → signal person_inserts_subflow → await it
    await insert_queue.join()
    if inserts_task := _batch_context.get("inserts_task"):
        for _ in range(insert_semaphore_limit):
            await insert_queue.put(None)
        await inserts_task

    _batch_context.clear()
    await asyncpg_pool.close()

    metrics["done"] = True
    await heartbeat

    elapsed = (datetime.now() - start_time).total_seconds()
    total_batches = batch_counter["uidcarterms"] + batch_counter["buids"]
    total_batches_completed = metrics["uidcarterm_batches_completed"] + metrics["buid_batches_completed"]

    logger.info(f"\n✅ PERSON_RAW_FLOW COMPLETE")
    logger.info(f"   BUIDs: {len(buids):,} total | Students: {metrics['cs_success']:,} ({metrics['uidcarterm_total']:,} terms) | BUIDs only: {metrics['buids_only_count']:,}")
    logger.info(f"   Batches: {total_batches_completed} of {total_batches} completed")
    logger.info(f"   Records: {metrics['persons_received']:,} received | {metrics['insert_success']:,} inserted | {metrics['insert_skipped']:,} skipped")
    logger.info(f"   Errors: CS={metrics['errors']['cs']} API={metrics['errors']['person_api']} DB={metrics['errors']['db']} | Duration: {int(elapsed//3600)}h {int((elapsed%3600)//60)}m {int(elapsed%60)}s")

    return {"status": "success", "buids_processed": len(buids), "records_inserted": metrics["insert_success"], "errors": metrics["errors"]}


if __name__ == "__main__":
    import asyncio
    asyncio.run(person_raw_flow())
