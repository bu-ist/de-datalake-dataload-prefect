import asyncio
import httpx
import math
from datetime import datetime
from prefect import flow
from prefect.logging import get_run_logger
import asyncpg
from config.resources import AsyncpgPoolResource, DEPersonApiResource, CsToolsResource, VDSApiResource, SAPApiResource
from flows.person.person_tasks import fetch_buids_from_cs_tools_task, fetch_buids_from_sap_task, query_all_buids_task, process_uidCarTerms_batch_task, process_buids_batch_task, insert_persons_batch_task


@flow(name="person-raw-flow", description="Retrieves person data from Data Engineering Person API and inserts into Postgres database", retries=1, retry_delay_seconds=300, log_prints=True)
async def person_raw_flow():
    logger = get_run_logger()

    UIDCARTERM_BATCH_SIZE = 600
    BUID_BATCH_SIZE = 100
    CSTOOLS_SEMAPHORE_LIMIT = 10
    PERSON_API_SEMAPHORE_LIMIT = 5
    INSERT_SEMAPHORE_LIMIT = 100

    asyncpg_pool_config = AsyncpgPoolResource.get_pool_config()
    person_api_config = DEPersonApiResource.get_config()
    cstools_config = CsToolsResource.get_config()
    vds_api_config = VDSApiResource.get_config()
    sap_api_config = SAPApiResource.get_config()

    asyncpg_pool = await asyncpg.create_pool(**asyncpg_pool_config)

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
        "start_time": datetime.now(),
        "done": False,
    }

    buids = []

    try:
        cs_buids_task = await fetch_buids_from_cs_tools_task(cstools_config)
        buids.extend(cs_buids_task)
    except Exception as e:
        logger.error(f"❌ Failed CS Tools fetch: {type(e).__name__}: {e}")
        raise

    #TODO: Fetch ENS population Too, or call EVERY Population.

    #TODO: Re-enable VDS BUID fetch after new credentials are set up
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

    try:
        sap_buids_task = await fetch_buids_from_sap_task(sap_api_config)
        buids.extend(sap_buids_task)
    except Exception as e:
        logger.error(f"❌ Failed SAP fetch: {type(e).__name__}: {e}")
        raise

    buids = list(set(buids))
    logger.info(f"✅ Processing {len(buids):,} unique BUIDs")

    #TODO: Any failed BUIDs will go into the person_live_update queue for reprocessing

    cs_sem = asyncio.Semaphore(CSTOOLS_SEMAPHORE_LIMIT)
    person_api_sem = asyncio.Semaphore(PERSON_API_SEMAPHORE_LIMIT)
    insert_sem = asyncio.Semaphore(INSERT_SEMAPHORE_LIMIT)

    uidCarTerms, buids_only = [], []
    uidCarTerms_threshold_event, buids_threshold_event, all_cs_done = asyncio.Event(), asyncio.Event(), asyncio.Event()
    person_api_batch_queue: asyncio.Queue = asyncio.Queue()
    batch_counter = {"uidcarterms": 0, "buids": 0}
    insert_tasks = []
    insert_tasks_lock = asyncio.Lock()

    async def person_api_worker(person_api_client: httpx.AsyncClient) -> None:
        while True:
            batch_item = await person_api_batch_queue.get()
            if batch_item is None:
                person_api_batch_queue.task_done()
                break

            batch_type, batch_data, batch_id = batch_item
            try:
                persons = []
                if batch_type == "uidcarterms":
                    persons = await process_uidCarTerms_batch_task(batch=batch_data, batch_id=batch_id, person_api_client=person_api_client, person_api_config=person_api_config, person_api_sem=person_api_sem, metrics=metrics)
                elif batch_type == "buids":
                    persons = await process_buids_batch_task(batch=batch_data, batch_id=batch_id, person_api_client=person_api_client, person_api_config=person_api_config, person_api_sem=person_api_sem, metrics=metrics)

                if persons:
                    insert_task = asyncio.create_task(insert_persons_batch_task(persons=persons, batch_id=batch_id, batch_type=batch_type, asyncpg_pool=asyncpg_pool, insert_sem=insert_sem, metrics=metrics))
                    async with insert_tasks_lock:
                        insert_tasks.append(insert_task)
            except Exception as e:
                logger.error(f"Worker error {batch_type} batch {batch_id}: {type(e).__name__}: {e}")
                metrics["errors"]["person_api"] += 1
            finally:
                person_api_batch_queue.task_done()

    async def monitor_progress(interval: int = 15) -> None:
        total_buids = len(buids)
        start = metrics["start_time"]
        last_valid_total_runtime = None
        try:
            while not metrics["done"]:
                elapsed = (datetime.now() - start).total_seconds()
                cs_done = metrics["cs_queried"]
                progress = cs_done / total_buids if total_buids else 0

                if cs_done > 0:
                    avg_terms_per_student = metrics["uidcarterm_total"] / metrics["cs_success"] if metrics["cs_success"] > 0 else 0
                    frac_with_terms = metrics["cs_success"] / cs_done
                    remaining_buids = total_buids - cs_done
                    est_total_terms = metrics["uidcarterm_total"] + int(remaining_buids * frac_with_terms * avg_terms_per_student)
                    est_term_batches = math.ceil(est_total_terms / UIDCARTERM_BATCH_SIZE) if est_total_terms > 0 else 0
                    est_total_buids_without_terms = int((metrics["cs_empty"] / cs_done) * total_buids)
                    est_buid_batches = math.ceil(est_total_buids_without_terms / BUID_BATCH_SIZE) if est_total_buids_without_terms > 0 else 0
                    est_batches_total = est_term_batches + est_buid_batches
                else:
                    avg_terms_per_student = 0
                    est_term_batches = 0
                    est_buid_batches = 0
                    est_batches_total = 1

                total_batches_completed = metrics["uidcarterm_batches_completed"] + metrics["buid_batches_completed"]
                total_estimated_runtime = None
                if total_batches_completed > 0:
                    remaining_batches = max(est_batches_total - total_batches_completed, 0)
                    avg_batch_time = elapsed / max(total_batches_completed, 1)
                    total_estimated_runtime = elapsed + avg_batch_time * remaining_batches
                    last_valid_total_runtime = total_estimated_runtime

                if total_estimated_runtime is None and last_valid_total_runtime is not None:
                    total_estimated_runtime = last_valid_total_runtime

                eta_str = f"{int(total_estimated_runtime//3600):02}:{int((total_estimated_runtime%3600)//60):02}:{int(total_estimated_runtime%60):02}" if total_estimated_runtime is not None else "N/A"
                elapsed_str = f"{int(elapsed//3600):02}:{int((elapsed%3600)//60):02}:{int(elapsed%60):02}"

                cs_active = CSTOOLS_SEMAPHORE_LIMIT - cs_sem._value
                person_api_active = PERSON_API_SEMAPHORE_LIMIT - person_api_sem._value
                insert_active = INSERT_SEMAPHORE_LIMIT - insert_sem._value
                logger.info(
                    f"╔═══════════════════════════════════════════════════════════════════╗"
                    f"\n║ HEARTBEAT [{elapsed_str}] — ETA: {eta_str}               "
                    f"\n╠═══════════════════════════════════════════════════════════════════╣"
                    f"\n║ DATA COLLECTION                                                   "
                    f"\n║   CS Tools Queries: {cs_done:>6,} / {total_buids:<6,} ({progress*100:>5.1f}%)              "
                    f"\n║     └─ With Terms:    {metrics['cs_success']:>6,} students → {metrics['uidcarterm_total']:>6,} term records   "
                    f"\n║     └─ Without Terms: {metrics['cs_empty']:>6,} people (BUID only)               "
                    f"\n╠═══════════════════════════════════════════════════════════════════╣"
                    f"\n║ API BATCHES (Person API)                                          "
                    f"\n║   Student Batches:    {metrics['uidcarterm_batches_completed']:>3} / {est_term_batches:<3} completed                "
                    f"\n║   BUID Batches:       {metrics['buid_batches_completed']:>3} / {est_buid_batches:<3} completed                "
                    f"\n║   Total:              {total_batches_completed:>3} / {est_batches_total:<3} completed                "
                    f"\n║   Batch Queue:        {person_api_batch_queue.qsize():>3} pending                             "
                    f"\n╠═══════════════════════════════════════════════════════════════════╣"
                    f"\n║ DATABASE OPERATIONS                                               "
                    f"\n║   Persons Received:   {metrics['persons_received']:>6,}                                   "
                    f"\n║   Inserted:           {metrics['insert_success']:>6,} records                            "
                    f"\n║   Skipped:            {metrics['insert_skipped']:>6,} records                            "
                    f"\n╠═══════════════════════════════════════════════════════════════════╣"
                    f"\n║ RESOURCE USAGE                                                    "
                    f"\n║   Semaphores Active:  CS={cs_active}/{CSTOOLS_SEMAPHORE_LIMIT}  API={person_api_active}/{PERSON_API_SEMAPHORE_LIMIT}  Insert={insert_active}/{INSERT_SEMAPHORE_LIMIT}     "
                    f"\n║   Errors:             CS={metrics['errors']['cs']}  API={metrics['errors']['person_api']}  DB={metrics['errors']['db']}                  "
                    f"\n╚═══════════════════════════════════════════════════════════════════╝"
                )
                await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"⚠️  Heartbeat error: {e}")

    async def monitor_uidCarTerms() -> None:
        while True:
            done, pending = await asyncio.wait(
                [asyncio.create_task(uidCarTerms_threshold_event.wait()), asyncio.create_task(buids_threshold_event.wait()), asyncio.create_task(all_cs_done.wait())],
                return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()

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

            if all_cs_done.is_set():
                #TODO: run uidcarterms evenly across all semaphores if cstools queries are small. Will be important once we implement live updates
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
        person_api_workers = [asyncio.create_task(person_api_worker(person_api_client)) for _ in range(PERSON_API_SEMAPHORE_LIMIT)]

        async with httpx.AsyncClient(timeout=60, follow_redirects=True, verify=False, headers=cstools_config["headers"]) as cs_client:
            monitor_task = asyncio.create_task(monitor_uidCarTerms())

            await query_all_buids_task(buids=buids, cs_client=cs_client, cstools_config=cstools_config, cs_sem=cs_sem, metrics=metrics, uidCarTerms=uidCarTerms, buids_only=buids_only, uidCarTerms_threshold_event=uidCarTerms_threshold_event, buids_threshold_event=buids_threshold_event, UIDCARTERM_BATCH_SIZE=UIDCARTERM_BATCH_SIZE, BUID_BATCH_SIZE=BUID_BATCH_SIZE)

            all_cs_done.set()
            await monitor_task
            await person_api_batch_queue.join()

            for _ in person_api_workers:
                await person_api_batch_queue.put(None)
            await asyncio.gather(*person_api_workers, return_exceptions=True)
            await asyncio.gather(*insert_tasks, return_exceptions=True)

    metrics["done"] = True
    await heartbeat

    await asyncpg_pool.close()

    elapsed = (datetime.now() - metrics["start_time"]).total_seconds()
    total_batches_completed = metrics["uidcarterm_batches_completed"] + metrics["buid_batches_completed"]

    logger.info(f"\n✅ PERSON_RAW_FLOW COMPLETE")
    logger.info(f"   BUIDs: {len(buids):,} total | Students: {metrics['cs_success']:,} ({metrics['uidcarterm_total']:,} terms) | BUIDs only: {metrics['buids_only_count']:,}")
    logger.info(f"   Batches: {total_batches_completed} completed")
    logger.info(f"   Records: {metrics['persons_received']:,} received | {metrics['insert_success']:,} inserted | {metrics['insert_skipped']:,} skipped")
    logger.info(f"   Errors: CS={metrics['errors']['cs']} API={metrics['errors']['person_api']} DB={metrics['errors']['db']} | Duration: {int(elapsed//3600)}h {int((elapsed%3600)//60)}m {int(elapsed%60)}s")

    return {"status": "success", "buids_processed": len(buids), "records_inserted": metrics["insert_success"], "errors": metrics["errors"]}


if __name__ == "__main__":
    import asyncio
    asyncio.run(person_raw_flow())
