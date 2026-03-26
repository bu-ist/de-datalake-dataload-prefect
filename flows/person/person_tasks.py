import asyncio
import json
import httpx
from datetime import datetime
from typing import List
from prefect import task
from prefect.cache_policies import NONE as NO_CACHE
from prefect.logging import get_run_logger


@task(name="fetch-buids-from-cs-tools", retries=2, retry_delay_seconds=30, tags=["fetch-buids"])
async def fetch_buids_from_cs_tools_task(cstools_config: dict) -> List[str]:
    logger = get_run_logger()
    logger.info(f"📡 Fetching BUIDs from CS Tools...")
    try:
        async with httpx.AsyncClient(verify=False, follow_redirects=True) as client:
            resp = await client.post(
                cstools_config["url"],
                json={"query_name": "BU_PARM_0216_QRY"},
                headers=cstools_config["headers"],
                timeout=30,
            )
            resp.raise_for_status()
            rows = resp.json().get("data", [])
        buids = [row.get("CAMPUS_ID") for row in rows if row.get("CAMPUS_ID")]
        logger.info(f"✅ Retrieved {len(buids)} BUIDs from CS Tools")
        return buids
    except Exception as e:
        logger.error(f"❌ Fetch failed: {type(e).__name__}: {e}")
        raise


@task(name="fetch-buids-from-sap", retries=2, retry_delay_seconds=30, tags=["fetch-buids"])
async def fetch_buids_from_sap_task(sap_api_config: dict) -> List[str]:
    logger = get_run_logger()
    logger.info(f"📡 Fetching BUIDs from SAP...")
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
    logger.info(f"✅ Retrieved {len(buids)} BUIDs from SAP")
    return buids


async def query_cs_single(
    buid: str,
    cs_client: httpx.AsyncClient,
    cstools_config: dict,
    cs_sem: asyncio.Semaphore,
    metrics: dict,
    logger,
    uidCarTerms: list,
    buids_only: list,
    uidCarTerms_threshold_event: asyncio.Event,
    buids_threshold_event: asyncio.Event,
    uidcarterm_batch_size: int,
    buid_batch_size: int,
) -> None:
    async with cs_sem:
        metrics["cs_queried"] += 1
        for attempt in range(1, 6):
            try:
                payload = {"query_name": "BU_TERM_STD_FULL_TERM", "prompt_names": ["BUID"], "prompt_values": [buid]}
                resp = await cs_client.post(cstools_config["url"], json=payload, headers=cstools_config["headers"], timeout=30)
                resp.raise_for_status()
                uidCarTerm = resp.json().get('data', [])

                #TODO: Add logic for Faculty Terms. Consider if buid is a student and faculty for Terms

                if uidCarTerm:
                    metrics["cs_success"] += 1
                    metrics["uidcarterm_total"] += len(uidCarTerm)
                    uidCarTerms.append(uidCarTerm)
                    if len([item for row in uidCarTerms for item in row]) >= uidcarterm_batch_size:
                        uidCarTerms_threshold_event.set()
                else:
                    metrics["cs_empty"] += 1
                    metrics["buids_only_count"] += 1
                    buids_only.append(buid)
                    if len(buids_only) >= buid_batch_size:
                        buids_threshold_event.set()
                return
            except Exception as e:
                if attempt < 5:
                    await asyncio.sleep(10 * 3 ** (attempt - 1))
                else:
                    metrics["errors"]["cs"] += 1
                    logger.error(f"❌ CS Tools query failed for BUID {buid} after {attempt} attempts: {type(e).__name__}: {e}")
                    return


@task(name="query-all-buids-from-cs-tools", retries=1, retry_delay_seconds=30, cache_policy=NO_CACHE, tags=["query-buid-terms"])
async def query_all_buids_task(
    buids: List[str],
    cs_client: httpx.AsyncClient,
    cstools_config: dict,
    cs_sem: asyncio.Semaphore,
    metrics: dict,
    uidCarTerms: list,
    buids_only: list,
    uidCarTerms_threshold_event: asyncio.Event,
    buids_threshold_event: asyncio.Event,
    uidcarterm_batch_size: int,
    buid_batch_size: int,
) -> None:
    logger = get_run_logger()
    logger.info(f"⏳ Starting CS Tools queries for {len(buids)} BUIDs...")
    await asyncio.gather(
        *(query_cs_single(
            buid=buid,
            cs_client=cs_client,
            cstools_config=cstools_config,
            cs_sem=cs_sem,
            metrics=metrics,
            logger=logger,
            uidCarTerms=uidCarTerms,
            buids_only=buids_only,
            uidCarTerms_threshold_event=uidCarTerms_threshold_event,
            buids_threshold_event=buids_threshold_event,
            uidcarterm_batch_size=uidcarterm_batch_size,
            buid_batch_size=buid_batch_size,
        ) for buid in buids),
        return_exceptions=True
    )
    logger.info(f"✅ CS Tools complete: {metrics['cs_success']} successful, {metrics['cs_empty']} empty, {metrics['errors']['cs']} errors")


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
    logger = get_run_logger()
    flattened = [item for row in batch for item in row]
    if not flattened:
        logger.info(f"📦 Batch {batch_id}: Empty - skipping")
        return []

    unique_buids = set(item.get("CAMPUS_ID") for item in flattened if item.get("CAMPUS_ID"))
    num_terms = len(flattened)
    num_students = len(unique_buids)

    logger.info(f"📦 Batch {batch_id} [Students]: {num_students} students, {num_terms} terms — [{', '.join(sorted(unique_buids))}]")

    uid_car_term_data = [
        {("buid" if k=="CAMPUS_ID" else k.lower()): v for k, v in item.items() if k != "attr:rownumber"}
        for item in flattened
    ]
    payload = {"objects": ["student", "affiliate", "faculty", "employee"], "student": {"uid_car_term": uid_car_term_data}}

    async with person_api_sem:
        metrics["uidcarterm_batches_sent"] += 1
        api_start = datetime.now()
        #TODO: Person API takes about 5 minutes to finish. Timeout after 11 minutes. Log any buids that failed or insert them into queue to redo (live updates queue)
        resp = await person_api_client.post(person_api_config["url"], json=payload, timeout=1200)
        resp.raise_for_status()
        api_duration = (datetime.now() - api_start).total_seconds()
        persons = [p for p in resp.json().get("data", []) if p.get("personid")]
        metrics["persons_received"] += len(persons)
        metrics["uidcarterm_batches_completed"] += 1

        logger.info(f"✅ Batch {batch_id} [Students]: {len(persons)} persons in {api_duration:.1f}s")
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
    logger = get_run_logger()
    if not batch:
        logger.info(f"📦 Batch {batch_id}: Empty - skipping")
        return []

    logger.info(f"📦 Batch {batch_id} [BUIDs]: {len(batch)} BUIDs — [{', '.join(batch)}]")

    payload = {"buids": batch, "objects": ["student", "affiliate", "faculty", "employee"]}

    async with person_api_sem:
        metrics["buid_batches_sent"] += 1
        api_start = datetime.now()
        #TODO: Person API takes about 5 minutes to finish. Timeout after 11 minutes. Log any buids that failed or insert them into queue to redo (live updates queue)
        resp = await person_api_client.post(person_api_config["url"], json=payload, timeout=10000)
        resp.raise_for_status()
        api_duration = (datetime.now() - api_start).total_seconds()
        persons = [p for p in resp.json().get("data", []) if p.get("personid")]
        metrics["persons_received"] += len(persons)
        metrics["buid_batches_completed"] += 1

        logger.info(f"✅ Batch {batch_id} [BUIDs]: {len(persons)} persons in {api_duration:.1f}s")
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
    logger = get_run_logger()
    insert_batch_start = datetime.now()

    all_uids = sorted(p.get("personid") for p in persons if p.get("personid"))
    logger.info(f"📥 Insert batch {batch_id} [{batch_type}]: {len(persons)} persons — [{', '.join(all_uids)}]")

    records = []
    skipped_count = 0

    for p in persons:
        uid = p.get("personid")
        if not uid:
            skipped_count += 1
            continue

        person_basic = p.get("personBasic")
        if person_basic and isinstance(person_basic, dict):
            for k in ("ssn", "socialSecurityNumber", "sexualOrientation"):
                person_basic.pop(k, None)

        student_info = p.get("studentInfo")
        if student_info and isinstance(student_info, dict):
            for k in ("finAid", "finAidReceived"):
                student_info.pop(k, None)

        records.append((uid, json.dumps(p)))

    if records:
        try:
            async with insert_sem:
                async with asyncpg_pool.acquire() as conn:
                    #TODO: Add to skipped count if person_raw doesn't need to be updated
                    await conn.executemany("INSERT INTO person_raw.person_data (bu_uid, person_data) VALUES ($1, $2::jsonb)", records)
        except Exception as e:
            insert_batch_duration = (datetime.now() - insert_batch_start).total_seconds()
            logger.error(f"❌ Batch {batch_id} [{batch_type}]: Insert failed after all retries: {type(e).__name__}: {e} ({insert_batch_duration:.2f}s)")
            raise

    inserted_count = len(records)
    metrics["insert_success"] += inserted_count
    metrics["insert_skipped"] += skipped_count

    insert_batch_duration = (datetime.now() - insert_batch_start).total_seconds()
    logger.info(f"✅ Batch {batch_id} [{batch_type}]: {inserted_count} inserted, {skipped_count} skipped ({insert_batch_duration:.2f}s)")

    return {"batch_id": batch_id, "batch_type": batch_type, "inserted": inserted_count, "skipped": skipped_count, "duration_seconds": insert_batch_duration}
