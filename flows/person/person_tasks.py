import asyncio
import json
import tomllib
import httpx
from datetime import datetime
from pathlib import Path
from typing import List
from prefect import task
from prefect.cache_policies import NONE as NO_CACHE
from prefect.logging import get_run_logger

_QUERIES_PATH = Path(__file__).resolve().parents[2] / "config" / "person_queries.toml"


@task(name="ps-buid-query", task_run_name="PSQuery-{query_name}", retries=2, retry_delay_seconds=30, tags=["fetch-buids"])
async def fetch_ps_buid_query_task(query: dict, cstools_config: dict, query_name: str) -> List[str]:
    logger = get_run_logger()
    logger.info(f"📡 PSQuery [{query_name}]")
    try:
        async with httpx.AsyncClient(verify=False, follow_redirects=True) as client:
            resp = await client.post(
                cstools_config["url"],
                params=query.get("params") or None,
                json=query.get("json") or {},
                headers=cstools_config["headers"],
                timeout=120,
            )
            if resp.is_error:
                logger.error(f"❌ PSQuery [{query_name}] {resp.status_code}: {resp.text}")
            resp.raise_for_status()
            rows = resp.json().get("data", [])
        buids = [row.get("CAMPUS_ID") for row in rows if row.get("CAMPUS_ID")]
        logger.info(f"✅ PSQuery [{query_name}]: {len(buids)} BUIDs retrieved")
        return buids
    except Exception as e:
        logger.error(f"❌ PSQuery [{query_name}] failed: {type(e).__name__}: {e}")
        raise


@task(name="vds-buid-query", task_run_name="VDS-{query_name}", retries=2, retry_delay_seconds=30, tags=["fetch-buids"])
async def fetch_vds_buid_query_task(query: dict, vds_api_config: dict, query_name: str) -> List[str]:
    logger = get_run_logger()
    logger.info(f"📡 VDS [{query_name}]")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                vds_api_config["url"],
                json=query.get("json") or {},
                headers=vds_api_config["headers"],
                timeout=60,
            )
            if resp.is_error:
                logger.error(f"❌ VDS [{query_name}] {resp.status_code}: {resp.text}")
            resp.raise_for_status()
            data = resp.json()
            rows = []
            for item in (data if isinstance(data, list) else [data]):
                rows.extend(item.get("entity", {}).get("resources", []))
            logger.info(f"🔍 VDS [{query_name}]: {len(rows)} resources in response")
        buids = [row.get("attributes", {}).get("PersonBuid") for row in rows if row.get("attributes", {}).get("PersonBuid")]
        logger.info(f"✅ VDS [{query_name}]: {len(buids)} BUIDs retrieved")
        return buids
    except Exception as e:
        logger.error(f"❌ VDS [{query_name}] failed: {type(e).__name__}: {e}")
        raise


@task(name="sap-buid-query", task_run_name="SAP-{bapi_name}", retries=2, retry_delay_seconds=30, tags=["fetch-buids"])
async def fetch_sap_buid_query_task(query: dict, sap_api_config: dict, bapi_name: str) -> List[str]:
    logger = get_run_logger()
    logger.info(f"📡 SAP [{bapi_name}]")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                sap_api_config["url"],
                params=query.get("params") or None,
                json=query.get("json") or {},
                headers=sap_api_config["headers"],
                timeout=30,
            )
            if resp.is_error:
                logger.error(f"❌ SAP [{bapi_name}] {resp.status_code}: {resp.text}")
            resp.raise_for_status()
            rows = resp.json().get("ET_EMP_LIST", [])
        buids = [row.get("BUID") for row in rows if row.get("EMP_STATUS") == "3 - Active" and row.get("BUID")]
        logger.info(f"✅ SAP [{bapi_name}]: {len(buids)} BUIDs retrieved")
        return buids
    except Exception as e:
        logger.error(f"❌ SAP [{bapi_name}] failed: {type(e).__name__}: {e}")
        raise




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
                if resp.is_error:
                    logger.error(f"❌ CS Tools [{buid}] {resp.status_code}: {resp.text}")
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

    buid_car_terms: dict = {}
    for item in flattened:
        buid = item.get("CAMPUS_ID", "?")
        career = item.get("ACAD_CAREER", "?")
        term = str(item.get("STRM", "?"))
        buid_car_terms.setdefault(buid, []).append(f"{{{career}, {term}}}")
    buid_summary = "\n  ".join(
        f"{buid}: [{', '.join(pairs)}]"
        for buid, pairs in sorted(buid_car_terms.items())
    )
    uid_car_term_data = [
        {("buid" if k=="CAMPUS_ID" else k.lower()): v for k, v in item.items() if k != "attr:rownumber"}
        for item in flattened
    ]
    payload = {"objects": ["student", "affiliate", "faculty", "employee"], "student": {"uid_car_term": uid_car_term_data}, "options": {"response_format": "ndjson", "batch_size": num_terms + 1}}

    async with person_api_sem:
        metrics["uidcarterm_batches_sent"] += 1
        logger.info(f"📦 Batch {batch_id} [Students]: {num_students} BUIDs, {num_terms} terms\n  {buid_summary}")
        api_start = datetime.now()
        persons = []
        #TODO: Person API takes about 5 minutes to finish. Timeout after 11 minutes. Log any buids that failed or insert them into queue to redo (live updates queue)
        async with person_api_client.stream("POST", person_api_config["url"], json=payload, timeout=1200) as resp:
            if resp.is_error:
                await resp.aread()
                logger.error(f"❌ Person API batch {batch_id} [Students] {resp.status_code}: {resp.text}")
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if not line.strip():
                    continue
                record = json.loads(line)
                if record.get("type") == "record":
                    data = record.get("data", {})
                    if data.get("personid"):
                        persons.append(data)
        api_duration = (datetime.now() - api_start).total_seconds()
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

    payload = {"buids": batch, "objects": ["student", "affiliate", "faculty", "employee"], "options": {"response_format": "ndjson"}}

    async with person_api_sem:
        metrics["buid_batches_sent"] += 1
        api_start = datetime.now()
        persons = []
        #TODO: Person API takes about 5 minutes to finish. Timeout after 11 minutes. Log any buids that failed or insert them into queue to redo (live updates queue)
        async with person_api_client.stream("POST", person_api_config["url"], json=payload, timeout=10000) as resp:
            if resp.is_error:
                await resp.aread()
                logger.error(f"❌ Person API batch {batch_id} [BUIDs] {resp.status_code}: {resp.text}")
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if not line.strip():
                    continue
                record = json.loads(line)
                if record.get("type") == "record":
                    data = record.get("data", {})
                    if data.get("personid"):
                        persons.append(data)
        api_duration = (datetime.now() - api_start).total_seconds()
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
    metrics: dict,
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

    trigger_skipped = 0
    trigger_skipped_buids: list = []
    actually_inserted_buids: list = []
    if records:
        try:
            async with insert_sem:
                async with asyncpg_pool.acquire() as conn:
                    result = await conn.fetch(
                        "INSERT INTO person_raw.person_data (bu_uid, person_data) "
                        "SELECT * FROM unnest($1::text[], $2::jsonb[]) AS t(bu_uid, person_data) "
                        "RETURNING bu_uid",
                        [r[0] for r in records],
                        [r[1] for r in records],
                    )
            inserted_buids_set = {row["bu_uid"] for row in result}
            trigger_skipped_buids = sorted(r[0] for r in records if r[0] not in inserted_buids_set)
            actually_inserted_buids = sorted(inserted_buids_set)
            trigger_skipped = len(trigger_skipped_buids)
            skipped_count += trigger_skipped
        except Exception as e:
            insert_batch_duration = (datetime.now() - insert_batch_start).total_seconds()
            logger.error(f"❌ Batch {batch_id} [{batch_type}]: Insert failed after all retries: {type(e).__name__}: {e} ({insert_batch_duration:.2f}s)")
            raise

    inserted_count = len(records) - trigger_skipped
    metrics["insert_success"] += inserted_count
    metrics["insert_skipped"] += skipped_count

    insert_batch_duration = (datetime.now() - insert_batch_start).total_seconds()
    inserted_detail = f"\n  inserted:        [{', '.join(actually_inserted_buids)}]" if actually_inserted_buids else ""
    skipped_detail = f"\n  skipped (unchanged): [{', '.join(trigger_skipped_buids)}]" if trigger_skipped_buids else ""
    logger.info(f"✅ Batch {batch_id} [{batch_type}]: {inserted_count} inserted, {skipped_count} skipped ({insert_batch_duration:.2f}s){inserted_detail}{skipped_detail}")

    return {"batch_id": batch_id, "batch_type": batch_type, "inserted": inserted_count, "skipped": skipped_count, "duration_seconds": insert_batch_duration}
