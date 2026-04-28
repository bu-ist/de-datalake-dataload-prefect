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
    logger.info(f"📡 PSQuery [{query_name}] — integrations: {query.get('integrations', [])}")
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
    logger.info(f"📡 SAP [{bapi_name}] — integrations: {query.get('integrations', [])}")
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


async def _query_student_single(
    buid: str,
    cs_client: httpx.AsyncClient,
    cstools_config: dict,
    cs_sem: asyncio.Semaphore,
    metrics: dict,
    logger,
) -> list:
    async with cs_sem:
        metrics["student_cs_queried"] += 1
        for attempt in range(1, 6):
            try:
                payload = {"query_name": "BU_TERM_STD_FULL_TERM", "prompt_names": ["BUID"], "prompt_values": [buid]}
                resp = await cs_client.post(cstools_config["url"], json=payload, headers=cstools_config["headers"], timeout=30)
                if resp.is_error:
                    logger.error(f"❌ CS Tools [student/{buid}] {resp.status_code}: {resp.text}")
                resp.raise_for_status()
                terms = resp.json().get("data", [])
                if terms:
                    metrics["student_cs_success"] += 1
                    metrics["student_term_total"] += len(terms)
                else:
                    metrics["student_cs_empty"] += 1
                return terms
            except Exception as e:
                if attempt < 5:
                    await asyncio.sleep(10 * 3 ** (attempt - 1))
                else:
                    metrics["errors"]["cs_student"] += 1
                    logger.error(f"❌ CS Tools [student/{buid}] failed after {attempt} attempts: {type(e).__name__}: {e}")
                    metrics["student_cs_empty"] += 1
                    return []


async def _query_faculty_single(
    buid: str,
    cs_client: httpx.AsyncClient,
    cstools_config: dict,
    cs_sem: asyncio.Semaphore,
    metrics: dict,
    logger,
) -> list:
    async with cs_sem:
        metrics["faculty_cs_queried"] += 1
        for attempt in range(1, 6):
            try:
                payload = {"query_name": "BU_FACULTY_GET", "prompt_names": ["EMPLID"], "prompt_values": [buid]}
                resp = await cs_client.post(cstools_config["url"], json=payload, headers=cstools_config["headers"], timeout=30)
                if resp.is_error:
                    logger.error(f"❌ CS Tools [faculty/{buid}] {resp.status_code}: {resp.text}")
                resp.raise_for_status()
                terms = resp.json().get("data", [])
                if terms:
                    metrics["faculty_cs_success"] += 1
                    metrics["faculty_term_total"] += len(terms)
                else:
                    metrics["faculty_cs_empty"] += 1
                return terms
            except Exception as e:
                if attempt < 5:
                    await asyncio.sleep(10 * 3 ** (attempt - 1))
                else:
                    metrics["errors"]["cs_faculty"] += 1
                    logger.error(f"❌ CS Tools [faculty/{buid}] failed after {attempt} attempts: {type(e).__name__}: {e}")
                    metrics["faculty_cs_empty"] += 1
                    return []


def _finalize_buid(
    buid: str,
    buid_expected: dict,
    buid_done: dict,
    student_results: dict,
    faculty_results: dict,
    metrics: dict,
    term_buids: list,
    buids_only: list,
    term_buids_threshold_event: asyncio.Event,
    buids_threshold_event: asyncio.Event,
    term_batch_size: int,
    buid_batch_size: int,
) -> None:
    """Route a BUID once all its expected CS queries have completed. No await points — safe under asyncio."""
    buid_done[buid] += 1
    if buid_done[buid] < buid_expected[buid]:
        return
    st = student_results.get(buid, [])
    ft = faculty_results.get(buid, [])
    if st or ft:
        term_buids.append({"buid": buid, "student_terms": st, "faculty_terms": ft})
        if sum(len(e["student_terms"]) + len(e["faculty_terms"]) for e in term_buids) >= term_batch_size:
            term_buids_threshold_event.set()
    else:
        metrics["buids_only_count"] += 1
        buids_only.append(buid)
        if len(buids_only) >= buid_batch_size:
            buids_threshold_event.set()


@task(name="query-student-terms", retries=1, retry_delay_seconds=30, cache_policy=NO_CACHE, tags=["query-buid-terms"])
async def query_student_terms_task(
    student_buids: List[str],
    buid_expected: dict,
    buid_done: dict,
    student_results: dict,
    faculty_results: dict,
    cs_client: httpx.AsyncClient,
    cstools_config: dict,
    cs_sem: asyncio.Semaphore,
    metrics: dict,
    term_buids: list,
    buids_only: list,
    term_buids_threshold_event: asyncio.Event,
    buids_threshold_event: asyncio.Event,
    term_batch_size: int,
    buid_batch_size: int,
) -> None:
    logger = get_run_logger()
    logger.info(f"⏳ BU_TERM_STD_FULL_TERM: {len(student_buids):,} student BUIDs")

    async def run(buid: str) -> None:
        terms = await _query_student_single(buid, cs_client, cstools_config, cs_sem, metrics, logger)
        student_results[buid] = terms
        _finalize_buid(buid, buid_expected, buid_done, student_results, faculty_results, metrics,
                       term_buids, buids_only, term_buids_threshold_event, buids_threshold_event,
                       term_batch_size, buid_batch_size)

    await asyncio.gather(*[run(b) for b in student_buids], return_exceptions=True)
    logger.info(
        f"✅ BU_TERM_STD_FULL_TERM complete: "
        f"{metrics['student_cs_success']:,} with terms ({metrics['student_term_total']:,} records), "
        f"{metrics['student_cs_empty']:,} empty, {metrics['errors']['cs_student']} errors"
    )


@task(name="query-faculty-terms", retries=1, retry_delay_seconds=30, cache_policy=NO_CACHE, tags=["query-buid-terms"])
async def query_faculty_terms_task(
    faculty_buids: List[str],
    buid_expected: dict,
    buid_done: dict,
    student_results: dict,
    faculty_results: dict,
    cs_client: httpx.AsyncClient,
    cstools_config: dict,
    cs_sem: asyncio.Semaphore,
    metrics: dict,
    term_buids: list,
    buids_only: list,
    term_buids_threshold_event: asyncio.Event,
    buids_threshold_event: asyncio.Event,
    term_batch_size: int,
    buid_batch_size: int,
) -> None:
    logger = get_run_logger()
    logger.info(f"⏳ BU_FACULTY_GET: {len(faculty_buids):,} faculty BUIDs")

    async def run(buid: str) -> None:
        terms = await _query_faculty_single(buid, cs_client, cstools_config, cs_sem, metrics, logger)
        faculty_results[buid] = terms
        _finalize_buid(buid, buid_expected, buid_done, student_results, faculty_results, metrics,
                       term_buids, buids_only, term_buids_threshold_event, buids_threshold_event,
                       term_batch_size, buid_batch_size)

    await asyncio.gather(*[run(b) for b in faculty_buids], return_exceptions=True)
    logger.info(
        f"✅ BU_FACULTY_GET complete: "
        f"{metrics['faculty_cs_success']:,} with terms ({metrics['faculty_term_total']:,} records), "
        f"{metrics['faculty_cs_empty']:,} empty, {metrics['errors']['cs_faculty']} errors"
    )


@task(
    name="process-terms-batch",
    retries=5,
    retry_delay_seconds=10,
    task_run_name="terms-batch-{batch_id}",
    cache_policy=NO_CACHE,
    tags=["get-person-batch"]
)
async def process_uidCarTerms_batch_task(
    batch: List[dict],
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

    uid_car_term_data = []
    emplid_term_data = []
    buid_summary_parts = []

    for entry in batch:
        buid = entry["buid"]
        student_terms = entry.get("student_terms", [])
        faculty_terms = entry.get("faculty_terms", [])

        for t in student_terms:
            uid_car_term_data.append(
                {("buid" if k == "CAMPUS_ID" else k.lower()): v for k, v in t.items() if k != "attr:rownumber"}
            )
        for t in faculty_terms:
            emplid_term_data.append({"emplid": t.get("EMPLID", buid), "strm": str(t.get("STRM", ""))})

        pairs = [f"{{{t.get('ACAD_CAREER', '?')}, {t.get('STRM')}}}" for t in student_terms] + \
                [f"{{faculty, {t.get('STRM')}}}" for t in faculty_terms]
        buid_summary_parts.append(f"{buid}: [{', '.join(pairs)}]")

    num_buids = len(batch)
    num_terms = len(uid_car_term_data) + len(emplid_term_data)
    buid_summary = "\n  ".join(buid_summary_parts)

    payload: dict = {
        "objects": ["student", "affiliate", "faculty", "employee"],
        "options": {"response_format": "ndjson", "batch_size": num_terms + 1},
    }
    if uid_car_term_data:
        payload["student"] = {"uid_car_term": uid_car_term_data}
    if emplid_term_data:
        payload["faculty"] = {"emplid_term": emplid_term_data}

    async with person_api_sem:
        metrics["uidcarterm_batches_sent"] += 1
        logger.info(
            f"📦 Batch {batch_id} [Terms]: {num_buids} BUIDs, "
            f"{len(uid_car_term_data)} student terms, {len(emplid_term_data)} faculty terms\n  {buid_summary}"
        )
        api_start = datetime.now()
        persons = []
        #TODO: Person API takes about 5 minutes to finish. Timeout after 11 minutes. Log any buids that failed or insert them into queue to redo (live updates queue)
        async with person_api_client.stream("POST", person_api_config["url"], json=payload, timeout=1200) as resp:
            if resp.is_error:
                await resp.aread()
                logger.error(f"❌ Person API batch {batch_id} [Terms] {resp.status_code}: {resp.text}")
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

        logger.info(f"✅ Batch {batch_id} [Terms]: {len(persons)} persons in {api_duration:.1f}s")
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
        async with person_api_client.stream("POST", person_api_config["url"], json=payload, timeout=1200) as resp:
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
