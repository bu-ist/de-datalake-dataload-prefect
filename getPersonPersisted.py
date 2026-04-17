import asyncio
import json
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from config.resources import CsToolsResource, PostgresResource, SAPApiResource, VDSApiResource

_pool: asyncpg.Pool | None = None

_DB_BATCH_SIZE = 500   # BUIDs per SQL query
_DB_CONCURRENCY = 12   # max concurrent DB connections (matches pool min_size)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool
    _pool = await PostgresResource.get_pool()
    yield
    await _pool.close()


app = FastAPI(lifespan=lifespan)


class UidCarTerm(BaseModel):
    uid: str
    career: str
    term: str


class PersonQueryRequest(BaseModel):
    vds_query: Optional[dict[str, Any]] = None
    ps_query: Optional[dict[str, Any]] = None
    sap_query: Optional[dict[str, Any]] = None
    buids: Optional[list[str]] = None
    uidCarTerms: Optional[list[UidCarTerm]] = None


async def _fetch_vds_buids(query: dict) -> list[str]:
    config = VDSApiResource.get_config()
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            config["url"],
            json=query.get("json") or {},
            headers=config["headers"],
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
    rows: list = []
    for item in (data if isinstance(data, list) else [data]):
        rows.extend(item.get("entity", {}).get("resources", []))
    return [row["attributes"]["PersonBuid"] for row in rows if row.get("attributes", {}).get("PersonBuid")]


async def _fetch_ps_buids_and_terms(query: dict) -> tuple[list[str], list[UidCarTerm]]:
    """Runs a PS/CS Tools query and returns both the BUIDs and the uidCarTerms derived from
    the (CAMPUS_ID, ACAD_CAREER, STRM) columns — these are used to filter studentSemester."""
    config = CsToolsResource.get_config()
    async with httpx.AsyncClient(verify=False, follow_redirects=True) as client:
        resp = await client.post(
            config["url"],
            params=query.get("params") or None,
            json=query.get("json") or {},
            headers=config["headers"],
            timeout=120,
        )
        resp.raise_for_status()
        rows = resp.json().get("data", [])
    buids = [row["CAMPUS_ID"] for row in rows if row.get("CAMPUS_ID")]
    uid_car_terms = [
        UidCarTerm(uid=row["CAMPUS_ID"], career=row["ACAD_CAREER"], term=str(row["STRM"]))
        for row in rows
        if row.get("CAMPUS_ID") and row.get("ACAD_CAREER") and row.get("STRM") is not None
    ]
    return buids, uid_car_terms


async def _fetch_sap_buids(query: dict) -> list[str]:
    config = SAPApiResource.get_config()
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            config["url"],
            params=query.get("params") or None,
            json=query.get("json") or {},
            headers=config["headers"],
            timeout=30,
        )
        resp.raise_for_status()
        rows = resp.json().get("ET_EMP_LIST", [])
    return [row["BUID"] for row in rows if row.get("EMP_STATUS") == "3 - Active" and row.get("BUID")]


def _apply_semester_filter(pd: dict, allowed: set[tuple[str, str]]) -> None:
    si = pd.get("studentInfo")
    if not si or not isinstance(si, dict):
        return
    sems = si.get("studentSemester")
    if not sems:
        return
    si["studentSemester"] = [
        s for s in sems
        if (
            s.get("studentSemesterInfo", {}).get("academicCareer", {}).get("code", ""),
            s.get("studentSemesterInfo", {}).get("academicTerm", {}).get("term", {}).get("code", ""),
        ) in allowed
    ]


async def _stream_persons_ndjson(
    buids: list[str],
    uid_to_pairs: dict[str, set[tuple[str, str]]],
) -> AsyncGenerator[bytes, None]:
    """Fetches person records in batches of _DB_BATCH_SIZE with up to _DB_CONCURRENCY
    concurrent connections, yielding each record as an NDJSON line as batches complete."""
    batches = [buids[i:i + _DB_BATCH_SIZE] for i in range(0, len(buids), _DB_BATCH_SIZE)]
    sem = asyncio.Semaphore(_DB_CONCURRENCY)

    async def fetch_batch(batch: list[str]) -> list:
        async with sem:
            async with _pool.acquire() as conn:
                return await conn.fetch(
                    "SELECT bu_uid, person_data, modified_date"
                    " FROM person_xform.current_person_data"
                    " WHERE bu_uid = ANY($1::text[])",
                    batch,
                )

    for coro in asyncio.as_completed([fetch_batch(b) for b in batches]):
        rows = await coro
        for row in rows:
            pd = row["person_data"]
            if isinstance(pd, str):
                pd = json.loads(pd)
            uid = row["bu_uid"]
            if uid in uid_to_pairs:
                _apply_semester_filter(pd, uid_to_pairs[uid])
            record = {
                "bu_uid": uid,
                "person_data": pd,
                "modified_date": row["modified_date"].isoformat() if row["modified_date"] else None,
            }
            yield (json.dumps(record) + "\n").encode()


@app.post("/persons")
async def get_persons(request: PersonQueryRequest):
    all_buids: set[str] = set()
    all_uid_car_terms: list[UidCarTerm] = list(request.uidCarTerms or [])

    gather_tasks: list[tuple[str, Any]] = []
    if request.vds_query is not None:
        gather_tasks.append(("vds", _fetch_vds_buids(request.vds_query)))
    if request.ps_query is not None:
        gather_tasks.append(("ps", _fetch_ps_buids_and_terms(request.ps_query)))
    if request.sap_query is not None:
        gather_tasks.append(("sap", _fetch_sap_buids(request.sap_query)))

    if gather_tasks:
        results = await asyncio.gather(*[t[1] for t in gather_tasks], return_exceptions=True)
        for (label, _), result in zip(gather_tasks, results):
            if isinstance(result, Exception):
                raise HTTPException(
                    status_code=502,
                    detail=f"{label.upper()} query failed: {type(result).__name__}: {result}",
                )
            if label == "ps":
                ps_buids, ps_terms = result
                all_buids.update(ps_buids)
                all_uid_car_terms.extend(ps_terms)
            else:
                all_buids.update(result)

    if request.buids:
        all_buids.update(request.buids)

    # UIDs from uidCarTerms are also valid BUID sources — deduplicated before the DB query
    for uct in all_uid_car_terms:
        all_buids.add(uct.uid)

    if not all_buids:
        return StreamingResponse(iter([]), media_type="application/x-ndjson")

    uid_to_pairs: dict[str, set[tuple[str, str]]] = {}
    for uct in all_uid_car_terms:
        uid_to_pairs.setdefault(uct.uid, set()).add((uct.career, str(uct.term)))

    return StreamingResponse(
        _stream_persons_ndjson(list(all_buids), uid_to_pairs),
        media_type="application/x-ndjson",
    )
