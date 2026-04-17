# getPersonPersisted API

FastAPI service that fetches person records from `person_xform.current_person_data` by resolving BUIDs from upstream queries and/or a direct list, then filtering `studentSemester` data by uidCarTerms.

**Run:** `uvicorn getPersonPersisted:app --reload`
**Swagger UI:** `http://localhost:8000/docs`

---

## POST `/persons`

Returns a list of person records from `person_xform.current_person_data`.

BUIDs are collected from any combination of `vds_query`, `ps_query`, `sap_query`, and `buids`. All query sources run concurrently. Duplicate BUIDs across sources are deduplicated before the DB lookup.

### Request Body

```json
{
  "vds_query":   { ... },
  "ps_query":    { ... },
  "sap_query":   { ... },
  "buids":       ["U12345678", "U87654321"],
  "uidCarTerms": [
    { "uid": "U12345678", "career": "UGRD", "term": "2242" }
  ]
}
```

All fields are optional. At least one source of BUIDs must be provided, or an empty array is returned.

---

### Fields

#### `vds_query`
Calls the VDS API. Shape mirrors a `[[VDSQueries]]` TOML entry.

```json
{
  "json": {
    "params": "filter=(&(AffiliateAssignmentEndDate=*)...)"
  }
}
```

BUIDs are read from `entity.resources[].attributes.PersonBuid`.

---

#### `ps_query`
Calls the CS Tools (PeopleSoft) endpoint. Shape mirrors a `[[PSQueries]]` TOML entry.

```json
{
  "json": { "query_name": "BU_PARM_0216_QRY" }
}
```

```json
{
  "params": { "some_param": "value" },
  "json":   { "query_name": "BU_TERM_STD_FULL_TERM" }
}
```

BUIDs are read from `data[].CAMPUS_ID`.

**uidCarTerms are automatically derived from `ps_query` results.** Each row's `(CAMPUS_ID, ACAD_CAREER, STRM)` is converted into a uidCarTerm and merged with any manually-supplied `uidCarTerms`. This means `studentSemester` for PS-query persons will be filtered to only the terms returned by the query.

---

#### `sap_query`
Calls the SAP API. Shape mirrors a `[[SAPQueries]]` TOML entry.

```json
{
  "params": { "BAPIName": "Z_HR_EMPLOYEE_OBJ_LIST", "account": "HR" }
}
```

BUIDs are read from `ET_EMP_LIST[].BUID` where `EMP_STATUS == "3 - Active"`.

---

#### `buids`
Direct list of BUIDs to fetch, bypassing any upstream query.

```json
["U12345678", "U87654321"]
```

No uidCarTerms are derived from this source. All semesters are returned unless uidCarTerms are separately provided.

---

#### `uidCarTerms`
Manually supply (uid, career, term) tuples to filter `studentInfo.studentSemester`.

```json
[
  { "uid": "U12345678", "career": "UGRD", "term": "2242" },
  { "uid": "U12345678", "career": "GRAD", "term": "2252" },
  { "uid": "U87654321", "career": "UGRD", "term": "2242" }
]
```

These are merged with any uidCarTerms derived from `ps_query`.

---

### uidCarTerms Filtering

After fetching person records from the DB, `studentInfo.studentSemester` is filtered in Python:

- A person whose `bu_uid` **appears** in the uidCarTerms list has their `studentSemester` filtered to only entries where `studentSemesterInfo.academicCareer` and `studentSemesterInfo.academicTerm` match one of their allowed (career, term) pairs.
- A person whose `bu_uid` **does not appear** in the uidCarTerms list is returned with all semesters intact.

---

### Response

Array of objects, one per matched person:

```json
[
  {
    "bu_uid": "U12345678",
    "modified_date": "2026-04-15T10:23:00.000000",
    "person_data": {
      "personid": "U12345678",
      "personBasic": { ... },
      "email": [ ... ],
      "phone": [ ... ],
      "studentInfo": {
        "studentSemester": [ ... ],
        "academicProgress": { ... },
        "address": [ ... ]
      },
      "employeeInfo": { ... },
      "affiliateInfo": { ... }
    }
  }
]
```

Returns `[]` if no BUIDs are resolved or no matching records exist in the DB.

---

### Errors

| Status | Cause |
|--------|-------|
| `200` | Success (may be empty array) |
| `502` | Upstream query failed (VDS, PS, or SAP) — detail includes which source and the exception |
