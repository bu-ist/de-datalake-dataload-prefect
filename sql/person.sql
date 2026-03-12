/*===================================================================================
=                               RAW LAYER TABLES                                    =
===================================================================================*/

-- Raw person events — append-only unless filtered by trigger
CREATE TABLE person_raw.person_data (
    bu_uid          VARCHAR NOT NULL,
    person_data     JSONB   NOT NULL,
    dl_insert_date  TIMESTAMP DEFAULT now() NOT NULL
);

CREATE INDEX person_data_bu_uid_idx
    ON person_raw.person_data (bu_uid, dl_insert_date DESC);



/*===================================================================================
=                           TRANSFORM / CURRENT PERSON TABLE                         =
===================================================================================*/

-- Latest canonical record for each person
CREATE TABLE person_xform.current_person_data (
    bu_uid         VARCHAR PRIMARY KEY,
    person_data    JSONB NOT NULL,
    modified_date  TIMESTAMP  DEFAULT now() NOT NULL
);



/*===================================================================================
=                              CURATED TABLES BY SERVICE                             =
===================================================================================*/

-- Per-service filtered views of person data
CREATE TABLE person_curated.person_data_by_service (
    service       VARCHAR NOT NULL,
    bu_uid        VARCHAR NOT NULL,
    person_data   JSONB   NOT NULL,
    modified_date TIMESTAMP  DEFAULT now() NOT NULL,
    PRIMARY KEY (bu_uid, service)
);

-- Defines what JSON paths are allowed for each service
CREATE TABLE person_curated.person_schema_service_filters (
    service VARCHAR PRIMARY KEY,
    paths   TEXT[] NOT NULL
);



/*===================================================================================
=                              JSON CANONICALIZATION                                 =
===================================================================================*/

-- Produces a canonical JSON form (stable ordering, recursively normalized)
CREATE OR REPLACE FUNCTION person_raw.jsonb_canonical(in_json JSONB)
RETURNS JSONB
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
WITH RECURSIVE norm(j) AS (
    SELECT
        CASE jsonb_typeof(in_json)
            -- Canonicalize objects: sort keys recursively
            WHEN 'object' THEN (
                SELECT jsonb_object_agg(
                    key,
                    CASE
                        WHEN jsonb_typeof(value) IN ('object','array') THEN person_raw.jsonb_canonical(value)
                        ELSE value
                    END
                )
                FROM (
                    SELECT key, value
                    FROM jsonb_each(in_json)
                    ORDER BY key
                ) obj
            )
            -- Canonicalize arrays: recursively canonicalize elements, sort by their canonical text
            WHEN 'array' THEN (
                SELECT jsonb_agg(canon_elem)
                FROM (
                    SELECT canon_elem
                    FROM (
                        SELECT CASE
                                   WHEN jsonb_typeof(value) IN ('object','array') THEN person_raw.jsonb_canonical(value)
                                   ELSE value
                               END AS canon_elem
                        FROM jsonb_array_elements(in_json) AS t(value)
                    ) a
                    ORDER BY canon_elem::text
                ) sorted
            )
            -- Scalars remain unchanged
            ELSE in_json
        END
)
SELECT j FROM norm;
$$;



/*===================================================================================
=                         TRIGGER: PREVENT DUPLICATE RAW ROWS                        =
===================================================================================*/

-- Prevents inserting a raw record if canonical JSON is unchanged
CREATE OR REPLACE FUNCTION person_raw.person_data_upsert_prevent_duplicates()
RETURNS TRIGGER AS $$
DECLARE
    existing_person_data JSONB;
    new_canon       JSONB;
    existing_canon  JSONB;
BEGIN
    SELECT person_data
    INTO existing_person_data
    FROM person_raw.person_data
    WHERE bu_uid = NEW.bu_uid
    ORDER BY dl_insert_date DESC
    LIMIT 1;

    IF existing_person_data IS NOT NULL THEN
        -- Fast path: if the JSONB is byte-for-byte equal, skip canonicalization
        IF NEW.person_data = existing_person_data THEN
            RETURN NULL;
        END IF;

        new_canon      := person_raw.jsonb_canonical(NEW.person_data);
        existing_canon := person_raw.jsonb_canonical(existing_person_data);

        -- Skip insert entirely if canonical forms match
        IF new_canon = existing_canon THEN
            RETURN NULL;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_duplicate_person_data
    ON person_raw.person_data;

CREATE TRIGGER trg_prevent_duplicate_person_data
BEFORE INSERT ON person_raw.person_data
FOR EACH ROW
EXECUTE FUNCTION person_raw.person_data_upsert_prevent_duplicates();



/*===================================================================================
=                  TRIGGER: UPSERT CURRENT PERSON RECORD (LATEST ONLY)              =
===================================================================================*/

CREATE OR REPLACE FUNCTION person_raw.upsert_current_person_data()
RETURNS TRIGGER
LANGUAGE plpgsql AS $function$
BEGIN
    INSERT INTO person_xform.current_person_data (bu_uid, person_data, modified_date)
    VALUES (NEW.bu_uid, NEW.person_data, NOW())
    ON CONFLICT (bu_uid)
    DO UPDATE SET
        person_data   = EXCLUDED.person_data,
        modified_date = NOW();

    RETURN NEW;
END;
$function$;

DROP TRIGGER IF EXISTS trg_upsert_current_person_data
    ON person_raw.person_data;

CREATE TRIGGER trg_upsert_current_person_data
AFTER INSERT ON person_raw.person_data
FOR EACH ROW
EXECUTE FUNCTION person_raw.upsert_current_person_data();



/*===================================================================================
=           TRIGGER: UPSERT PER-SERVICE CURATED PERSON DATA FROM CURRENT            =
===================================================================================*/

CREATE OR REPLACE FUNCTION person_xform.upsert_all_person_data_by_service()
RETURNS TRIGGER
LANGUAGE plpgsql AS $function$
DECLARE
    v_service    TEXT;
    v_paths        TEXT[];
    allowed_tree JSONB;
    result       JSONB;
    other        JSONB;
    rec          RECORD;
BEGIN
    --------------------------------------------------------------------
    -- Remove stale services for this bu_uid
    --------------------------------------------------------------------
    DELETE FROM person_curated.person_data_by_service
    WHERE bu_uid = NEW.bu_uid;

    --------------------------------------------------------------------
    -- Insert full record if it has meaningful data beyond personid
    --------------------------------------------------------------------
    IF NEW.person_data IS NOT NULL
    AND jsonb_typeof(NEW.person_data) = 'object'
    AND (NEW.person_data ? 'personid')
    THEN
        other := NEW.person_data - 'personid';

        IF person_xform.jsonb_has_meaningful_data(other) THEN
            INSERT INTO person_curated.person_data_by_service (
                bu_uid, service, person_data, modified_date
            )
            VALUES (
                NEW.bu_uid, 'full', NEW.person_data, NEW.modified_date
            )
            ON CONFLICT (bu_uid, service)
            DO UPDATE
            SET person_data  = EXCLUDED.person_data,
                modified_date = EXCLUDED.modified_date;
        END IF;
    END IF;

    --------------------------------------------------------------------
    -- Process each configured service filter
    --------------------------------------------------------------------
    FOR rec IN
        SELECT psf.service, psf.paths
        FROM person_curated.person_schema_service_filters AS psf
        ORDER BY psf.service
    LOOP
        v_service := rec.service;
        v_paths   := rec.paths;

        IF v_paths IS NULL OR array_length(v_paths, 1) IS NULL THEN
            CONTINUE;
        END IF;

        allowed_tree := person_xform.build_allowed_tree_from_paths(v_paths);
        result       := person_xform.jsonb_filter_tree(NEW.person_data, allowed_tree);

        IF result IS NULL THEN
            CONTINUE;
        END IF;

        other := result - 'personid';

        IF NOT person_xform.jsonb_has_meaningful_data(other) THEN
            CONTINUE;
        END IF;

        INSERT INTO person_curated.person_data_by_service (
            bu_uid, service, person_data, modified_date
        )
        VALUES (
            NEW.bu_uid, v_service, result, NEW.modified_date
        )
        ON CONFLICT (bu_uid, service)
        DO UPDATE
        SET person_data  = EXCLUDED.person_data,
            modified_date = EXCLUDED.modified_date;
    END LOOP;

    RETURN NEW;
END;
$function$;

DROP TRIGGER IF EXISTS trg_upsert_person_data_by_service
    ON person_xform.current_person_data;

CREATE TRIGGER trg_upsert_person_data_by_service
AFTER INSERT OR UPDATE ON person_xform.current_person_data
FOR EACH ROW
EXECUTE FUNCTION person_xform.upsert_all_person_data_by_service();



/*===================================================================================
=                                JSON FILTERING LOGIC                                =
===================================================================================*/

-- Filters a JSON tree by an allowlist tree structure
CREATE OR REPLACE FUNCTION person_xform.jsonb_filter_tree(data JSONB, allowed_tree JSONB)
RETURNS JSONB
LANGUAGE plpgsql AS $function$
DECLARE
    col_key        TEXT;
    col_allowed    JSONB;
    filtered       JSONB := '{}'::jsonb;
    arr_elem       JSONB;
    filtered_list  JSONB := '[]'::jsonb;
    tmp            JSONB;
BEGIN
    -- Leaf rule: include subtree unless NULL
    IF allowed_tree = '{}'::jsonb THEN
        IF data IS NULL OR jsonb_typeof(data) = 'null' THEN
            RETURN NULL;
        END IF;
        RETURN data;
    END IF;
    --------------------------------------------------------------------
    -- Object filtering
    --------------------------------------------------------------------
    IF jsonb_typeof(data) = 'object' THEN
        FOR col_key, col_allowed IN
            SELECT key, value FROM jsonb_each(allowed_tree)
        LOOP
            IF data ? col_key THEN
                tmp := person_xform.jsonb_filter_tree(data -> col_key, col_allowed);

                IF tmp IS NOT NULL AND jsonb_typeof(tmp) <> 'null' THEN
                    filtered := jsonb_set(filtered, ARRAY[col_key], tmp, true);
                END IF;
            END IF;
        END LOOP;

        RETURN filtered;

    --------------------------------------------------------------------
    -- Array filtering
    --------------------------------------------------------------------
    ELSIF jsonb_typeof(data) = 'array' THEN
        FOR arr_elem IN SELECT value FROM jsonb_array_elements(data) LOOP
            IF jsonb_typeof(arr_elem) IN ('object', 'array') THEN
                tmp := person_xform.jsonb_filter_tree(arr_elem, allowed_tree);

                IF tmp IS NOT NULL AND jsonb_typeof(tmp) <> 'null' THEN
                    filtered_list := filtered_list || jsonb_build_array(tmp);
                END IF;
            END IF;
        END LOOP;

        RETURN filtered_list;
    --------------------------------------------------------------------
    -- Primitives not valid under structured allowances
    --------------------------------------------------------------------
    ELSE
        RETURN NULL;
    END IF;
END;
$function$;



/*===================================================================================
=                             JSON DEEP MERGE UTILITY                                =
===================================================================================*/

-- Deep merge JSON objects recursively
CREATE OR REPLACE FUNCTION person_xform.jsonb_deep_merge(a JSONB, b JSONB)
RETURNS JSONB
LANGUAGE sql AS $function$
SELECT CASE
    WHEN jsonb_typeof(a) <> 'object' OR jsonb_typeof(b) <> 'object' THEN b
    ELSE (
        SELECT jsonb_object_agg(
            key,
            CASE
                WHEN a -> key IS NULL THEN b -> key
                WHEN b -> key IS NULL THEN a -> key
                ELSE person_xform.jsonb_deep_merge(a -> key, b -> key)
            END
        )
        FROM (
            SELECT key FROM jsonb_each(a)
            UNION
            SELECT key FROM jsonb_each(b)
        ) t
    )
END;
$function$;



/*===================================================================================
=                          BUILD ALLOWLIST TREE FROM PATHS                           =
===================================================================================*/

-- Converts array of text paths into a nested JSON allowlist tree
CREATE OR REPLACE FUNCTION person_xform.build_allowed_tree_from_paths(paths TEXT[])
RETURNS JSONB
LANGUAGE plpgsql AS $function$
DECLARE
    tree JSONB := '{}'::jsonb;
    p TEXT;
    pathlist TEXT[];
    n INT;
    i INT;
    current JSONB;
    name_level TEXT;
BEGIN
    FOREACH p IN ARRAY paths LOOP
        pathlist := string_to_array(ltrim(p, '/'), '/');
        n := array_length(pathlist, 1);

        IF n IS NULL OR n = 0 THEN CONTINUE; END IF;

        current := '{}'::jsonb;

        FOR i IN REVERSE n .. 1 LOOP
            name_level := pathlist[i];

            IF current = '{}'::jsonb THEN
                current := jsonb_build_object(name_level, '{}'::jsonb);
            ELSE
                current := jsonb_build_object(name_level, current);
            END IF;
        END LOOP;

        tree := person_xform.jsonb_deep_merge(tree, current);
    END LOOP;

    RETURN tree;
END;
$function$;



/*===================================================================================
=                           CHECK FOR MEANINGFUL JSON DATA                           =
===================================================================================*/

CREATE OR REPLACE FUNCTION person_xform.jsonb_has_meaningful_data(data JSONB)
RETURNS BOOLEAN
LANGUAGE plpgsql AS $function$
DECLARE
    k TEXT;
    v JSONB;
BEGIN
    -- Empty values are not meaningful
    IF data IS NULL
        OR data = '{}'::jsonb
        OR data = '[]'::jsonb
        OR jsonb_typeof(data) = 'null'
    THEN
        RETURN FALSE;
    END IF;

    CASE jsonb_typeof(data)
        WHEN 'object' THEN
            FOR k, v IN SELECT key, value FROM jsonb_each(data) LOOP
                IF person_xform.jsonb_has_meaningful_data(v) THEN RETURN TRUE; END IF;
            END LOOP;
            RETURN FALSE;

        WHEN 'array' THEN
            FOR v IN SELECT value FROM jsonb_array_elements(data) LOOP
                IF person_xform.jsonb_has_meaningful_data(v) THEN RETURN TRUE; END IF;
            END LOOP;
            RETURN FALSE;

        ELSE
            RETURN TRUE;  -- Any scalar value is meaningful
    END CASE;
END;
$function$;



/*===================================================================================
=                                   SCHEMA FILTERS                                  =
===================================================================================*/

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('student', ARRAY[
    '/personid',
    '/personBasic/names',
    '/personBasic/birthDate',
    '/personDetails/emergencyContact',
    '/email/type',
    '/email/address',
    '/email/isPreferred',
    '/phone/type',
    '/phone/number',
    '/phone/isPreferred',
    '/studentInfo/address',
    '/studentInfo/academicProgress',
    '/studentInfo/studentSemester/studentSemesterInfo/registrationStatus',
    '/studentInfo/studentSemester/studentSemesterInfo/withdrawalStatus',
    '/studentInfo/studentSemester/studentSemesterInfo/currentClassYear',
    '/studentInfo/studentSemester/studentSemesterInfo/certification',
    '/studentInfo/studentSemester/studentSemesterInfo/certificationApproved',
    '/studentInfo/studentSemester/studentSemesterInfo/projectedGraduationDate'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('studentDegreeProgram', ARRAY[
    '/personid',
    '/studentInfo/studentSemester/studentSemesterInfo/degreeProgram/academicCareer',
    '/studentInfo/studentSemester/studentSemesterInfo/degreeProgram/academicGroup',
    '/studentInfo/studentSemester/studentSemesterInfo/degreeProgram/academicOrganization',
    '/studentInfo/studentSemester/studentSemesterInfo/degreeProgram/academicPlan',
    '/studentInfo/studentSemester/studentSemesterInfo/degreeProgram/college/description',
    '/studentInfo/studentSemester/studentSemesterInfo/degreeProgram/program/description',
    '/studentInfo/studentSemester/studentSemesterInfo/degreeProgram/degree/description'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('studentClass', ARRAY[
    '/personid',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/course',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/section',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/enrollmentStatus/description',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/campus/description',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/details/instructionType/description',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/details/instructor/firstName',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/details/instructor/lastName',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/details/instructor/email',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/details/location',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/details/calendar'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('studentGeneric', ARRAY[
    '/personid',
    '/personBasic/names',
    '/personBasic/birthDate',
    '/personDetails/emergencyContact',
    '/email/type',
    '/email/address',
    '/email/isPreferred',
    '/phone/type',
    '/phone/number',
    '/phone/isPreferred',
    '/studentInfo/studentSemester/studentSemesterInfo/academicTerm',
    '/studentInfo/studentSemester/studentSemesterInfo/academicCareer',
    '/studentInfo/studentSemester/studentSemesterInfo/degreeProgram/academicCareer',
    '/studentInfo/studentSemester/studentSemesterInfo/degreeProgram/academicGroup',
    '/studentInfo/studentSemester/studentSemesterInfo/degreeProgram/academicOrganization',
    '/studentInfo/studentSemester/studentSemesterInfo/degreeProgram/academicPlan',
    '/studentInfo/studentSemester/studentSemesterInfo/degreeProgram/college/description',
    '/studentInfo/studentSemester/studentSemesterInfo/degreeProgram/program/description',
    '/studentInfo/studentSemester/studentSemesterInfo/degreeProgram/degree/description',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/course',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/section',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/enrollmentStatus/description',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/campus/description',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/details/instructionType/description',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/details/instructor/firstName',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/details/instructor/lastName',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/details/instructor/email',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/details/location',
    '/studentInfo/studentSemester/studentSemesterInfo/studentClass/details/calendar'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('studentGPA', ARRAY[
    '/personid',
    '/studentInfo/studentSemester/studentSemesterInfo/academicTerm',
    '/studentInfo/studentSemester/studentSemesterInfo/academicCareer',
    '/studentInfo/studentSemester/studentSemesterInfo/currentGpa',
    '/studentInfo/studentSemester/studentSemesterInfo/cumulativeGpa',
    '/studentInfo/studentSemester/studentSemesterInfo/unitsTermTotal',
    '/studentInfo/studentSemester/studentSemesterInfo/unitsCumulative'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('studentTestScores', ARRAY[
    '/personid',
    '/studentInfo/testResults'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('studentAdvisor', ARRAY[
    '/personid',
    '/studentInfo/advisor'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('studentGroups', ARRAY[
    '/personid',
    '/personDetails/group'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('bioDemo', ARRAY[
    '/personid',
    '/personBasic/names',
    '/personBasic/birthDate',
    '/personBasic/pronouns'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('studentContactInfo', ARRAY[
    '/personid',
    '/email/type',
    '/email/address',
    '/email/isPreferred',
    '/phone/type',
    '/phone/number',
    '/phone/isPreferred',
    '/studentInfo/address'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('emergencyContact', ARRAY[
    '/personid',
    '/personDetails/emergencyContact'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('studentGeneralInfo', ARRAY[
    '/personid',
    '/personBasic/names',
    '/personBasic/birthDate',
    '/personBasic/pronouns',
    '/email/type',
    '/email/address',
    '/email/isPreferred',
    '/phone/type',
    '/phone/number',
    '/phone/isPreferred',
    '/studentInfo/address',
    '/personDetails/emergencyContact'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('studentAdmissionHistory', ARRAY[
    '/personid',
    '/studentInfo/admissionHistory'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('employeeContactInfo', ARRAY[
    '/personid',
    '/email/type',
    '/email/address',
    '/email/isPreferred',
    '/phone/type',
    '/phone/number',
    '/phone/isPreferred',
    '/employeeInfo/address',
    '/employeeInfo/positions/positionInfo/office'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('employeeGeneric', ARRAY[
    '/personid',
    '/email/type',
    '/email/address',
    '/email/isPreferred',
    '/phone/type',
    '/phone/number',
    '/phone/isPreferred',
    '/employeeInfo/address',
    '/employeeInfo/positions/positionInfo/basicData'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('employeeDepartment', ARRAY[
    '/personid',
    '/email/type',
    '/email/address',
    '/email/isPreferred',
    '/phone/type',
    '/phone/number',
    '/phone/isPreferred',
    '/employeeInfo/positions/positionInfo/department/unit',
    '/employeeInfo/positions/positionInfo/department/department',
    '/employeeInfo/positions/positionInfo/department/departmentName',
    '/employeeInfo/positions/positionInfo/department/organizationalUnit',
    '/employeeInfo/positions/positionInfo/department/organizationalUnitDescription'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('employeeAcademic', ARRAY[
    '/personid',
    '/email/type',
    '/email/address',
    '/email/isPreferred',
    '/phone/type',
    '/phone/number',
    '/phone/isPreferred',
    '/employeeInfo/positions/positionInfo/academic'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('employeeSupervisor', ARRAY[
    '/personid',
    '/email/type',
    '/email/address',
    '/email/isPreferred',
    '/phone/type',
    '/phone/number',
    '/phone/isPreferred',
    '/employeeInfo/positions/positionInfo/supervisor'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('affiliateContactInfo', ARRAY[
    '/personid',
    '/email/type',
    '/email/address',
    '/email/isPreferred',
    '/phone/type',
    '/phone/number',
    '/phone/isPreferred',
    '/affiliateInfo/address'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('affiliateGeneric', ARRAY[
    '/personid',
    '/email/type',
    '/email/address',
    '/email/isPreferred',
    '/phone/type',
    '/phone/number',
    '/phone/isPreferred',
    '/affiliateInfo/address',
    '/affiliateInfo/affiliateType',
    '/affiliateInfo/affiliateAssignment',
    '/affiliateInfo/assignmentBeginDate',
    '/affiliateInfo/assignmentEndDate',
    '/affiliateInfo/personalArea',
    '/affiliateInfo/personalSubArea'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('affiliateDepartment', ARRAY[
    '/personid',
    '/email/type',
    '/email/address',
    '/email/isPreferred',
    '/phone/type',
    '/phone/number',
    '/phone/isPreferred',
    '/affiliateInfo/address',
    '/affiliateInfo/department',
    '/affiliateInfo/unit',
    '/affiliateInfo/organizationalUnit',
    '/affiliateInfo/organizationalUnitParent'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO person_curated.person_schema_service_filters (service, paths) VALUES
('affiliateSupervisor', ARRAY[
    '/personid',
    '/email/type',
    '/email/address',
    '/email/isPreferred',
    '/phone/type',
    '/phone/number',
    '/phone/isPreferred',
    '/affiliateInfo/address',
    '/affiliateInfo/supervisorID',
    '/affiliateInfo/sponsorID'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;