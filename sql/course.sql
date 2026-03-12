/*===================================================================================
=                               RAW LAYER TABLES                                    =
===================================================================================*/

CREATE TABLE course_raw.course_data (
    academic_career VARCHAR(4) NOT NULL,
    term_code       INT4       NOT NULL,
    course_id       INT4       NOT NULL,
    session_code    VARCHAR(3) NOT NULL,
    course_data     JSONB      NOT NULL,
    dl_insert_date  TIMESTAMP  DEFAULT now() NOT NULL
);

CREATE INDEX idx_course_data_acad_term_course_session_insertdate
    ON course_raw.course_data (
        academic_career,
        term_code,
        course_id,
        session_code,
        dl_insert_date DESC
    );



/*===================================================================================
=                           TRANSFORM / CURRENT-LATEST TABLE                         =
===================================================================================*/

CREATE TABLE course_xform.current_course_data (
    academic_career VARCHAR(4) NOT NULL,
    term_code       INT4       NOT NULL,
    course_id       INT4       NOT NULL,
    session_code    VARCHAR(3) NOT NULL,
    course_data     JSONB      NOT NULL,
    modified_date   TIMESTAMP  DEFAULT now() NOT NULL,
    PRIMARY KEY (academic_career, term_code, course_id, session_code)
);



/*===================================================================================
=                              CURATED TABLES BY SERVICE                             =
===================================================================================*/

CREATE TABLE course_curated.course_data_by_service (
    service         VARCHAR     NOT NULL,
    academic_career VARCHAR(4) NOT NULL,
    term_code       INT4       NOT NULL,
    course_id       INT4       NOT NULL,
    session_code    VARCHAR(3) NOT NULL,
    course_data     JSONB      NOT NULL,
    modified_date   TIMESTAMP  DEFAULT now() NOT NULL,
    PRIMARY KEY (service, academic_career, term_code, course_id, session_code)
);

-- Defines JSON paths to keep for each service
CREATE TABLE course_curated.course_schema_service_filters (
    service VARCHAR PRIMARY KEY,
    paths   TEXT[] NOT NULL
);



/*===================================================================================
=                              JSON CANONICALIZATION                                 =
===================================================================================*/

CREATE OR REPLACE FUNCTION course_raw.jsonb_canonical(in_json JSONB)
RETURNS JSONB
LANGUAGE sql IMMUTABLE AS $$
WITH RECURSIVE norm(j) AS (
    SELECT
        CASE jsonb_typeof(in_json)
            -- Canonicalize objects: sort keys recursively
            WHEN 'object' THEN (
                SELECT jsonb_object_agg(key, course_raw.jsonb_canonical(value))
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
                    SELECT course_raw.jsonb_canonical(value) AS canon_elem
                    FROM jsonb_array_elements(in_json) AS t(value)
                    ORDER BY course_raw.jsonb_canonical(value)::text
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

CREATE OR REPLACE FUNCTION course_raw.course_data_upsert_prevent_duplicates()
RETURNS TRIGGER AS $$
DECLARE
    existing_record course_raw.course_data%ROWTYPE;
    new_canon       JSONB;
    existing_canon  JSONB;
BEGIN
    SELECT *
    INTO existing_record
    FROM course_raw.course_data
    WHERE academic_career = NEW.academic_career
      AND term_code       = NEW.term_code
      AND course_id       = NEW.course_id
      AND session_code    = NEW.session_code
    ORDER BY dl_insert_date DESC
    LIMIT 1;

    IF existing_record IS NOT NULL THEN
        new_canon      := course_raw.jsonb_canonical(NEW.course_data);
        existing_canon := course_raw.jsonb_canonical(existing_record.course_data);

        IF new_canon = existing_canon THEN
            RETURN NULL; -- Skip insert
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_duplicate_course_data
    ON course_raw.course_data;

CREATE TRIGGER trg_prevent_duplicate_course_data
BEFORE INSERT ON course_raw.course_data
FOR EACH ROW EXECUTE FUNCTION course_raw.course_data_upsert_prevent_duplicates();



/*===================================================================================
=                  TRIGGER: UPSERT CURRENT COURSE RECORD (LATEST ONLY)              =
===================================================================================*/

CREATE OR REPLACE FUNCTION course_raw.upsert_current_course_data()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO course_xform.current_course_data (
        academic_career,
        term_code,
        course_id,
        session_code,
        course_data,
        modified_date
    )
    VALUES (
        NEW.academic_career,
        NEW.term_code,
        NEW.course_id,
        NEW.session_code,
        NEW.course_data,
        now()
    )
    ON CONFLICT (academic_career, term_code, course_id, session_code)
    DO UPDATE SET
        course_data   = EXCLUDED.course_data,
        modified_date = now();

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_upsert_current_course_data
    ON course_raw.course_data;

CREATE TRIGGER trg_upsert_current_course_data
AFTER INSERT ON course_raw.course_data
FOR EACH ROW EXECUTE FUNCTION course_raw.upsert_current_course_data();



/*===================================================================================
=                       UTILITY: JSON DEEP MERGE (REUSED FOR PATH BUILDING)         =
===================================================================================*/

CREATE OR REPLACE FUNCTION course_xform.jsonb_deep_merge(a JSONB, b JSONB)
RETURNS JSONB LANGUAGE sql AS $function$
SELECT CASE
    WHEN jsonb_typeof(a) <> 'object'
      OR jsonb_typeof(b) <> 'object'
    THEN b
    ELSE (
        SELECT jsonb_object_agg(
            key,
            CASE
                WHEN a -> key IS NULL THEN b -> key
                WHEN b -> key IS NULL THEN a -> key
                ELSE course_xform.jsonb_deep_merge(a -> key, b -> key)
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
=                       BUILD ALLOWLIST TREE FROM SERVICE PATHS                     =
===================================================================================*/

CREATE OR REPLACE FUNCTION course_xform.build_allowed_tree_from_paths(paths TEXT[])
RETURNS JSONB LANGUAGE plpgsql AS $function$
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

        tree := course_xform.jsonb_deep_merge(tree, current);
    END LOOP;

    RETURN tree;
END;
$function$;



/*===================================================================================
=                           MEANINGFUL-DATA CHECK FOR COURSE                        =
===================================================================================*/

-- Nested identity keys to ignore:
--   termDetails.academicCareer
--   termDetails.term.code
--   v2.courseId
--   v2.sessionCode

CREATE OR REPLACE FUNCTION course_xform.jsonb_has_meaningful_data(data JSONB)
RETURNS BOOLEAN LANGUAGE plpgsql AS $function$
DECLARE
    k TEXT;
    v JSONB;
    path TEXT[];
BEGIN
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

                -- Ignore nested identity fields
                IF (k = 'termDetails' AND v ? 'academicCareer')
                    OR (k = 'termDetails' AND (v -> 'term') ? 'code')
                    OR (k = 'v2' AND (v ? 'courseId' OR v ? 'sessionCode'))
                THEN
                    CONTINUE;
                END IF;

                IF course_xform.jsonb_has_meaningful_data(v) THEN
                    RETURN TRUE;
                END IF;
            END LOOP;
            RETURN FALSE;

        WHEN 'array' THEN
            FOR v IN SELECT value FROM jsonb_array_elements(data) LOOP
                IF course_xform.jsonb_has_meaningful_data(v) THEN
                    RETURN TRUE;
                END IF;
            END LOOP;
            RETURN FALSE;

        ELSE
            RETURN TRUE; -- Primitive
    END CASE;
END;
$function$;



/*===================================================================================
=                                JSON FILTER TREE                                    =
===================================================================================*/

CREATE OR REPLACE FUNCTION course_xform.jsonb_filter_tree(data JSONB, allowed_tree JSONB)
RETURNS JSONB LANGUAGE plpgsql AS $function$
DECLARE
    col_key TEXT;
    col_allowed JSONB;
    filtered JSONB := '{}'::jsonb;
    arr_elem JSONB;
    tmp JSONB;
    filtered_list JSONB := '[]'::jsonb;
BEGIN
    -- Leaf: keep entire subtree if not null
    IF allowed_tree = '{}'::jsonb THEN
        IF data IS NULL OR jsonb_typeof(data) = 'null' THEN RETURN NULL; END IF;
        RETURN data;
    END IF;

    IF jsonb_typeof(data) = 'object' THEN
        FOR col_key, col_allowed IN SELECT key, value FROM jsonb_each(allowed_tree) LOOP
            IF data ? col_key THEN
                tmp := course_xform.jsonb_filter_tree(data -> col_key, col_allowed);
                IF tmp IS NOT NULL AND jsonb_typeof(tmp) <> 'null' THEN
                    filtered := jsonb_set(filtered, ARRAY[col_key], tmp, true);
                END IF;
            END IF;
        END LOOP;

        RETURN filtered;

    ELSIF jsonb_typeof(data) = 'array' THEN
        FOR arr_elem IN SELECT value FROM jsonb_array_elements(data) LOOP
            tmp := course_xform.jsonb_filter_tree(arr_elem, allowed_tree);
            IF tmp IS NOT NULL AND jsonb_typeof(tmp) <> 'null' THEN
                filtered_list := filtered_list || jsonb_build_array(tmp);
            END IF;
        END LOOP;

        RETURN filtered_list;

    ELSE
        RETURN NULL;
    END IF;
END;
$function$;



/*===================================================================================
=                   UPSERT COURSE DATA BY SERVICE (CURATED LAYER)                   =
===================================================================================*/

CREATE OR REPLACE FUNCTION course_xform.upsert_all_course_data_by_service()
RETURNS TRIGGER LANGUAGE plpgsql AS $function$
DECLARE
    rec RECORD;
    v_service TEXT;
    svc_paths TEXT[];
    allowed_tree JSONB;
    result JSONB;
    stripped JSONB;
BEGIN
    --------------------------------------------------------------------
    -- Remove outdated curated records for this course
    --------------------------------------------------------------------
    DELETE FROM course_curated.course_data_by_service
    WHERE academic_career = NEW.academic_career
      AND term_code       = NEW.term_code
      AND course_id       = NEW.course_id
      AND session_code    = NEW.session_code;

    --------------------------------------------------------------------
    -- FULL service
    --------------------------------------------------------------------
    stripped := NEW.course_data;

    -- Remove nested identity fields before checking meaningfulness
    stripped :=
        stripped
            #- '{termDetails,academicCareer}'
            #- '{termDetails,term,code}'
            #- '{v2,courseId}'
            #- '{v2,sessionCode}';

    IF course_xform.jsonb_has_meaningful_data(stripped) THEN
        INSERT INTO course_curated.course_data_by_service (
            service,
            academic_career,
            term_code,
            course_id,
            session_code,
            course_data,
            modified_date
        )
        VALUES (
            'full',
            NEW.academic_career,
            NEW.term_code,
            NEW.course_id,
            NEW.session_code,
            NEW.course_data,
            NEW.modified_date
        )
        ON CONFLICT (service, academic_career, term_code, course_id, session_code)
        DO UPDATE SET
            course_data  = EXCLUDED.course_data,
            modified_date = EXCLUDED.modified_date;
    END IF;

    --------------------------------------------------------------------
    -- SERVICE-SPECIFIC FILTERING
    --------------------------------------------------------------------
    FOR rec IN
    SELECT service, paths
    FROM course_curated.course_schema_service_filters
    ORDER BY service
    LOOP
        v_service := rec.service;
        svc_paths := rec.paths;

        IF array_length(svc_paths, 1) IS NULL THEN CONTINUE; END IF;

        allowed_tree := course_xform.build_allowed_tree_from_paths(svc_paths);
        result       := course_xform.jsonb_filter_tree(NEW.course_data, allowed_tree);

        IF result IS NULL THEN CONTINUE; END IF;

        stripped :=
            result
                #- '{termDetails,academicCareer}'
                #- '{termDetails,term,code}'
                #- '{v2,courseId}'
                #- '{v2,sessionCode}';

        IF NOT course_xform.jsonb_has_meaningful_data(stripped) THEN CONTINUE; END IF;

        INSERT INTO course_curated.course_data_by_service (
            service,
            academic_career,
            term_code,
            course_id,
            session_code,
            course_data,
            modified_date
        )
        VALUES (
            v_service,
            NEW.academic_career,
            NEW.term_code,
            NEW.course_id,
            NEW.session_code,
            result,
            NEW.modified_date
        )
        ON CONFLICT (service, academic_career, term_code, course_id, session_code)
        DO UPDATE SET
            course_data  = EXCLUDED.course_data,
            modified_date = EXCLUDED.modified_date;
    END LOOP;

    RETURN NEW;
END;
$function$;

DROP TRIGGER IF EXISTS trg_upsert_course_data_by_service
    ON course_xform.current_course_data;

CREATE TRIGGER trg_upsert_course_data_by_service
AFTER INSERT OR UPDATE ON course_xform.current_course_data
FOR EACH ROW EXECUTE FUNCTION course_xform.upsert_all_course_data_by_service();



/*===================================================================================
=                                   SCHEMA FILTERS                                  =
===================================================================================*/

INSERT INTO course_curated.course_schema_service_filters (service, paths) VALUES
('courseCatalog', ARRAY[
    '/term_Details/academicCareer',
    '/term_Details/term',
    '/courses/v2',
    '/courses/college',
    '/courses/department',
    '/courses/number',
    '/courses/title',
    '/courses/titleLong',
    '/courses/notes',
    '/courses/enrollmentLlimit',
    '/courses/courseLevel',
    '/courses/startDate',
    '/courses/endDate'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO course_curated.course_schema_service_filters (service, paths) VALUES
('courseSections', ARRAY[
    '/term_Details/academicCareer',
    '/term_Details/term',
    '/courses/v2',
    '/courses/college',
    '/courses/department',
    '/courses/number',
    '/courses/title',
    '/courses/titleLong',
    '/courses/sections/section',
    '/courses/sections/classNumber',
    '/courses/sections/startDate',
    '/courses/sections/endDate',
    '/courses/sections/enrollmentCap',
    '/courses/sections/enrollmentTotal',
    '/courses/sections/lastEnrollmentDate',
    '/courses/sections/lastDropDateWithPenalty',
    '/courses/sections/lastDropDateWithoutPenalty',
    '/courses/sections/suffix',
    '/courses/sections/title',
    '/courses/sections/description',
    '/courses/sections/combinedSectionDescription',
    '/courses/sections/combinedSection',
    '/courses/sections/group',
    '/courses/sections/sectionType',
    '/courses/sections/credits',
    '/courses/sections/creditVariance',
    '/courses/sections/unitsMinimum',
    '/courses/sections/unitsMaximum',
    '/courses/sections/enrolledTotal',
    '/courses/sections/topic',
    '/courses/sections/associatedClass',
    '/courses/sections/classEnrollmentStatus'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO course_curated.course_schema_service_filters (service, paths) VALUES
('classDetails', ARRAY[
    '/term_Details/academicCareer',
    '/term_Details/term',
    '/courses/sections/v2/instructionType',
    '/courses/sections/v2/meetingDetails'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;

INSERT INTO course_curated.course_schema_service_filters (service, paths) VALUES
('classRoster', ARRAY[
    '/term_Details/academicCareer',
    '/term_Details/term',
    '/courses/sections/v2/roster'
]::TEXT[])
ON CONFLICT (service) DO UPDATE SET paths = EXCLUDED.paths;
