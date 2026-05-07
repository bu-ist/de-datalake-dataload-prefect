/*===================================================================================
=                               RAW LAYER TABLES                                    =
===================================================================================*/

-- Term data raw snapshot of current ingestion
CREATE TABLE term_raw.term_data (
    acad_career VARCHAR(4) NOT NULL,
    strm        VARCHAR(4) NOT NULL,
    term_data   JSONB NOT NULL,
    modified_date TIMESTAMP DEFAULT now() NOT NULL
);



/*===================================================================================
=                           TRANSFORM / CURRENT TERM TABLE                           =
===================================================================================*/

CREATE TABLE term_xform.current_term_data (
    acad_career       VARCHAR(4)  NOT NULL,
    strm              VARCHAR(4)  NOT NULL,
    descr             VARCHAR(50) NULL,
    term_begin_dt     VARCHAR(8)  NULL,
    term_end_dt       VARCHAR(8)  NULL,
    current_ind       VARCHAR(1)  NULL,
    bu_cur_future_term VARCHAR(1) NULL,
    bu_future_term_1  VARCHAR(4)  NULL,
    bu_past_term_1    VARCHAR(4)  NULL,
    old_term          VARCHAR(5)  NULL,
    acad_year         VARCHAR(4)  NULL,
    rownumber         INT         NULL,
    modified_date     TIMESTAMP   DEFAULT now() NOT NULL,
    PRIMARY KEY (acad_career, strm, rownumber)
);



/*===================================================================================
=                                  CURATED TABLE                                     =
===================================================================================*/

-- Curated view of *selected term rows* based on STRM-selection logic
CREATE TABLE term_curated.term_data_by_service (
    service           VARCHAR      NOT NULL,   -- e.g. "active_terms"
    acad_career       VARCHAR(4)   NOT NULL,
    strm              VARCHAR(4)   NOT NULL,
    descr             VARCHAR(50)  NULL,
    term_begin_dt     VARCHAR(8)   NULL,
    term_end_dt       VARCHAR(8)   NULL,
    current_ind       VARCHAR(1)   NULL,
    bu_cur_future_term VARCHAR(1)  NULL,
    bu_future_term_1  VARCHAR(4)   NULL,
    bu_past_term_1    VARCHAR(4)   NULL,
    old_term          VARCHAR(5)   NULL,
    acad_year         VARCHAR(4)   NULL,
    rownumber         INT          NULL,
    modified_date     TIMESTAMP   DEFAULT now() NOT NULL,
    PRIMARY KEY (service, acad_career, strm)
);



/*===================================================================================
=                        RAW → CURRENT: Called within Dagster                        =
===================================================================================*/

CREATE OR REPLACE FUNCTION term_raw.refresh_current_term_data()
RETURNS VOID AS $$
BEGIN
    -- Wipe table to update with most up to date BU_TERM_QRY data
    TRUNCATE term_xform.current_term_data;

    INSERT INTO term_xform.current_term_data (
        acad_career,
        strm,
        descr,
        term_begin_dt,
        term_end_dt,
        current_ind,
        bu_cur_future_term,
        bu_future_term_1,
        bu_past_term_1,
        old_term,
        acad_year,
        rownumber,
        modified_date
    )
    SELECT
        t.acad_career,
        t.strm,
        t.term_data->>'DESCR',
        t.term_data->>'TERM_BEGIN_DT',
        t.term_data->>'TERM_END_DT',
        t.term_data->>'CURRENT_IND',
        t.term_data->>'BU_CUR_FUTURE_TERM',
        t.term_data->>'BU_FUTURE_TERM_1',
        t.term_data->>'BU_PAST_TERM_1',
        t.term_data->>'OLD_TERM',
        t.term_data->>'ACAD_YEAR',
        (t.term_data->>'attr:rownumber')::int,
        NOW()
    FROM term_raw.term_data t;

    -- Call curated refresh AFTER the current table has been fully rebuilt
    PERFORM term_xform.refresh_curated_terms();

END;
$$ LANGUAGE plpgsql;




/*===================================================================================
=                               UPSERT CURATED TERMS                                 =
===================================================================================*/

-- This matches Python EXACTLY:
--   1. Find current term (UGRD + CURRENT_IND = 'Y')
--   2. Take rownumber-1 (endTerm)
--   3. Take rownumber+1 (startTerm)
--   4. If the current term is not Summer:
--          If endTerm is Summer → next is startTerm.rownumber + 1
--          Else → next is startTerm.rownumber - 1
--   5. Insert only these STRM values into curated layer.

--TODO: Add table term_schema_service_filters to define term services active_terms and others
CREATE OR REPLACE FUNCTION term_xform.refresh_curated_terms()
RETURNS VOID AS $$
DECLARE
    cur_term     RECORD;
    end_term     RECORD;
    start_term   RECORD;
    extra_term   RECORD;
    chosen_strms TEXT[];
BEGIN
    --------------------------------------------------------------------
    -- STEP 1: Identify the "current" UGRD term
    --------------------------------------------------------------------
    SELECT *
    INTO cur_term
    FROM term_xform.current_term_data
    WHERE acad_career = 'UGRD'
      AND current_ind = 'Y'
    LIMIT 1;

    IF cur_term IS NULL THEN
        RETURN;
    END IF;

    --------------------------------------------------------------------
    -- STEP 2 & 3: Adjacent terms by rownumber
    --------------------------------------------------------------------
    SELECT * INTO end_term
    FROM term_xform.current_term_data
    WHERE rownumber = cur_term.rownumber - 1
          AND acad_career = 'UGRD';

    SELECT * INTO start_term
    FROM term_xform.current_term_data
    WHERE rownumber = cur_term.rownumber + 1
          AND acad_career = 'UGRD';

    chosen_strms := ARRAY[cur_term.strm];

    IF end_term IS NOT NULL THEN
        chosen_strms := chosen_strms || end_term.strm;
    END IF;

    IF start_term IS NOT NULL THEN
        chosen_strms := chosen_strms || start_term.strm;
    END IF;

    --------------------------------------------------------------------
    -- STEP 4: Conditional 4th term based on Summer logic
    --------------------------------------------------------------------
    IF cur_term.descr NOT ILIKE '%Summer%' THEN
        -- endTerm is Summer → use endTerm.rownumber - 1
        IF end_term IS NOT NULL AND end_term.descr ILIKE '%Summer%' THEN

            SELECT *
            INTO extra_term
            FROM term_xform.current_term_data
            WHERE rownumber = end_term.rownumber - 1
            AND acad_career = 'UGRD';

        ELSE
            -- otherwise → use startTerm.rownumber + 1
            SELECT *
            INTO extra_term
            FROM term_xform.current_term_data
            WHERE rownumber = start_term.rownumber + 1
            AND acad_career = 'UGRD';

        END IF;

        IF extra_term IS NOT NULL THEN
            chosen_strms := chosen_strms || extra_term.strm;
        END IF;

    END IF;

    --------------------------------------------------------------------
    --  Remove existing curated terms for this acad_career
    --------------------------------------------------------------------
    DELETE FROM term_curated.term_data_by_service
    WHERE service = 'active_terms'
      AND acad_career = 'UGRD';

    --------------------------------------------------------------------
    --  Insert chosen STRMs into curated table
    --------------------------------------------------------------------
    INSERT INTO term_curated.term_data_by_service (
        service, acad_career, strm,
        descr, term_begin_dt, term_end_dt,
        current_ind, bu_cur_future_term, bu_future_term_1,
        bu_past_term_1, old_term, acad_year, rownumber,
        modified_date
    )
    SELECT
        'active_terms',
        t.acad_career,
        t.strm,
        t.descr,
        t.term_begin_dt,
        t.term_end_dt,
        t.current_ind,
        t.bu_cur_future_term,
        t.bu_future_term_1,
        t.bu_past_term_1,
        t.old_term,
        t.acad_year,
        t.rownumber,
        NOW()
    FROM term_xform.current_term_data t
    WHERE t.strm = ANY(chosen_strms)
      AND t.acad_career = 'UGRD'
    ON CONFLICT (service, acad_career, strm)
    DO UPDATE SET
        descr              = EXCLUDED.descr,
        term_begin_dt      = EXCLUDED.term_begin_dt,
        term_end_dt        = EXCLUDED.term_end_dt,
        current_ind        = EXCLUDED.current_ind,
        bu_cur_future_term = EXCLUDED.bu_cur_future_term,
        bu_future_term_1   = EXCLUDED.bu_future_term_1,
        bu_past_term_1     = EXCLUDED.bu_past_term_1,
        old_term           = EXCLUDED.old_term,
        acad_year          = EXCLUDED.acad_year,
        rownumber          = EXCLUDED.rownumber,
        modified_date      = NOW();

END;
$$ LANGUAGE plpgsql;
