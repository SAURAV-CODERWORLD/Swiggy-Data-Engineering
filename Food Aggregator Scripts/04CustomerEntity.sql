/* ================================================================
   CUSTOMER ENTITY - COMPLETE END-TO-END PIPELINE
   ================================================================

   ARCHITECTURE

   SOURCE CSV
       ↓
   STAGE_SCH.CUSTOMER
       ↓
   STAGE_SCH.CUSTOMER_STM
       ↓
   CLEAN_SCH.CUSTOMER
       ↓
   CLEAN_SCH.CUSTOMER_STM
       ↓
   CONSUMPTION_SCH.CUSTOMER_DIM
       ↓
   SCD TYPE 2


   INITIAL FILE:
   intial/customers/customers-initial.csv

   DELTA FILE:
   delta/customer/day-02-insert-update.csv


   IMPORTANT:
   - STAGE stores source data mostly as TEXT.
   - CLEAN converts data into proper data types.
   - CLEAN contains only the latest version of each customer.
   - CUSTOMER_DIM preserves historical versions.
   ================================================================ */


/* ================================================================
   1. DATABASE
   ================================================================ */

USE ROLE SYSADMIN;

USE DATABASE SANDBOX;


/* ================================================================
   2. STAGE LAYER
   ---------------------------------------------------------------
   Raw source representation.
   Source columns are kept as TEXT.
   Audit columns are added for traceability.
   ================================================================ */

CREATE OR REPLACE TABLE STAGE_SCH.CUSTOMER
(
    CUSTOMERID       TEXT,
    NAME             TEXT,

    MOBILE           TEXT
        WITH TAG (common.pii_policy_tag = 'PII'),

    EMAIL            TEXT
        WITH TAG (common.pii_policy_tag = 'EMAIL'),

    LOGINBYUSING     TEXT,

    GENDER           TEXT
        WITH TAG (common.pii_policy_tag = 'PII'),

    DOB              TEXT
        WITH TAG (common.pii_policy_tag = 'PII'),

    ANNIVERSARY      TEXT,

    PREFERENCES      TEXT,

    CREATEDDATE      TEXT,

    MODIFIEDDATE     TEXT,

    /* Audit columns */
    _STG_FILE_NAME   TEXT,
    _STG_FILE_LOAD_TS TIMESTAMP,
    _STG_FILE_MD5    TEXT,
    _COPY_DATA_TS    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
COMMENT =
'Customer raw/stage table. Source columns are stored as text and audit columns provide file-level traceability.';


/* ================================================================
   3. STAGE STREAM
   ---------------------------------------------------------------
   APPEND_ONLY = TRUE because the stage receives new source files.
   This stream captures newly loaded source records.
   ================================================================ */

CREATE OR REPLACE STREAM STAGE_SCH.CUSTOMER_STM
ON TABLE STAGE_SCH.CUSTOMER
APPEND_ONLY = TRUE
COMMENT =
'Append-only stream that captures newly loaded customer source records.';


/* ================================================================
   4. INITIAL CSV → STAGE
   ================================================================ */
   LIST @STAGE_SCH.CSV_STG;

COPY INTO STAGE_SCH.CUSTOMER
(
    CUSTOMERID,
    NAME,
    MOBILE,
    EMAIL,
    LOGINBYUSING,
    GENDER,
    DOB,
    ANNIVERSARY,
    PREFERENCES,
    CREATEDDATE,
    MODIFIEDDATE,

    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)

FROM
(
    SELECT
        T.$1::TEXT AS CUSTOMERID,
        T.$2::TEXT AS NAME,
        T.$3::TEXT AS MOBILE,
        T.$4::TEXT AS EMAIL,
        T.$5::TEXT AS LOGINBYUSING,
        T.$6::TEXT AS GENDER,
        T.$7::TEXT AS DOB,
        T.$8::TEXT AS ANNIVERSARY,
        T.$9::TEXT AS PREFERENCES,
        T.$10::TEXT AS CREATEDDATE,
        T.$11::TEXT AS MODIFIEDDATE,

        METADATA$FILENAME,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$FILE_CONTENT_KEY,
        CURRENT_TIMESTAMP()

    FROM @STAGE_SCH.CSV_STG/intial/customer/customers-initial.csv T
)

FILE_FORMAT =
(
    FORMAT_NAME = 'STAGE_SCH.CSV_FILE_FORMAT'
)

ON_ERROR = ABORT_STATEMENT;


/* ================================================================
   5. CLEAN TABLE
   ---------------------------------------------------------------
   Proper data types are used here.
   ================================================================ */

CREATE OR REPLACE TABLE CLEAN_SCH.CUSTOMER
(
    CUSTOMER_SK NUMBER AUTOINCREMENT PRIMARY KEY,

    CUSTOMER_ID STRING NOT NULL,

    NAME STRING(100) NOT NULL,

    MOBILE STRING(15)
        WITH TAG (common.pii_policy_tag = 'PII'),

    EMAIL STRING(100)
        WITH TAG (common.pii_policy_tag = 'EMAIL'),

    LOGIN_BY_USING STRING(50),

    GENDER STRING(10)
        WITH TAG (common.pii_policy_tag = 'PII'),

    DOB DATE
        WITH TAG (common.pii_policy_tag = 'PII'),

    ANNIVERSARY DATE,

    PREFERENCES STRING,

    CREATED_DT TIMESTAMP_TZ,

    MODIFIED_DT TIMESTAMP_TZ,

    /* Audit columns */
    _STG_FILE_NAME STRING,
    _STG_FILE_LOAD_TS TIMESTAMP_NTZ,
    _STG_FILE_MD5 STRING,
    _COPY_DATA_TS TIMESTAMP_NTZ
)
COMMENT =
'Customer clean layer containing the latest cleaned and standardized customer record.';


/* ================================================================
   6. INITIAL STAGE → CLEAN
   ---------------------------------------------------------------
   Convert TEXT values into proper data types.
   ================================================================ */

INSERT INTO CLEAN_SCH.CUSTOMER
(
    CUSTOMER_ID,
    NAME,
    MOBILE,
    EMAIL,
    LOGIN_BY_USING,
    GENDER,
    DOB,
    ANNIVERSARY,
    PREFERENCES,
    CREATED_DT,
    MODIFIED_DT,

    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)

SELECT
    CUSTOMERID::STRING,
    NAME::STRING,
    MOBILE::STRING,
    EMAIL::STRING,
    LOGINBYUSING::STRING,
    GENDER::STRING,

    TRY_TO_DATE(
        DOB,
        'YYYY-MM-DD'
    ) AS DOB,

    TRY_TO_DATE(
        ANNIVERSARY,
        'YYYY-MM-DD'
    ) AS ANNIVERSARY,

    PREFERENCES::STRING,

    TRY_TO_TIMESTAMP_TZ(
        CREATEDDATE,
        'YYYY-MM-DD"T"HH24:MI:SS.FF6'
    ) AS CREATED_DT,

    TRY_TO_TIMESTAMP_TZ(
        MODIFIEDDATE,
        'YYYY-MM-DD"T"HH24:MI:SS.FF6'
    ) AS MODIFIED_DT,

    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS

FROM STAGE_SCH.CUSTOMER;


/* ================================================================
   7. CLEAN STREAM
   ---------------------------------------------------------------
   IMPORTANT:
   Created AFTER the initial CLEAN load.

   Therefore the initial 99,899 customers are not treated as
   incremental changes.

   Future INSERT/UPDATE/DELETE operations will be captured.
   ================================================================ */

CREATE OR REPLACE STREAM CLEAN_SCH.CUSTOMER_STM
ON TABLE CLEAN_SCH.CUSTOMER
COMMENT =
'Stream that captures INSERT, UPDATE and DELETE changes from the clean customer table.';


/* ================================================================
   8. CUSTOMER DIMENSION
   ---------------------------------------------------------------
   SCD TYPE 2 dimension.
   ================================================================ */

CREATE OR REPLACE TABLE CONSUMPTION_SCH.CUSTOMER_DIM
(
    CUSTOMER_HK NUMBER PRIMARY KEY,

    CUSTOMER_ID STRING NOT NULL,

    NAME STRING(100) NOT NULL,

    MOBILE STRING(15)
        WITH TAG (common.pii_policy_tag = 'PII'),

    EMAIL STRING(100)
        WITH TAG (common.pii_policy_tag = 'EMAIL'),

    LOGIN_BY_USING STRING(50),

    GENDER STRING(10)
        WITH TAG (common.pii_policy_tag = 'PII'),

    DOB DATE
        WITH TAG (common.pii_policy_tag = 'PII'),

    ANNIVERSARY DATE,

    PREFERENCES STRING,

    EFF_START_DATE TIMESTAMP_TZ,

    EFF_END_DATE TIMESTAMP_TZ,

    IS_CURRENT BOOLEAN
)
COMMENT =
'Customer dimension implementing SCD Type 2 for historical customer tracking.';


/* ================================================================
   9. INITIAL CLEAN → CUSTOMER DIMENSION
   ---------------------------------------------------------------
   Every initial customer receives its first dimension version.

   IS_CURRENT  = TRUE
   EFF_END_DATE = NULL
   ================================================================ */

INSERT INTO CONSUMPTION_SCH.CUSTOMER_DIM
(
    CUSTOMER_HK,
    CUSTOMER_ID,
    NAME,
    MOBILE,
    EMAIL,
    LOGIN_BY_USING,
    GENDER,
    DOB,
    ANNIVERSARY,
    PREFERENCES,
    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT
)

SELECT
    HASH(
        CUSTOMER_ID,
        CURRENT_TIMESTAMP()
    ) AS CUSTOMER_HK,

    CUSTOMER_ID,
    NAME,
    MOBILE,
    EMAIL,
    LOGIN_BY_USING,
    GENDER,
    DOB,
    ANNIVERSARY,
    PREFERENCES,

    CURRENT_TIMESTAMP(),
    NULL,
    TRUE

FROM CLEAN_SCH.CUSTOMER;


/* ================================================================
   10. DAY-02 DELTA FILE → STAGE
   ================================================================ */

COPY INTO STAGE_SCH.CUSTOMER
(
    CUSTOMERID,
    NAME,
    MOBILE,
    EMAIL,
    LOGINBYUSING,
    GENDER,
    DOB,
    ANNIVERSARY,
    PREFERENCES,
    CREATEDDATE,
    MODIFIEDDATE,

    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)

FROM
(
    SELECT
        T.$1::TEXT AS CUSTOMERID,
        T.$2::TEXT AS NAME,
        T.$3::TEXT AS MOBILE,
        T.$4::TEXT AS EMAIL,
        T.$5::TEXT AS LOGINBYUSING,
        T.$6::TEXT AS GENDER,
        T.$7::TEXT AS DOB,
        T.$8::TEXT AS ANNIVERSARY,
        T.$9::TEXT AS PREFERENCES,
        T.$10::TEXT AS CREATEDDATE,
        T.$11::TEXT AS MODIFIEDDATE,

        METADATA$FILENAME,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$FILE_CONTENT_KEY,
        CURRENT_TIMESTAMP()

   FILE_FORMAT =
(
    FORMAT_NAME = 'STAGE_SCH.CSV_FILE_FORMAT'
)

ON_ERROR = ABORT_STATEMENT;

LIST @STAGE_SCH.CSV_STG/delta/customer/day-02-insert-update.csv;


/* ================================================================
   11. STAGE STREAM → CLEAN
   ---------------------------------------------------------------
   The STAGE stream contains the newly loaded DAY-02 records.

   MERGE:
       Existing CUSTOMER_ID → UPDATE
       New CUSTOMER_ID      → INSERT
   ================================================================ */
MERGE INTO CLEAN_SCH.CUSTOMER T
USING (
    SELECT *
    FROM STAGE_SCH.CUSTOMER_STM
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CUSTOMERID
        ORDER BY _STG_FILE_LOAD_TS DESC, _COPY_DATA_TS DESC
    ) = 1
) S
ON T.CUSTOMER_ID = S.CUSTOMERID

WHEN MATCHED THEN UPDATE SET
    T.NAME            = S.NAME,
    T.MOBILE          = S.MOBILE,
    T.EMAIL           = S.EMAIL,
    T.LOGIN_BY_USING  = S.LOGINBYUSING,
    T.GENDER          = S.GENDER,
    T.DOB             = TRY_TO_DATE(S.DOB, 'YYYY-MM-DD'),
    T.ANNIVERSARY     = TRY_TO_DATE(S.ANNIVERSARY, 'YYYY-MM-DD'),
    T.PREFERENCES     = S.PREFERENCES,
    T.CREATED_DT      = TRY_TO_TIMESTAMP_TZ(S.CREATEDDATE),
    T.MODIFIED_DT     = TRY_TO_TIMESTAMP_TZ(S.MODIFIEDDATE),
    T._STG_FILE_NAME  = S._STG_FILE_NAME,
    T._STG_FILE_LOAD_TS = S._STG_FILE_LOAD_TS,
    T._STG_FILE_MD5   = S._STG_FILE_MD5,
    T._COPY_DATA_TS   = S._COPY_DATA_TS

WHEN NOT MATCHED THEN INSERT (
    CUSTOMER_ID,
    NAME,
    MOBILE,
    EMAIL,
    LOGIN_BY_USING,
    GENDER,
    DOB,
    ANNIVERSARY,
    PREFERENCES,
    CREATED_DT,
    MODIFIED_DT,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)
VALUES (
    S.CUSTOMERID,
    S.NAME,
    S.MOBILE,
    S.EMAIL,
    S.LOGINBYUSING,
    S.GENDER,
    TRY_TO_DATE(S.DOB, 'YYYY-MM-DD'),
    TRY_TO_DATE(S.ANNIVERSARY, 'YYYY-MM-DD'),
    S.PREFERENCES,
    TRY_TO_TIMESTAMP_TZ(S.CREATEDDATE),
    TRY_TO_TIMESTAMP_TZ(S.MODIFIEDDATE),
    S._STG_FILE_NAME,
    S._STG_FILE_LOAD_TS,
    S._STG_FILE_MD5,
    S._COPY_DATA_TS
);



/* ================================================================
   12. MATERIALIZE CLEAN STREAM
   ---------------------------------------------------------------
   IMPORTANT:
   We first copy the stream into a temporary table.

   This gives us a fixed snapshot of the changes and allows us
   to use those changes in multiple SQL statements.
   ================================================================ */

CREATE OR REPLACE TEMP TABLE TEMP_CUSTOMER_CHANGES AS

SELECT
    *,
    CURRENT_TIMESTAMP() AS CHANGE_TS

FROM CLEAN_SCH.CUSTOMER_STM;


/* ================================================================
   13. CLOSE OLD SCD2 RECORDS
   ---------------------------------------------------------------
   For existing customers that changed:

       OLD VERSION
       IS_CURRENT = TRUE

              ↓

       OLD VERSION
       IS_CURRENT = FALSE
       EFF_END_DATE = CHANGE_TS
   ================================================================ */

UPDATE CONSUMPTION_SCH.CUSTOMER_DIM AS TARGET

SET
    TARGET.EFF_END_DATE = SOURCE.CHANGE_TS,
    TARGET.IS_CURRENT = FALSE

FROM TEMP_CUSTOMER_CHANGES AS SOURCE

WHERE TARGET.CUSTOMER_ID = SOURCE.CUSTOMER_ID

  AND TARGET.IS_CURRENT = TRUE

  AND SOURCE.METADATA$ACTION = 'DELETE'

  AND SOURCE.METADATA$ISUPDATE = TRUE;


/* ================================================================
   14. INSERT NEW SCD2 VERSIONS
   ---------------------------------------------------------------
   For INSERT events from the CLEAN stream:

   - Existing customer's new version
   - Completely new customer

   Both receive:

       EFF_START_DATE = CHANGE_TS
       EFF_END_DATE   = NULL
       IS_CURRENT     = TRUE
   ================================================================ */

INSERT INTO CONSUMPTION_SCH.CUSTOMER_DIM
(
    CUSTOMER_HK,
    CUSTOMER_ID,
    NAME,
    MOBILE,
    EMAIL,
    LOGIN_BY_USING,
    GENDER,
    DOB,
    ANNIVERSARY,
    PREFERENCES,
    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT
)

SELECT
    HASH(
        CUSTOMER_ID,
        CHANGE_TS
    ) AS CUSTOMER_HK,

    CUSTOMER_ID,
    NAME,
    MOBILE,
    EMAIL,
    LOGIN_BY_USING,
    GENDER,
    DOB,
    ANNIVERSARY,
    PREFERENCES,

    CHANGE_TS,
    NULL,
    TRUE

FROM TEMP_CUSTOMER_CHANGES

WHERE METADATA$ACTION = 'INSERT';


/* ================================================================
   15. VALIDATION
   ================================================================ */


/* Check total CLEAN records */

SELECT
    COUNT(*) AS TOTAL_CLEAN_CUSTOMERS

FROM CLEAN_SCH.CUSTOMER;


/* Check total DIMENSION records */

SELECT
    COUNT(*) AS TOTAL_DIMENSION_VERSIONS

FROM CONSUMPTION_SCH.CUSTOMER_DIM;


/* Check currently active customers */

SELECT
    COUNT(*) AS CURRENT_CUSTOMERS

FROM CONSUMPTION_SCH.CUSTOMER_DIM

WHERE IS_CURRENT = TRUE;


/* Check SCD2 versions per customer */

SELECT
    CUSTOMER_ID,

    COUNT(*) AS VERSION_COUNT,

    SUM(
        IFF(IS_CURRENT = TRUE, 1, 0)
    ) AS CURRENT_VERSION_COUNT

FROM CONSUMPTION_SCH.CUSTOMER_DIM

GROUP BY CUSTOMER_ID

ORDER BY CUSTOMER_ID;


/* ================================================================
   16. VIEW COMPLETE CUSTOMER HISTORY
   ================================================================ */

SELECT
    CUSTOMER_ID,
    NAME,
    MOBILE,
    EMAIL,
    LOGIN_BY_USING,
    GENDER,
    DOB,
    ANNIVERSARY,
    PREFERENCES,

    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT

FROM CONSUMPTION_SCH.CUSTOMER_DIM

ORDER BY
    CUSTOMER_ID,
    EFF_START_DATE;


/* ================================================================
   FINAL CONCEPT

   INITIAL LOAD:

       CSV
        ↓
       STAGE
        ↓
       CLEAN
        ↓
       DIMENSION


   FUTURE DELTA:

       DELTA CSV
          ↓
       STAGE
          ↓
       STAGE STREAM
          ↓
       CLEAN MERGE
          ↓
       CLEAN STREAM
          ↓
       TEMP CHANGE TABLE
          ↓
       ┌──────────────────────────┐
       │                          │
       ▼                          ▼
   Existing Customer          New Customer
       │                          │
       ▼                          ▼
   Close old version          Insert version
       │                          │
       └──────────┬───────────────┘
                  ▼
             CUSTOMER_DIM


   SCD TYPE 2 RULE:

   OLD VERSION
       IS_CURRENT = FALSE
       EFF_END_DATE = CHANGE_TS

   NEW VERSION
       IS_CURRENT = TRUE
       EFF_START_DATE = CHANGE_TS
       EFF_END_DATE = NULL
   ================================================================ */