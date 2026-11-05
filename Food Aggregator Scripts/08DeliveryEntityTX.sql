-- ============================================================
-- 08_DELIVERYENTITYTX.SQL
-- DELIVERY ENTITY TRANSACTION DATA PIPELINE
--
-- FLOW:
-- STAGE → CLEAN → CONSUMPTION
-- INITIAL LOAD → DAY-02 DELTA → SCD TYPE 2
-- ============================================================


-- ============================================================
-- 1. DATABASE / ROLE
-- ============================================================

USE ROLE SYSADMIN;

USE DATABASE SANDBOX;


-- ============================================================
-- 2. STAGE LAYER
-- ============================================================

USE SCHEMA STAGE_SCH;


CREATE OR REPLACE TABLE STAGE_SCH.DELIVERY_ENTITY_TX (
    
    DELIVERY_ID TEXT,
    ORDER_ID TEXT,
    DELIVERY_AGENT_ID TEXT,
    DELIVERY_STATUS TEXT,
    ESTIMATED_TIME TEXT,
    DELIVERY_ADDRESS TEXT,
    DELIVERY_DATE TEXT,
    CREATED_DATE TEXT,
    MODIFIED_DATE TEXT,

    -- Audit columns
    _STG_FILE_NAME TEXT,
    _STG_FILE_LOAD_TS TIMESTAMP,
    _STG_FILE_MD5 TEXT,
    _COPY_DATA_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP

)
COMMENT = 'Raw Delivery Entity Transaction staging table';


-- ============================================================
-- 3. STAGE STREAM
-- ============================================================

CREATE OR REPLACE STREAM STAGE_SCH.DELIVERY_ENTITY_TX_STM
ON TABLE STAGE_SCH.DELIVERY_ENTITY_TX
APPEND_ONLY = TRUE
COMMENT = 'Append-only stream for Delivery Entity Transaction';


-- ============================================================
-- 4. CHECK INITIAL FILE
-- ============================================================

LIST @STAGE_SCH.CSV_STG/intial/delivery/delivery-initial-load.csv;


-- ============================================================
-- 5. INITIAL STAGE LOAD
-- ============================================================

COPY INTO STAGE_SCH.DELIVERY_ENTITY_TX
(
    DELIVERY_ID,
    ORDER_ID,
    DELIVERY_AGENT_ID,
    DELIVERY_STATUS,
    ESTIMATED_TIME,
    DELIVERY_ADDRESS,
    DELIVERY_DATE,
    CREATED_DATE,
    MODIFIED_DATE,

    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)

FROM
(
    SELECT

        T.$1::TEXT,
        T.$2::TEXT,
        T.$3::TEXT,
        T.$4::TEXT,
        T.$5::TEXT,
        T.$6::TEXT,
        T.$7::TEXT,
        T.$8::TEXT,
        T.$9::TEXT,

        METADATA$FILENAME,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$FILE_CONTENT_KEY,
        CURRENT_TIMESTAMP

    FROM @STAGE_SCH.CSV_STG/intial/delivery/delivery-initial-load.csv T
)

FILE_FORMAT = (
    FORMAT_NAME = 'STAGE_SCH.CSV_FILE_FORMAT'
)

ON_ERROR = ABORT_STATEMENT;


-- ============================================================
-- 6. INITIAL STAGE VALIDATION
-- ============================================================

SELECT COUNT(*) AS INITIAL_STAGE_COUNT
FROM STAGE_SCH.DELIVERY_ENTITY_TX;


SELECT *
FROM STAGE_SCH.DELIVERY_ENTITY_TX
ORDER BY TRY_TO_NUMBER(DELIVERY_ID);


-- ============================================================
-- 7. CLEAN LAYER
-- ============================================================

CREATE OR REPLACE TABLE CLEAN_SCH.DELIVERY_ENTITY_TX
(

    DELIVERY_ENTITY_TX_SK INT AUTOINCREMENT PRIMARY KEY,

    DELIVERY_ID INT NOT NULL UNIQUE,
    ORDER_ID INT,
    DELIVERY_AGENT_ID INT,
    DELIVERY_STATUS STRING,
    ESTIMATED_TIME STRING,
    DELIVERY_ADDRESS INT,

    DELIVERY_DATE TIMESTAMP_NTZ,
    CREATED_DATE TIMESTAMP_NTZ,
    MODIFIED_DATE TIMESTAMP_NTZ,

    -- Audit columns
    _STG_FILE_NAME STRING,
    _STG_FILE_LOAD_TS TIMESTAMP_NTZ,
    _STG_FILE_MD5 STRING,
    _COPY_DATA_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP

)
COMMENT = 'Clean Delivery Entity Transaction table';


-- ============================================================
-- 8. INITIAL STAGE → CLEAN
-- ============================================================

MERGE INTO CLEAN_SCH.DELIVERY_ENTITY_TX AS TARGET

USING
(
    SELECT

        TRY_TO_NUMBER(DELIVERY_ID) AS DELIVERY_ID,
        TRY_TO_NUMBER(ORDER_ID) AS ORDER_ID,
        TRY_TO_NUMBER(DELIVERY_AGENT_ID) AS DELIVERY_AGENT_ID,

        TRIM(DELIVERY_STATUS) AS DELIVERY_STATUS,

        TRIM(ESTIMATED_TIME) AS ESTIMATED_TIME,

        TRY_TO_NUMBER(DELIVERY_ADDRESS) AS DELIVERY_ADDRESS,

        TRY_TO_TIMESTAMP_NTZ(DELIVERY_DATE) AS DELIVERY_DATE,

        TRY_TO_TIMESTAMP_NTZ(CREATED_DATE) AS CREATED_DATE,

        TRY_TO_TIMESTAMP_NTZ(MODIFIED_DATE) AS MODIFIED_DATE,

        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS

    FROM STAGE_SCH.DELIVERY_ENTITY_TX

    QUALIFY ROW_NUMBER() OVER
    (
        PARTITION BY TRY_TO_NUMBER(DELIVERY_ID)
        ORDER BY _COPY_DATA_TS DESC
    ) = 1

) AS SOURCE

ON TARGET.DELIVERY_ID = SOURCE.DELIVERY_ID


WHEN MATCHED THEN UPDATE SET

    ORDER_ID = SOURCE.ORDER_ID,
    DELIVERY_AGENT_ID = SOURCE.DELIVERY_AGENT_ID,
    DELIVERY_STATUS = SOURCE.DELIVERY_STATUS,
    ESTIMATED_TIME = SOURCE.ESTIMATED_TIME,
    DELIVERY_ADDRESS = SOURCE.DELIVERY_ADDRESS,

    DELIVERY_DATE = SOURCE.DELIVERY_DATE,
    CREATED_DATE = SOURCE.CREATED_DATE,
    MODIFIED_DATE = SOURCE.MODIFIED_DATE,

    _STG_FILE_NAME = SOURCE._STG_FILE_NAME,
    _STG_FILE_LOAD_TS = SOURCE._STG_FILE_LOAD_TS,
    _STG_FILE_MD5 = SOURCE._STG_FILE_MD5,
    _COPY_DATA_TS = CURRENT_TIMESTAMP


WHEN NOT MATCHED THEN INSERT
(
    DELIVERY_ID,
    ORDER_ID,
    DELIVERY_AGENT_ID,
    DELIVERY_STATUS,
    ESTIMATED_TIME,
    DELIVERY_ADDRESS,

    DELIVERY_DATE,
    CREATED_DATE,
    MODIFIED_DATE,

    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)

VALUES
(
    SOURCE.DELIVERY_ID,
    SOURCE.ORDER_ID,
    SOURCE.DELIVERY_AGENT_ID,
    SOURCE.DELIVERY_STATUS,
    SOURCE.ESTIMATED_TIME,
    SOURCE.DELIVERY_ADDRESS,

    SOURCE.DELIVERY_DATE,
    SOURCE.CREATED_DATE,
    SOURCE.MODIFIED_DATE,

    SOURCE._STG_FILE_NAME,
    SOURCE._STG_FILE_LOAD_TS,
    SOURCE._STG_FILE_MD5,
    CURRENT_TIMESTAMP
);


-- ============================================================
-- 9. INITIAL CLEAN VALIDATION
-- ============================================================

SELECT COUNT(*) AS CLEAN_INITIAL_COUNT
FROM CLEAN_SCH.DELIVERY_ENTITY_TX;


SELECT *
FROM CLEAN_SCH.DELIVERY_ENTITY_TX
ORDER BY DELIVERY_ID;


-- ============================================================
-- 10. CREATE CLEAN STREAM
-- IMPORTANT:
-- Stream is created AFTER initial CLEAN load
-- so initial records are NOT captured as Day-02 changes.
-- ============================================================

CREATE OR REPLACE STREAM CLEAN_SCH.DELIVERY_ENTITY_TX_STM
ON TABLE CLEAN_SCH.DELIVERY_ENTITY_TX
COMMENT = 'Stream capturing Delivery Entity Transaction changes';


-- ============================================================
-- 11. CONSUMPTION / SCD TYPE 2 DIMENSION
-- ============================================================

CREATE OR REPLACE TABLE CONSUMPTION_SCH.DELIVERY_ENTITY_TX_DIM
(

    DELIVERY_ENTITY_TX_HK NUMBER PRIMARY KEY,

    DELIVERY_ID NUMBER NOT NULL,
    ORDER_ID NUMBER,
    DELIVERY_AGENT_ID NUMBER,
    DELIVERY_STATUS STRING,
    ESTIMATED_TIME STRING,
    DELIVERY_ADDRESS NUMBER,

    DELIVERY_DATE TIMESTAMP_NTZ,
    CREATED_DATE TIMESTAMP_NTZ,
    MODIFIED_DATE TIMESTAMP_NTZ,

    EFF_START_DATE TIMESTAMP_NTZ,
    EFF_END_DATE TIMESTAMP_NTZ,

    IS_CURRENT BOOLEAN DEFAULT TRUE

)
COMMENT = 'Delivery Entity Transaction SCD Type 2 Dimension';


-- ============================================================
-- 12. INITIAL CLEAN → CONSUMPTION
-- ============================================================

INSERT INTO CONSUMPTION_SCH.DELIVERY_ENTITY_TX_DIM
(

    DELIVERY_ENTITY_TX_HK,

    DELIVERY_ID,
    ORDER_ID,
    DELIVERY_AGENT_ID,
    DELIVERY_STATUS,
    ESTIMATED_TIME,
    DELIVERY_ADDRESS,

    DELIVERY_DATE,
    CREATED_DATE,
    MODIFIED_DATE,

    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT

)

SELECT

    HASH
    (
        DELIVERY_ID,
        CURRENT_TIMESTAMP()
    ) AS DELIVERY_ENTITY_TX_HK,

    DELIVERY_ID,
    ORDER_ID,
    DELIVERY_AGENT_ID,
    DELIVERY_STATUS,
    ESTIMATED_TIME,
    DELIVERY_ADDRESS,

    DELIVERY_DATE,
    CREATED_DATE,
    MODIFIED_DATE,

    CURRENT_TIMESTAMP(),
    NULL,
    TRUE

FROM CLEAN_SCH.DELIVERY_ENTITY_TX;


-- ============================================================
-- 13. INITIAL DIMENSION VALIDATION
-- ============================================================

SELECT

    COUNT(*) AS TOTAL_ROWS,

    COUNT_IF(IS_CURRENT = TRUE)
        AS CURRENT_ROWS,

    COUNT_IF(IS_CURRENT = FALSE)
        AS HISTORICAL_ROWS

FROM CONSUMPTION_SCH.DELIVERY_ENTITY_TX_DIM;


-- ============================================================
-- 14. DAY-02 FILE CHECK
-- ============================================================

LIST @STAGE_SCH.CSV_STG/delta/delivery/day-02-delivery.csv;


-- ============================================================
-- 15. DAY-02 STAGE LOAD
-- ============================================================

COPY INTO STAGE_SCH.DELIVERY_ENTITY_TX
(
    DELIVERY_ID,
    ORDER_ID,
    DELIVERY_AGENT_ID,
    DELIVERY_STATUS,
    ESTIMATED_TIME,
    DELIVERY_ADDRESS,
    DELIVERY_DATE,
    CREATED_DATE,
    MODIFIED_DATE,

    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)

FROM
(
    SELECT

        T.$1::TEXT,
        T.$2::TEXT,
        T.$3::TEXT,
        T.$4::TEXT,
        T.$5::TEXT,
        T.$6::TEXT,
        T.$7::TEXT,
        T.$8::TEXT,
        T.$9::TEXT,

        METADATA$FILENAME,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$FILE_CONTENT_KEY,
        CURRENT_TIMESTAMP

    FROM @STAGE_SCH.CSV_STG/delta/delivery/day-02-delivery.csv T
)

FILE_FORMAT = (
    FORMAT_NAME = 'STAGE_SCH.CSV_FILE_FORMAT'
)

ON_ERROR = ABORT_STATEMENT;


-- ============================================================
-- 16. DAY-02 STAGE VALIDATION
-- ============================================================

SELECT COUNT(*) AS DAY02_STAGE_ROWS
FROM STAGE_SCH.DELIVERY_ENTITY_TX
WHERE _STG_FILE_NAME ILIKE '%DAY-02-DELIVERY.CSV%';


SELECT *
FROM STAGE_SCH.DELIVERY_ENTITY_TX
WHERE _STG_FILE_NAME ILIKE '%DAY-02-DELIVERY.CSV%'
ORDER BY TRY_TO_NUMBER(DELIVERY_ID);


-- ============================================================
-- 17. DAY-02 STAGE → CLEAN
-- ============================================================

MERGE INTO CLEAN_SCH.DELIVERY_ENTITY_TX AS TARGET

USING
(
    SELECT

        TRY_TO_NUMBER(DELIVERY_ID) AS DELIVERY_ID,
        TRY_TO_NUMBER(ORDER_ID) AS ORDER_ID,
        TRY_TO_NUMBER(DELIVERY_AGENT_ID) AS DELIVERY_AGENT_ID,

        TRIM(DELIVERY_STATUS) AS DELIVERY_STATUS,

        TRIM(ESTIMATED_TIME) AS ESTIMATED_TIME,

        TRY_TO_NUMBER(DELIVERY_ADDRESS) AS DELIVERY_ADDRESS,

        TRY_TO_TIMESTAMP_NTZ(DELIVERY_DATE) AS DELIVERY_DATE,

        TRY_TO_TIMESTAMP_NTZ(CREATED_DATE) AS CREATED_DATE,

        TRY_TO_TIMESTAMP_NTZ(MODIFIED_DATE) AS MODIFIED_DATE,

        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS

    FROM STAGE_SCH.DELIVERY_ENTITY_TX

    WHERE _STG_FILE_NAME ILIKE '%DAY-02-DELIVERY.CSV%'

    QUALIFY ROW_NUMBER() OVER
    (
        PARTITION BY TRY_TO_NUMBER(DELIVERY_ID)
        ORDER BY _COPY_DATA_TS DESC
    ) = 1

) AS SOURCE

ON TARGET.DELIVERY_ID = SOURCE.DELIVERY_ID


WHEN MATCHED THEN UPDATE SET

    ORDER_ID = SOURCE.ORDER_ID,
    DELIVERY_AGENT_ID = SOURCE.DELIVERY_AGENT_ID,
    DELIVERY_STATUS = SOURCE.DELIVERY_STATUS,
    ESTIMATED_TIME = SOURCE.ESTIMATED_TIME,
    DELIVERY_ADDRESS = SOURCE.DELIVERY_ADDRESS,

    DELIVERY_DATE = SOURCE.DELIVERY_DATE,
    CREATED_DATE = SOURCE.CREATED_DATE,
    MODIFIED_DATE = SOURCE.MODIFIED_DATE,

    _STG_FILE_NAME = SOURCE._STG_FILE_NAME,
    _STG_FILE_LOAD_TS = SOURCE._STG_FILE_LOAD_TS,
    _STG_FILE_MD5 = SOURCE._STG_FILE_MD5,
    _COPY_DATA_TS = CURRENT_TIMESTAMP


WHEN NOT MATCHED THEN INSERT
(
    DELIVERY_ID,
    ORDER_ID,
    DELIVERY_AGENT_ID,
    DELIVERY_STATUS,
    ESTIMATED_TIME,
    DELIVERY_ADDRESS,

    DELIVERY_DATE,
    CREATED_DATE,
    MODIFIED_DATE,

    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)

VALUES
(
    SOURCE.DELIVERY_ID,
    SOURCE.ORDER_ID,
    SOURCE.DELIVERY_AGENT_ID,
    SOURCE.DELIVERY_STATUS,
    SOURCE.ESTIMATED_TIME,
    SOURCE.DELIVERY_ADDRESS,

    SOURCE.DELIVERY_DATE,
    SOURCE.CREATED_DATE,
    SOURCE.MODIFIED_DATE,

    SOURCE._STG_FILE_NAME,
    SOURCE._STG_FILE_LOAD_TS,
    SOURCE._STG_FILE_MD5,
    CURRENT_TIMESTAMP
);


-- ============================================================
-- 18. DAY-02 CLEAN VALIDATION
-- ============================================================

SELECT COUNT(*) AS DAY02_CLEAN_ROWS
FROM CLEAN_SCH.DELIVERY_ENTITY_TX
WHERE _STG_FILE_NAME ILIKE '%DAY-02-DELIVERY.CSV%';


SELECT *
FROM CLEAN_SCH.DELIVERY_ENTITY_TX
WHERE _STG_FILE_NAME ILIKE '%DAY-02-DELIVERY.CSV%'
ORDER BY DELIVERY_ID;


-- ============================================================
-- 19. MATERIALIZE CLEAN STREAM
-- IMPORTANT:
-- We store the stream data in a TEMP table because the stream
-- should not be consumed multiple times directly.
-- ============================================================

CREATE OR REPLACE TEMP TABLE TEMP_DELIVERY_ENTITY_TX_CHANGES AS

SELECT *
FROM CLEAN_SCH.DELIVERY_ENTITY_TX_STM;


-- ============================================================
-- 20. CHECK STREAM CHANGES
-- ============================================================

SELECT

    DELIVERY_ID,
    ORDER_ID,
    DELIVERY_AGENT_ID,
    DELIVERY_STATUS,
    METADATA$ACTION,
    METADATA$ISUPDATE

FROM TEMP_DELIVERY_ENTITY_TX_CHANGES

ORDER BY DELIVERY_ID;


-- ============================================================
-- 21. CLOSE OLD SCD2 RECORDS
-- ============================================================

UPDATE CONSUMPTION_SCH.DELIVERY_ENTITY_TX_DIM AS TARGET

SET

    EFF_END_DATE = CURRENT_TIMESTAMP(),
    IS_CURRENT = FALSE

WHERE TARGET.IS_CURRENT = TRUE

AND EXISTS
(
    SELECT 1

    FROM TEMP_DELIVERY_ENTITY_TX_CHANGES AS SOURCE

    WHERE SOURCE.DELIVERY_ID = TARGET.DELIVERY_ID

      AND SOURCE.METADATA$ACTION = 'DELETE'

      AND SOURCE.METADATA$ISUPDATE = TRUE
);


-- ============================================================
-- 22. INSERT NEW / UPDATED SCD2 RECORDS
-- ============================================================

INSERT INTO CONSUMPTION_SCH.DELIVERY_ENTITY_TX_DIM
(

    DELIVERY_ENTITY_TX_HK,

    DELIVERY_ID,
    ORDER_ID,
    DELIVERY_AGENT_ID,
    DELIVERY_STATUS,
    ESTIMATED_TIME,
    DELIVERY_ADDRESS,

    DELIVERY_DATE,
    CREATED_DATE,
    MODIFIED_DATE,

    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT

)

SELECT

    HASH
    (
        DELIVERY_ID,
        CURRENT_TIMESTAMP()
    ) AS DELIVERY_ENTITY_TX_HK,

    DELIVERY_ID,
    ORDER_ID,
    DELIVERY_AGENT_ID,
    DELIVERY_STATUS,
    ESTIMATED_TIME,
    DELIVERY_ADDRESS,

    DELIVERY_DATE,
    CREATED_DATE,
    MODIFIED_DATE,

    CURRENT_TIMESTAMP(),
    NULL,
    TRUE

FROM TEMP_DELIVERY_ENTITY_TX_CHANGES

WHERE METADATA$ACTION = 'INSERT'

QUALIFY ROW_NUMBER() OVER
(
    PARTITION BY DELIVERY_ID
    ORDER BY _COPY_DATA_TS DESC
) = 1;


-- ============================================================
-- 23. FINAL DIMENSION VALIDATION
-- ============================================================

SELECT

    COUNT(*) AS TOTAL_ROWS,

    COUNT_IF(IS_CURRENT = TRUE)
        AS CURRENT_ROWS,

    COUNT_IF(IS_CURRENT = FALSE)
        AS HISTORICAL_ROWS

FROM CONSUMPTION_SCH.DELIVERY_ENTITY_TX_DIM;


-- ============================================================
-- 24. SHOW CURRENT DELIVERY TRANSACTIONS
-- ============================================================

SELECT *

FROM CONSUMPTION_SCH.DELIVERY_ENTITY_TX_DIM

WHERE IS_CURRENT = TRUE

ORDER BY DELIVERY_ID;


-- ============================================================
-- 25. CHECK SCD2 VERSIONS
-- ============================================================

SELECT

    DELIVERY_ID,
    COUNT(*) AS VERSION_COUNT

FROM CONSUMPTION_SCH.DELIVERY_ENTITY_TX_DIM

GROUP BY DELIVERY_ID

HAVING COUNT(*) > 1

ORDER BY DELIVERY_ID;


-- ============================================================
-- 26. CHECK MULTIPLE CURRENT VERSIONS
-- ============================================================

SELECT

    DELIVERY_ID,

    COUNT_IF(IS_CURRENT = TRUE)
        AS CURRENT_VERSION_COUNT

FROM CONSUMPTION_SCH.DELIVERY_ENTITY_TX_DIM

GROUP BY DELIVERY_ID

HAVING COUNT_IF(IS_CURRENT = TRUE) > 1

ORDER BY DELIVERY_ID;