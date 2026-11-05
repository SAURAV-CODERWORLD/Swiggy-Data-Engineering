-- ============================================================
-- DELIVERY AGENT DATA PIPELINE
-- STAGE → CLEAN → CONSUMPTION
-- SCD TYPE 2 IMPLEMENTATION
-- INITIAL + DAY-02
-- ============================================================

USE ROLE SYSADMIN;
USE DATABASE SANDBOX;


-- ============================================================
-- PART 1 : STAGE LAYER
-- ============================================================

USE SCHEMA STAGE_SCH;

CREATE OR REPLACE TABLE STAGE_SCH.DELIVERYAGENT (
    DELIVERYAGENTID TEXT COMMENT 'Primary Key (Source System)',
    NAME TEXT,
    PHONE TEXT,
    VEHICLETYPE TEXT,
    LOCATIONID TEXT,
    STATUS TEXT,
    GENDER TEXT,
    RATING TEXT,
    CREATEDDATE TEXT,
    MODIFIEDDATE TEXT,

    -- Audit columns
    _STG_FILE_NAME TEXT,
    _STG_FILE_LOAD_TS TIMESTAMP,
    _STG_FILE_MD5 TEXT,
    _COPY_DATA_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
COMMENT = 'Raw Delivery Agent staging table';


-- ============================================================
-- STAGE STREAM
-- ============================================================

CREATE OR REPLACE STREAM STAGE_SCH.DELIVERYAGENT_STM
ON TABLE STAGE_SCH.DELIVERYAGENT
APPEND_ONLY = TRUE
COMMENT = 'Append-only stream capturing Delivery Agent staging records';


-- ============================================================
-- CHECK INITIAL FILE
-- ============================================================

LIST @STAGE_SCH.CSV_STG/intial/delivery-agent;


-- ============================================================
-- INITIAL STAGE LOAD
-- ============================================================

COPY INTO STAGE_SCH.DELIVERYAGENT (
    DELIVERYAGENTID,
    NAME,
    PHONE,
    VEHICLETYPE,
    LOCATIONID,
    STATUS,
    GENDER,
    RATING,
    CREATEDDATE,
    MODIFIEDDATE,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)
FROM (
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
        T.$10::TEXT,
        METADATA$FILENAME,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$FILE_CONTENT_KEY,
        CURRENT_TIMESTAMP
    FROM @STAGE_SCH.CSV_STG/intial/delivery-agent/delivery-agent-initial.csv T
)
FILE_FORMAT = (
    FORMAT_NAME = 'STAGE_SCH.CSV_FILE_FORMAT'
)
ON_ERROR = ABORT_STATEMENT;


-- ============================================================
-- INITIAL STAGE VALIDATION
-- ============================================================

SELECT COUNT(*) AS INITIAL_STAGE_ROWS
FROM STAGE_SCH.DELIVERYAGENT;


SELECT *
FROM STAGE_SCH.DELIVERYAGENT
LIMIT 10;


-- ============================================================
-- PART 2 : CLEAN LAYER
-- ============================================================

CREATE OR REPLACE TABLE CLEAN_SCH.DELIVERY_AGENT (
    DELIVERY_AGENT_SK INT AUTOINCREMENT PRIMARY KEY,

    DELIVERY_AGENT_ID INT NOT NULL UNIQUE,
    NAME STRING NOT NULL,
    PHONE STRING NOT NULL,
    VEHICLE_TYPE STRING NOT NULL,
    LOCATION_ID_FK INT,

    STATUS STRING,
    GENDER STRING,
    RATING NUMBER(4,2),

    CREATED_DT TIMESTAMP_NTZ,
    MODIFIED_DT TIMESTAMP_NTZ,

    -- Audit columns
    _STG_FILE_NAME STRING,
    _STG_FILE_LOAD_TS TIMESTAMP_NTZ,
    _STG_FILE_MD5 STRING,
    _COPY_DATA_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
)
COMMENT = 'Clean Delivery Agent table with typed and standardized data';


-- ============================================================
-- INITIAL STAGE → CLEAN
-- ============================================================

MERGE INTO CLEAN_SCH.DELIVERY_AGENT AS TARGET

USING (
    SELECT
        TRY_TO_NUMBER(DELIVERYAGENTID) AS DELIVERY_AGENT_ID,
        TRIM(NAME) AS NAME,
        TRIM(PHONE) AS PHONE,
        TRIM(VEHICLETYPE) AS VEHICLE_TYPE,
        TRY_TO_NUMBER(LOCATIONID) AS LOCATION_ID_FK,
        TRIM(STATUS) AS STATUS,
        TRIM(GENDER) AS GENDER,
        TRY_TO_DECIMAL(RATING, 4, 2) AS RATING,
        TRY_TO_TIMESTAMP(CREATEDDATE) AS CREATED_DT,
        TRY_TO_TIMESTAMP(MODIFIEDDATE) AS MODIFIED_DT,

        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS

    FROM STAGE_SCH.DELIVERYAGENT

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRY_TO_NUMBER(DELIVERYAGENTID)
        ORDER BY _COPY_DATA_TS DESC
    ) = 1

) AS SOURCE

ON TARGET.DELIVERY_AGENT_ID = SOURCE.DELIVERY_AGENT_ID


WHEN MATCHED THEN UPDATE SET

    NAME = SOURCE.NAME,
    PHONE = SOURCE.PHONE,
    VEHICLE_TYPE = SOURCE.VEHICLE_TYPE,
    LOCATION_ID_FK = SOURCE.LOCATION_ID_FK,
    STATUS = SOURCE.STATUS,
    GENDER = SOURCE.GENDER,
    RATING = SOURCE.RATING,
    CREATED_DT = SOURCE.CREATED_DT,
    MODIFIED_DT = SOURCE.MODIFIED_DT,

    _STG_FILE_NAME = SOURCE._STG_FILE_NAME,
    _STG_FILE_LOAD_TS = SOURCE._STG_FILE_LOAD_TS,
    _STG_FILE_MD5 = SOURCE._STG_FILE_MD5,
    _COPY_DATA_TS = CURRENT_TIMESTAMP


WHEN NOT MATCHED THEN INSERT (

    DELIVERY_AGENT_ID,
    NAME,
    PHONE,
    VEHICLE_TYPE,
    LOCATION_ID_FK,
    STATUS,
    GENDER,
    RATING,
    CREATED_DT,
    MODIFIED_DT,

    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS

)

VALUES (

    SOURCE.DELIVERY_AGENT_ID,
    SOURCE.NAME,
    SOURCE.PHONE,
    SOURCE.VEHICLE_TYPE,
    SOURCE.LOCATION_ID_FK,
    SOURCE.STATUS,
    SOURCE.GENDER,
    SOURCE.RATING,
    SOURCE.CREATED_DT,
    SOURCE.MODIFIED_DT,

    SOURCE._STG_FILE_NAME,
    SOURCE._STG_FILE_LOAD_TS,
    SOURCE._STG_FILE_MD5,
    CURRENT_TIMESTAMP
);


-- ============================================================
-- INITIAL CLEAN VALIDATION
-- ============================================================

SELECT COUNT(*) AS CLEAN_DELIVERY_AGENT_ROWS
FROM CLEAN_SCH.DELIVERY_AGENT;


SELECT *
FROM CLEAN_SCH.DELIVERY_AGENT
ORDER BY DELIVERY_AGENT_ID
LIMIT 20;


-- ============================================================
-- IMPORTANT:
-- CREATE CLEAN STREAM AFTER INITIAL CLEAN LOAD
-- ============================================================

CREATE OR REPLACE STREAM CLEAN_SCH.DELIVERY_AGENT_STM
ON TABLE CLEAN_SCH.DELIVERY_AGENT
COMMENT = 'Stream capturing Delivery Agent INSERT, UPDATE and DELETE changes';


-- ============================================================
-- PART 3 : CONSUMPTION / DIMENSION
-- ============================================================

CREATE OR REPLACE TABLE CONSUMPTION_SCH.DELIVERY_AGENT_DIM (
    DELIVERY_AGENT_HK NUMBER PRIMARY KEY,

    DELIVERY_AGENT_ID NUMBER NOT NULL,
    NAME STRING NOT NULL,
    PHONE STRING UNIQUE,
    VEHICLE_TYPE STRING,
    LOCATION_ID_FK NUMBER NOT NULL,
    STATUS STRING,
    GENDER STRING,
    RATING NUMBER(4,2),

    EFF_START_DATE TIMESTAMP_NTZ,
    EFF_END_DATE TIMESTAMP_NTZ,
    IS_CURRENT BOOLEAN
)
COMMENT = 'Delivery Agent dimension implementing SCD Type 2';


-- ============================================================
-- INITIAL CLEAN → DIM
-- ============================================================

INSERT INTO CONSUMPTION_SCH.DELIVERY_AGENT_DIM (

    DELIVERY_AGENT_HK,
    DELIVERY_AGENT_ID,
    NAME,
    PHONE,
    VEHICLE_TYPE,
    LOCATION_ID_FK,
    STATUS,
    GENDER,
    RATING,

    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT

)

SELECT

    HASH(
        DELIVERY_AGENT_ID,
        CURRENT_TIMESTAMP()
    ) AS DELIVERY_AGENT_HK,

    DELIVERY_AGENT_ID,
    NAME,
    PHONE,
    VEHICLE_TYPE,
    LOCATION_ID_FK,
    STATUS,
    GENDER,
    RATING,

    CURRENT_TIMESTAMP(),
    NULL,
    TRUE

FROM CLEAN_SCH.DELIVERY_AGENT;


-- ============================================================
-- INITIAL DIM VALIDATION
-- ============================================================

SELECT
    COUNT(*) AS TOTAL_ROWS,
    COUNT_IF(IS_CURRENT = TRUE) AS CURRENT_ROWS,
    COUNT_IF(IS_CURRENT = FALSE) AS HISTORICAL_ROWS
FROM CONSUMPTION_SCH.DELIVERY_AGENT_DIM;


-- ============================================================
-- PART 4 : DAY-02 STAGE LOAD
-- ============================================================

LIST @STAGE_SCH.CSV_STG/delta/delivery-agent;


COPY INTO STAGE_SCH.DELIVERYAGENT (
    DELIVERYAGENTID,
    NAME,
    PHONE,
    VEHICLETYPE,
    LOCATIONID,
    STATUS,
    GENDER,
    RATING,
    CREATEDDATE,
    MODIFIEDDATE,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)
FROM (
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
        T.$10::TEXT,
        METADATA$FILENAME,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$FILE_CONTENT_KEY,
        CURRENT_TIMESTAMP
    FROM @STAGE_SCH.CSV_STG/delta/delivery-agent/day-02-delivery-agent.csv T
)
FILE_FORMAT = (
    FORMAT_NAME = 'STAGE_SCH.CSV_FILE_FORMAT'
)
ON_ERROR = ABORT_STATEMENT;


-- ============================================================
-- DAY-02 STAGE VERIFICATION
-- ============================================================

SELECT COUNT(*) AS DAY02_STAGE_ROWS
FROM STAGE_SCH.DELIVERYAGENT
WHERE _STG_FILE_NAME ILIKE '%day-02-delivery-agent.csv%';


SELECT *
FROM STAGE_SCH.DELIVERYAGENT
WHERE _STG_FILE_NAME ILIKE '%day-02-delivery-agent.csv%'
ORDER BY DELIVERYAGENTID;


-- ============================================================
-- PART 5 : DAY-02 STAGE → CLEAN
-- ============================================================

MERGE INTO CLEAN_SCH.DELIVERY_AGENT AS TARGET

USING (

    SELECT
        TRY_TO_NUMBER(DELIVERYAGENTID) AS DELIVERY_AGENT_ID,
        TRIM(NAME) AS NAME,
        TRIM(PHONE) AS PHONE,
        TRIM(VEHICLETYPE) AS VEHICLE_TYPE,
        TRY_TO_NUMBER(LOCATIONID) AS LOCATION_ID_FK,
        TRIM(STATUS) AS STATUS,
        TRIM(GENDER) AS GENDER,
        TRY_TO_DECIMAL(RATING, 4, 2) AS RATING,
        TRY_TO_TIMESTAMP(CREATEDDATE) AS CREATED_DT,
        TRY_TO_TIMESTAMP(MODIFIEDDATE) AS MODIFIED_DT,

        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS

    FROM STAGE_SCH.DELIVERYAGENT

    WHERE _STG_FILE_NAME ILIKE '%day-02-delivery-agent.csv%'

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRY_TO_NUMBER(DELIVERYAGENTID)
        ORDER BY _COPY_DATA_TS DESC
    ) = 1

) AS SOURCE

ON TARGET.DELIVERY_AGENT_ID = SOURCE.DELIVERY_AGENT_ID


WHEN MATCHED THEN UPDATE SET

    NAME = SOURCE.NAME,
    PHONE = SOURCE.PHONE,
    VEHICLE_TYPE = SOURCE.VEHICLE_TYPE,
    LOCATION_ID_FK = SOURCE.LOCATION_ID_FK,
    STATUS = SOURCE.STATUS,
    GENDER = SOURCE.GENDER,
    RATING = SOURCE.RATING,
    CREATED_DT = SOURCE.CREATED_DT,
    MODIFIED_DT = SOURCE.MODIFIED_DT,

    _STG_FILE_NAME = SOURCE._STG_FILE_NAME,
    _STG_FILE_LOAD_TS = SOURCE._STG_FILE_LOAD_TS,
    _STG_FILE_MD5 = SOURCE._STG_FILE_MD5,
    _COPY_DATA_TS = CURRENT_TIMESTAMP


WHEN NOT MATCHED THEN INSERT (

    DELIVERY_AGENT_ID,
    NAME,
    PHONE,
    VEHICLE_TYPE,
    LOCATION_ID_FK,
    STATUS,
    GENDER,
    RATING,
    CREATED_DT,
    MODIFIED_DT,

    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS

)

VALUES (

    SOURCE.DELIVERY_AGENT_ID,
    SOURCE.NAME,
    SOURCE.PHONE,
    SOURCE.VEHICLE_TYPE,
    SOURCE.LOCATION_ID_FK,
    SOURCE.STATUS,
    SOURCE.GENDER,
    SOURCE.RATING,
    SOURCE.CREATED_DT,
    SOURCE.MODIFIED_DT,

    SOURCE._STG_FILE_NAME,
    SOURCE._STG_FILE_LOAD_TS,
    SOURCE._STG_FILE_MD5,
    CURRENT_TIMESTAMP
);


-- ============================================================
-- DAY-02 CLEAN VALIDATION
-- ============================================================

SELECT COUNT(*) AS DAY02_CLEAN_ROWS
FROM CLEAN_SCH.DELIVERY_AGENT
WHERE _STG_FILE_NAME ILIKE '%day-02-delivery-agent.csv%';


SELECT *
FROM CLEAN_SCH.DELIVERY_AGENT
WHERE _STG_FILE_NAME ILIKE '%day-02-delivery-agent.csv%'
ORDER BY DELIVERY_AGENT_ID;


-- ============================================================
-- PART 6 : MATERIALIZE CLEAN STREAM
-- ============================================================

CREATE OR REPLACE TEMP TABLE TEMP_DELIVERY_AGENT_CHANGES AS
SELECT *
FROM CLEAN_SCH.DELIVERY_AGENT_STM;


-- Check stream changes before processing
SELECT
    DELIVERY_AGENT_ID,
    METADATA$ACTION,
    METADATA$ISUPDATE
FROM TEMP_DELIVERY_AGENT_CHANGES
ORDER BY DELIVERY_AGENT_ID;


-- ============================================================
-- PART 7 : CLOSE OLD SCD2 RECORDS
-- ============================================================

UPDATE CONSUMPTION_SCH.DELIVERY_AGENT_DIM AS TARGET

SET
    EFF_END_DATE = CURRENT_TIMESTAMP(),
    IS_CURRENT = FALSE

WHERE TARGET.IS_CURRENT = TRUE

AND EXISTS (

    SELECT 1

    FROM TEMP_DELIVERY_AGENT_CHANGES AS SOURCE

    WHERE SOURCE.DELIVERY_AGENT_ID = TARGET.DELIVERY_AGENT_ID

      AND SOURCE.METADATA$ACTION = 'DELETE'
      AND SOURCE.METADATA$ISUPDATE = TRUE
);


-- ============================================================
-- PART 8 : INSERT NEW / UPDATED SCD2 RECORDS
-- ============================================================

INSERT INTO CONSUMPTION_SCH.DELIVERY_AGENT_DIM (

    DELIVERY_AGENT_HK,
    DELIVERY_AGENT_ID,
    NAME,
    PHONE,
    VEHICLE_TYPE,
    LOCATION_ID_FK,
    STATUS,
    GENDER,
    RATING,

    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT

)

SELECT

    HASH(
        DELIVERY_AGENT_ID,
        CURRENT_TIMESTAMP()
    ) AS DELIVERY_AGENT_HK,

    DELIVERY_AGENT_ID,
    NAME,
    PHONE,
    VEHICLE_TYPE,
    LOCATION_ID_FK,
    STATUS,
    GENDER,
    RATING,

    CURRENT_TIMESTAMP(),
    NULL,
    TRUE

FROM TEMP_DELIVERY_AGENT_CHANGES

WHERE METADATA$ACTION = 'INSERT'

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY DELIVERY_AGENT_ID
    ORDER BY _COPY_DATA_TS DESC
) = 1;


-- ============================================================
-- PART 9 : FINAL VALIDATION
-- ============================================================

SELECT
    COUNT(*) AS TOTAL_ROWS,
    COUNT_IF(IS_CURRENT = TRUE) AS CURRENT_ROWS,
    COUNT_IF(IS_CURRENT = FALSE) AS HISTORICAL_ROWS
FROM CONSUMPTION_SCH.DELIVERY_AGENT_DIM;


-- ============================================================
-- CURRENT DELIVERY AGENTS
-- ============================================================

SELECT *
FROM CONSUMPTION_SCH.DELIVERY_AGENT_DIM
WHERE IS_CURRENT = TRUE
ORDER BY DELIVERY_AGENT_ID;


-- ============================================================
-- CHECK DELIVERY AGENTS WITH MULTIPLE SCD2 VERSIONS
-- ============================================================

SELECT
    DELIVERY_AGENT_ID,
    COUNT(*) AS VERSION_COUNT
FROM CONSUMPTION_SCH.DELIVERY_AGENT_DIM
GROUP BY DELIVERY_AGENT_ID
HAVING COUNT(*) > 1
ORDER BY DELIVERY_AGENT_ID;


-- ============================================================
-- CHECK THAT EACH DELIVERY AGENT HAS ONLY ONE CURRENT VERSION
-- ============================================================

SELECT
    DELIVERY_AGENT_ID,
    COUNT_IF(IS_CURRENT = TRUE) AS CURRENT_VERSION_COUNT
FROM CONSUMPTION_SCH.DELIVERY_AGENT_DIM
GROUP BY DELIVERY_AGENT_ID
HAVING COUNT_IF(IS_CURRENT = TRUE) > 1
ORDER BY DELIVERY_AGENT_ID;