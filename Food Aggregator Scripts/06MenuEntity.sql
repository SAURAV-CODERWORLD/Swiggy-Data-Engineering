-- ============================================================
-- MENU DATA PIPELINE
-- STAGE → CLEAN → CONSUMPTION
-- SCD TYPE 2 IMPLEMENTATION
-- ============================================================

USE ROLE SYSADMIN;
USE DATABASE SANDBOX;


-- ============================================================
-- PART 1 : STAGE LAYER
-- ============================================================

USE SCHEMA STAGE_SCH;

CREATE OR REPLACE TABLE STAGE_SCH.MENU (
    MENUID TEXT COMMENT 'Primary Key (Source System)',
    RESTAURANTID TEXT COMMENT 'Restaurant FK (Source System)',
    ITEMNAME TEXT,
    DESCRIPTION TEXT,
    PRICE TEXT,
    CATEGORY TEXT,
    AVAILABILITY TEXT,
    ITEMTYPE TEXT,
    CREATEDDATE TEXT,
    MODIFIEDDATE TEXT,

    -- Audit columns
    _STG_FILE_NAME TEXT,
    _STG_FILE_LOAD_TS TIMESTAMP,
    _STG_FILE_MD5 TEXT,
    _COPY_DATA_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
COMMENT = 'Raw Menu staging table. Source data is stored as TEXT with audit columns.';


-- ============================================================
-- STAGE STREAM
-- Created before loading so STAGE changes can be tracked
-- ============================================================

CREATE OR REPLACE STREAM STAGE_SCH.MENU_STM
ON TABLE STAGE_SCH.MENU
APPEND_ONLY = TRUE
COMMENT = 'Append-only stream capturing Menu records loaded into STAGE';


-- ============================================================
-- CHECK INITIAL FILE
-- ============================================================

LIST @STAGE_SCH.CSV_STG/intial/menu;


-- ============================================================
-- INITIAL MENU LOAD : STAGE
-- ============================================================

COPY INTO STAGE_SCH.MENU (
    MENUID,
    RESTAURANTID,
    ITEMNAME,
    DESCRIPTION,
    PRICE,
    CATEGORY,
    AVAILABILITY,
    ITEMTYPE,
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
    FROM @STAGE_SCH.CSV_STG/intial/menu/menu-initial-load.csv T
)
FILE_FORMAT = (
    FORMAT_NAME = 'STAGE_SCH.CSV_FILE_FORMAT'
)
ON_ERROR = ABORT_STATEMENT;


-- ============================================================
-- INITIAL STAGE VALIDATION
-- ============================================================

SELECT COUNT(*) AS STAGE_MENU_COUNT
FROM STAGE_SCH.MENU;


SELECT *
FROM STAGE_SCH.MENU
LIMIT 10;


-- ============================================================
-- PART 2 : CLEAN LAYER
-- ============================================================

CREATE OR REPLACE TABLE CLEAN_SCH.MENU (
    MENU_SK INT AUTOINCREMENT PRIMARY KEY,

    MENU_ID INT NOT NULL UNIQUE,
    RESTAURANT_ID_FK INT,

    ITEM_NAME STRING NOT NULL,
    DESCRIPTION STRING NOT NULL,
    PRICE DECIMAL(10,2) NOT NULL,

    CATEGORY STRING,
    AVAILABILITY BOOLEAN,
    ITEM_TYPE STRING,

    CREATED_DT TIMESTAMP_NTZ,
    MODIFIED_DT TIMESTAMP_NTZ,

    -- Audit columns
    _STG_FILE_NAME STRING,
    _STG_FILE_LOAD_TS TIMESTAMP_NTZ,
    _STG_FILE_MD5 STRING,
    _COPY_DATA_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
)
COMMENT = 'Clean Menu table with typed and standardized data.';


-- ============================================================
-- INITIAL STAGE → CLEAN LOAD
-- ============================================================

MERGE INTO CLEAN_SCH.MENU AS TARGET

USING (
    SELECT
        TRY_CAST(MENUID AS INT) AS MENU_ID,
        TRY_CAST(RESTAURANTID AS INT) AS RESTAURANT_ID_FK,

        TRIM(ITEMNAME) AS ITEM_NAME,
        TRIM(DESCRIPTION) AS DESCRIPTION,

        TRY_CAST(PRICE AS DECIMAL(10,2)) AS PRICE,

        TRIM(CATEGORY) AS CATEGORY,

        CASE
            WHEN LOWER(TRIM(AVAILABILITY)) = 'true' THEN TRUE
            WHEN LOWER(TRIM(AVAILABILITY)) = 'false' THEN FALSE
            ELSE NULL
        END AS AVAILABILITY,

        TRIM(ITEMTYPE) AS ITEM_TYPE,

        TRY_CAST(CREATEDDATE AS TIMESTAMP_NTZ) AS CREATED_DT,
        TRY_CAST(MODIFIEDDATE AS TIMESTAMP_NTZ) AS MODIFIED_DT,

        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS

    FROM STAGE_SCH.MENU

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRY_CAST(MENUID AS INT)
        ORDER BY _COPY_DATA_TS DESC
    ) = 1

) AS SOURCE

ON TARGET.MENU_ID = SOURCE.MENU_ID


WHEN MATCHED THEN UPDATE SET

    RESTAURANT_ID_FK = SOURCE.RESTAURANT_ID_FK,
    ITEM_NAME = SOURCE.ITEM_NAME,
    DESCRIPTION = SOURCE.DESCRIPTION,
    PRICE = SOURCE.PRICE,
    CATEGORY = SOURCE.CATEGORY,
    AVAILABILITY = SOURCE.AVAILABILITY,
    ITEM_TYPE = SOURCE.ITEM_TYPE,
    CREATED_DT = SOURCE.CREATED_DT,
    MODIFIED_DT = SOURCE.MODIFIED_DT,

    _STG_FILE_NAME = SOURCE._STG_FILE_NAME,
    _STG_FILE_LOAD_TS = SOURCE._STG_FILE_LOAD_TS,
    _STG_FILE_MD5 = SOURCE._STG_FILE_MD5,
    _COPY_DATA_TS = CURRENT_TIMESTAMP


WHEN NOT MATCHED THEN INSERT (

    MENU_ID,
    RESTAURANT_ID_FK,
    ITEM_NAME,
    DESCRIPTION,
    PRICE,
    CATEGORY,
    AVAILABILITY,
    ITEM_TYPE,
    CREATED_DT,
    MODIFIED_DT,

    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS

)

VALUES (

    SOURCE.MENU_ID,
    SOURCE.RESTAURANT_ID_FK,
    SOURCE.ITEM_NAME,
    SOURCE.DESCRIPTION,
    SOURCE.PRICE,
    SOURCE.CATEGORY,
    SOURCE.AVAILABILITY,
    SOURCE.ITEM_TYPE,
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

SELECT COUNT(*) AS CLEAN_MENU_COUNT
FROM CLEAN_SCH.MENU;


SELECT *
FROM CLEAN_SCH.MENU
ORDER BY MENU_ID
LIMIT 20;


-- ============================================================
-- IMPORTANT:
-- CREATE CLEAN STREAM AFTER INITIAL CLEAN LOAD
-- So initial records are NOT treated as Day-02 changes.
-- ============================================================

CREATE OR REPLACE STREAM CLEAN_SCH.MENU_STM
ON TABLE CLEAN_SCH.MENU
COMMENT = 'Stream capturing Menu INSERT, UPDATE and DELETE changes';


-- ============================================================
-- PART 3 : CONSUMPTION / MENU DIMENSION
-- ============================================================

CREATE OR REPLACE TABLE CONSUMPTION_SCH.MENU_DIM (

    MENU_DIM_HK NUMBER PRIMARY KEY,

    MENU_ID INT NOT NULL,
    RESTAURANT_ID_FK INT NOT NULL,

    ITEM_NAME STRING,
    DESCRIPTION STRING,
    PRICE DECIMAL(10,2),
    CATEGORY STRING,
    AVAILABILITY BOOLEAN,
    ITEM_TYPE STRING,

    EFF_START_DATE TIMESTAMP_NTZ,
    EFF_END_DATE TIMESTAMP_NTZ,
    IS_CURRENT BOOLEAN
)
COMMENT = 'Menu dimension implementing SCD Type 2 history.';


-- ============================================================
-- INITIAL CLEAN → DIM LOAD
-- ============================================================

INSERT INTO CONSUMPTION_SCH.MENU_DIM (

    MENU_DIM_HK,
    MENU_ID,
    RESTAURANT_ID_FK,
    ITEM_NAME,
    DESCRIPTION,
    PRICE,
    CATEGORY,
    AVAILABILITY,
    ITEM_TYPE,

    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT

)

SELECT

    HASH(
        MENU_ID,
        CURRENT_TIMESTAMP()
    ) AS MENU_DIM_HK,

    MENU_ID,
    RESTAURANT_ID_FK,
    ITEM_NAME,
    DESCRIPTION,
    PRICE,
    CATEGORY,
    AVAILABILITY,
    ITEM_TYPE,

    CURRENT_TIMESTAMP() AS EFF_START_DATE,
    NULL AS EFF_END_DATE,
    TRUE AS IS_CURRENT

FROM CLEAN_SCH.MENU;


-- ============================================================
-- INITIAL DIM VALIDATION
-- ============================================================

SELECT
    COUNT(*) AS TOTAL_ROWS,
    COUNT_IF(IS_CURRENT = TRUE) AS CURRENT_ROWS,
    COUNT_IF(IS_CURRENT = FALSE) AS HISTORICAL_ROWS
FROM CONSUMPTION_SCH.MENU_DIM;


-- ============================================================
-- PART 4 : DAY-02 STAGE LOAD
-- ============================================================

LIST @STAGE_SCH.CSV_STG/delta/menu;


COPY INTO STAGE_SCH.MENU (
    MENUID,
    RESTAURANTID,
    ITEMNAME,
    DESCRIPTION,
    PRICE,
    CATEGORY,
    AVAILABILITY,
    ITEMTYPE,
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
    FROM @STAGE_SCH.CSV_STG/delta/menu/day-02-menu-data.csv T
)
FILE_FORMAT = (
    FORMAT_NAME = 'STAGE_SCH.CSV_FILE_FORMAT'
)
ON_ERROR = ABORT_STATEMENT;


-- ============================================================
-- DAY-02 STAGE VERIFICATION
-- ============================================================

SELECT COUNT(*) AS DAY02_STAGE_ROWS
FROM STAGE_SCH.MENU
WHERE _STG_FILE_NAME ILIKE '%day-02-menu-data.csv%';


SELECT *
FROM STAGE_SCH.MENU
WHERE _STG_FILE_NAME ILIKE '%day-02-menu-data.csv%'
ORDER BY MENUID;


-- ============================================================
-- PART 5 : DAY-02 STAGE → CLEAN
-- ============================================================

MERGE INTO CLEAN_SCH.MENU AS TARGET

USING (

    SELECT
        TRY_CAST(MENUID AS INT) AS MENU_ID,
        TRY_CAST(RESTAURANTID AS INT) AS RESTAURANT_ID_FK,

        TRIM(ITEMNAME) AS ITEM_NAME,
        TRIM(DESCRIPTION) AS DESCRIPTION,

        TRY_CAST(PRICE AS DECIMAL(10,2)) AS PRICE,

        TRIM(CATEGORY) AS CATEGORY,

        CASE
            WHEN LOWER(TRIM(AVAILABILITY)) = 'true' THEN TRUE
            WHEN LOWER(TRIM(AVAILABILITY)) = 'false' THEN FALSE
            ELSE NULL
        END AS AVAILABILITY,

        TRIM(ITEMTYPE) AS ITEM_TYPE,

        TRY_CAST(CREATEDDATE AS TIMESTAMP_NTZ) AS CREATED_DT,
        TRY_CAST(MODIFIEDDATE AS TIMESTAMP_NTZ) AS MODIFIED_DT,

        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS

    FROM STAGE_SCH.MENU

    WHERE _STG_FILE_NAME ILIKE '%day-02-menu-data.csv%'

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRY_CAST(MENUID AS INT)
        ORDER BY _COPY_DATA_TS DESC
    ) = 1

) AS SOURCE

ON TARGET.MENU_ID = SOURCE.MENU_ID


WHEN MATCHED THEN UPDATE SET

    RESTAURANT_ID_FK = SOURCE.RESTAURANT_ID_FK,
    ITEM_NAME = SOURCE.ITEM_NAME,
    DESCRIPTION = SOURCE.DESCRIPTION,
    PRICE = SOURCE.PRICE,
    CATEGORY = SOURCE.CATEGORY,
    AVAILABILITY = SOURCE.AVAILABILITY,
    ITEM_TYPE = SOURCE.ITEM_TYPE,

    CREATED_DT = SOURCE.CREATED_DT,
    MODIFIED_DT = SOURCE.MODIFIED_DT,

    _STG_FILE_NAME = SOURCE._STG_FILE_NAME,
    _STG_FILE_LOAD_TS = SOURCE._STG_FILE_LOAD_TS,
    _STG_FILE_MD5 = SOURCE._STG_FILE_MD5,
    _COPY_DATA_TS = CURRENT_TIMESTAMP


WHEN NOT MATCHED THEN INSERT (

    MENU_ID,
    RESTAURANT_ID_FK,
    ITEM_NAME,
    DESCRIPTION,
    PRICE,
    CATEGORY,
    AVAILABILITY,
    ITEM_TYPE,

    CREATED_DT,
    MODIFIED_DT,

    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS

)

VALUES (

    SOURCE.MENU_ID,
    SOURCE.RESTAURANT_ID_FK,
    SOURCE.ITEM_NAME,
    SOURCE.DESCRIPTION,
    SOURCE.PRICE,
    SOURCE.CATEGORY,
    SOURCE.AVAILABILITY,
    SOURCE.ITEM_TYPE,

    SOURCE.CREATED_DT,
    SOURCE.MODIFIED_DT,

    SOURCE._STG_FILE_NAME,
    SOURCE._STG_FILE_LOAD_TS,
    SOURCE._STG_FILE_MD5,
    CURRENT_TIMESTAMP
);


-- ============================================================
-- DAY-02 CLEAN VERIFICATION
-- ============================================================

SELECT COUNT(*) AS DAY02_CLEAN_ROWS
FROM CLEAN_SCH.MENU
WHERE _STG_FILE_NAME ILIKE '%day-02-menu-data.csv%';


SELECT *
FROM CLEAN_SCH.MENU
WHERE _STG_FILE_NAME ILIKE '%day-02-menu-data.csv%'
ORDER BY MENU_ID;


-- ============================================================
-- PART 6 : MATERIALIZE STREAM CHANGES
-- IMPORTANT:
-- This prevents the stream from being consumed multiple times.
-- ============================================================

CREATE OR REPLACE TEMP TABLE TEMP_MENU_CHANGES AS
SELECT *
FROM CLEAN_SCH.MENU_STM;


SELECT
    MENU_ID,
    METADATA$ACTION,
    METADATA$ISUPDATE
FROM TEMP_MENU_CHANGES
ORDER BY MENU_ID;


-- ============================================================
-- PART 7 : CLOSE OLD SCD2 VERSIONS
-- Only existing/current Menu IDs that were UPDATED
-- ============================================================

UPDATE CONSUMPTION_SCH.MENU_DIM AS TARGET

SET
    EFF_END_DATE = CURRENT_TIMESTAMP(),
    IS_CURRENT = FALSE

WHERE TARGET.IS_CURRENT = TRUE

AND EXISTS (

    SELECT 1

    FROM TEMP_MENU_CHANGES AS SOURCE

    WHERE SOURCE.MENU_ID = TARGET.MENU_ID

      AND SOURCE.METADATA$ACTION = 'DELETE'
      AND SOURCE.METADATA$ISUPDATE = TRUE
);


-- ============================================================
-- PART 8 : INSERT NEW / UPDATED SCD2 VERSIONS
-- ============================================================

INSERT INTO CONSUMPTION_SCH.MENU_DIM (

    MENU_DIM_HK,
    MENU_ID,
    RESTAURANT_ID_FK,
    ITEM_NAME,
    DESCRIPTION,
    PRICE,
    CATEGORY,
    AVAILABILITY,
    ITEM_TYPE,

    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT

)

SELECT

    HASH(
        MENU_ID,
        CURRENT_TIMESTAMP()
    ) AS MENU_DIM_HK,

    MENU_ID,
    RESTAURANT_ID_FK,
    ITEM_NAME,
    DESCRIPTION,
    PRICE,
    CATEGORY,
    AVAILABILITY,
    ITEM_TYPE,

    CURRENT_TIMESTAMP(),
    NULL,
    TRUE

FROM TEMP_MENU_CHANGES

WHERE METADATA$ACTION = 'INSERT'

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY MENU_ID
    ORDER BY _COPY_DATA_TS DESC
) = 1;


-- ============================================================
-- PART 9 : FINAL MENU DIMENSION VERIFICATION
-- ============================================================

SELECT
    COUNT(*) AS TOTAL_ROWS,
    COUNT_IF(IS_CURRENT = TRUE) AS CURRENT_ROWS,
    COUNT_IF(IS_CURRENT = FALSE) AS HISTORICAL_ROWS
FROM CONSUMPTION_SCH.MENU_DIM;


-- ============================================================
-- CURRENT MENU RECORDS
-- ============================================================

SELECT *
FROM CONSUMPTION_SCH.MENU_DIM
WHERE IS_CURRENT = TRUE
ORDER BY MENU_ID;


-- ============================================================
-- CHECK MENU IDs WITH MULTIPLE SCD2 VERSIONS historical record check
-- ============================================================

SELECT
    MENU_ID,
    COUNT(*) AS VERSION_COUNT
FROM CONSUMPTION_SCH.MENU_DIM
GROUP BY MENU_ID
HAVING COUNT(*) > 1
ORDER BY MENU_ID;


-- ============================================================
-- CHECK CURRENT VERSION COUNT PER MENU ID historical record check but new id are provided
-- ============================================================

SELECT
    MENU_ID,
    COUNT_IF(IS_CURRENT = TRUE) AS CURRENT_VERSION_COUNT
FROM CONSUMPTION_SCH.MENU_DIM
GROUP BY MENU_ID
HAVING COUNT_IF(IS_CURRENT = TRUE) > 1
ORDER BY MENU_ID;

SELECT
    COUNT(*) AS TOTAL_ROWS,
    COUNT_IF(IS_CURRENT = TRUE) AS CURRENT_ROWS,
    COUNT_IF(IS_CURRENT = FALSE) AS HISTORICAL_ROWS
FROM CONSUMPTION_SCH.MENU_DIM;

SELECT *
FROM CONSUMPTION_SCH.MENU_DIM
ORDER BY MENU_ID;

SELECT
    c.MENU_ID,
    c.ITEM_NAME,
    c.PRICE
FROM CLEAN_SCH.MENU c
WHERE c._STG_FILE_NAME ILIKE '%day-02-menu-data.csv%'
ORDER BY c.MENU_ID;

SELECT
    COUNT(*) AS TOTAL_ROWS,
    COUNT_IF(IS_CURRENT = TRUE) AS CURRENT_ROWS,
    COUNT_IF(IS_CURRENT = FALSE) AS HISTORICAL_ROWS
FROM CONSUMPTION_SCH.MENU_DIM;

SELECT
    MENU_ID,
    RESTAURANT_ID_FK,
    ITEM_NAME,
    PRICE,
    IS_CURRENT,
    EFF_START_DATE,
    EFF_END_DATE
FROM CONSUMPTION_SCH.MENU_DIM
ORDER BY MENU_ID;