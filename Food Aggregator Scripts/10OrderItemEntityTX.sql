-- ============================================================
-- ORDER ITEM ENTITY TRANSACTION PIPELINE
-- ORDERITEMENTITYTX
--
-- FLOW:
-- STAGE → CLEAN
-- INITIAL LOAD → DAY-02 DELTA
--
-- ORDER ITEM DOES NOT SUPPORT SCD2
-- ============================================================


-- ============================================================
-- 1. DATABASE / ROLE
-- ============================================================

USE ROLE SYSADMIN;

USE DATABASE SANDBOX;


-- ============================================================
-- 2. STAGE SCHEMA
-- ============================================================

USE SCHEMA STAGE_SCH;


CREATE OR REPLACE TABLE STAGE_SCH.ORDERITEM (
    
    ORDERITEMID TEXT COMMENT 'Primary Key (Source System)',
    ORDERID TEXT COMMENT 'Order FK (Source System)',
    MENUID TEXT COMMENT 'Menu FK (Source System)',
    QUANTITY TEXT,
    PRICE TEXT,
    SUBTOTAL TEXT,
    CREATEDDATE TEXT,
    MODIFIEDDATE TEXT,

    -- Audit columns
    _STG_FILE_NAME TEXT,
    _STG_FILE_LOAD_TS TIMESTAMP,
    _STG_FILE_MD5 TEXT,
    _COPY_DATA_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP

)
COMMENT = 'Raw Order Item staging table';


-- ============================================================
-- 3. STAGE STREAM
-- ============================================================

CREATE OR REPLACE STREAM STAGE_SCH.ORDERITEM_STM
ON TABLE STAGE_SCH.ORDERITEM
APPEND_ONLY = TRUE
COMMENT = 'Append-only stream capturing Order Item delta data';


-- ============================================================
-- 4. INITIAL FILE CHECK
-- ============================================================

LIST @STAGE_SCH.CSV_STG/intial/order-Item/order-Item-initial.csv;


-- ============================================================
-- 5. INITIAL STAGE LOAD
-- ============================================================

COPY INTO STAGE_SCH.ORDERITEM
(
    ORDERITEMID,
    ORDERID,
    MENUID,
    QUANTITY,
    PRICE,
    SUBTOTAL,
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

        T.$1::TEXT,
        T.$2::TEXT,
        T.$3::TEXT,
        T.$4::TEXT,
        T.$5::TEXT,
        T.$6::TEXT,
        T.$7::TEXT,
        T.$8::TEXT,

        METADATA$FILENAME,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$FILE_CONTENT_KEY,
        CURRENT_TIMESTAMP

    FROM @STAGE_SCH.CSV_STG/intial/order-Item/order-Item-initial.csv T
)

FILE_FORMAT = (
    FORMAT_NAME = 'STAGE_SCH.CSV_FILE_FORMAT'
)

ON_ERROR = ABORT_STATEMENT;


-- ============================================================
-- 6. INITIAL STAGE VALIDATION
-- ============================================================

SELECT COUNT(*) AS INITIAL_STAGE_COUNT
FROM STAGE_SCH.ORDERITEM;


SELECT *
FROM STAGE_SCH.ORDERITEM
ORDER BY TRY_TO_NUMBER(ORDERITEMID);


-- ============================================================
-- 7. CLEAN TABLE
-- ============================================================

CREATE OR REPLACE TABLE CLEAN_SCH.ORDER_ITEM
(

    ORDER_ITEM_SK NUMBER AUTOINCREMENT PRIMARY KEY
        COMMENT 'Surrogate Key (EDW)',

    ORDER_ITEM_ID NUMBER NOT NULL UNIQUE
        COMMENT 'Primary Key (Source System)',

    ORDER_ID_FK NUMBER NOT NULL
        COMMENT 'Order FK (Source System)',

    MENU_ID_FK NUMBER NOT NULL
        COMMENT 'Menu FK (Source System)',

    QUANTITY NUMBER(10,2),

    PRICE NUMBER(10,2),

    SUBTOTAL NUMBER(10,2),

    CREATED_DT TIMESTAMP_NTZ,

    MODIFIED_DT TIMESTAMP_NTZ,

    -- Audit columns
    _STG_FILE_NAME VARCHAR(255),
    _STG_FILE_LOAD_TS TIMESTAMP_NTZ,
    _STG_FILE_MD5 VARCHAR(255),
    _COPY_DATA_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP

)
COMMENT = 'Clean Order Item transactional table';


-- ============================================================
-- 8. INITIAL STAGE → CLEAN
--
-- Deduplication is used to prevent duplicate ORDER_ITEM_ID
-- records from causing MERGE errors.
-- ============================================================

MERGE INTO CLEAN_SCH.ORDER_ITEM AS TARGET

USING
(
    SELECT

        TRY_TO_NUMBER(ORDERITEMID) AS ORDER_ITEM_ID,
        TRY_TO_NUMBER(ORDERID) AS ORDER_ID_FK,
        TRY_TO_NUMBER(MENUID) AS MENU_ID_FK,

        TRY_TO_DECIMAL(QUANTITY, 10, 2) AS QUANTITY,
        TRY_TO_DECIMAL(PRICE, 10, 2) AS PRICE,
        TRY_TO_DECIMAL(SUBTOTAL, 10, 2) AS SUBTOTAL,

        TRY_TO_TIMESTAMP_NTZ(CREATEDDATE) AS CREATED_DT,
        TRY_TO_TIMESTAMP_NTZ(MODIFIEDDATE) AS MODIFIED_DT,

        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS

    FROM STAGE_SCH.ORDERITEM

    QUALIFY ROW_NUMBER() OVER
    (
        PARTITION BY TRY_TO_NUMBER(ORDERITEMID)
        ORDER BY _COPY_DATA_TS DESC
    ) = 1

) AS SOURCE

ON TARGET.ORDER_ITEM_ID = SOURCE.ORDER_ITEM_ID


WHEN MATCHED THEN UPDATE SET

    ORDER_ID_FK = SOURCE.ORDER_ID_FK,
    MENU_ID_FK = SOURCE.MENU_ID_FK,
    QUANTITY = SOURCE.QUANTITY,
    PRICE = SOURCE.PRICE,
    SUBTOTAL = SOURCE.SUBTOTAL,

    CREATED_DT = SOURCE.CREATED_DT,
    MODIFIED_DT = SOURCE.MODIFIED_DT,

    _STG_FILE_NAME = SOURCE._STG_FILE_NAME,
    _STG_FILE_LOAD_TS = SOURCE._STG_FILE_LOAD_TS,
    _STG_FILE_MD5 = SOURCE._STG_FILE_MD5,
    _COPY_DATA_TS = CURRENT_TIMESTAMP


WHEN NOT MATCHED THEN INSERT
(
    ORDER_ITEM_ID,
    ORDER_ID_FK,
    MENU_ID_FK,
    QUANTITY,
    PRICE,
    SUBTOTAL,

    CREATED_DT,
    MODIFIED_DT,

    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)

VALUES
(
    SOURCE.ORDER_ITEM_ID,
    SOURCE.ORDER_ID_FK,
    SOURCE.MENU_ID_FK,
    SOURCE.QUANTITY,
    SOURCE.PRICE,
    SOURCE.SUBTOTAL,

    SOURCE.CREATED_DT,
    SOURCE.MODIFIED_DT,

    SOURCE._STG_FILE_NAME,
    SOURCE._STG_FILE_LOAD_TS,
    SOURCE._STG_FILE_MD5,
    CURRENT_TIMESTAMP
);


-- ============================================================
-- 9. INITIAL CLEAN VALIDATION
-- ============================================================

SELECT COUNT(*) AS INITIAL_CLEAN_COUNT
FROM CLEAN_SCH.ORDER_ITEM;


SELECT *
FROM CLEAN_SCH.ORDER_ITEM
ORDER BY ORDER_ITEM_ID;


-- ============================================================
-- 10. CLEAN STREAM
--
-- IMPORTANT:
-- Create the stream AFTER initial CLEAN loading.
-- Therefore initial records are NOT captured as Day-02 changes.
-- ============================================================

CREATE OR REPLACE STREAM CLEAN_SCH.ORDER_ITEM_STM
ON TABLE CLEAN_SCH.ORDER_ITEM
COMMENT = 'Stream tracking Order Item INSERT, UPDATE and DELETE changes';


-- ============================================================
-- 11. DAY-02 FILE CHECK
-- ============================================================

LIST @STAGE_SCH.CSV_STG/delta/order-item/day-02-order-item.csv;


-- ============================================================
-- 12. DAY-02 STAGE LOAD
-- ============================================================

COPY INTO STAGE_SCH.ORDERITEM
(
    ORDERITEMID,
    ORDERID,
    MENUID,
    QUANTITY,
    PRICE,
    SUBTOTAL,
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

        T.$1::TEXT,
        T.$2::TEXT,
        T.$3::TEXT,
        T.$4::TEXT,
        T.$5::TEXT,
        T.$6::TEXT,
        T.$7::TEXT,
        T.$8::TEXT,

        METADATA$FILENAME,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$FILE_CONTENT_KEY,
        CURRENT_TIMESTAMP

    FROM @STAGE_SCH.CSV_STG/delta/order-item/day-02-order-item.csv T
)

FILE_FORMAT = (
    FORMAT_NAME = 'STAGE_SCH.CSV_FILE_FORMAT'
)

ON_ERROR = ABORT_STATEMENT;


-- ============================================================
-- 13. DAY-02 STAGE VALIDATION
-- ============================================================

SELECT COUNT(*) AS DAY02_STAGE_COUNT
FROM STAGE_SCH.ORDERITEM
WHERE _STG_FILE_NAME ILIKE '%day-02-order-item.csv%';


SELECT *
FROM STAGE_SCH.ORDERITEM
WHERE _STG_FILE_NAME ILIKE '%day-02-order-item.csv%'
ORDER BY TRY_TO_NUMBER(ORDERITEMID);


-- ============================================================
-- 14. DAY-02 STAGE → CLEAN
-- ============================================================

MERGE INTO CLEAN_SCH.ORDER_ITEM AS TARGET

USING
(
    SELECT

        TRY_TO_NUMBER(ORDERITEMID) AS ORDER_ITEM_ID,
        TRY_TO_NUMBER(ORDERID) AS ORDER_ID_FK,
        TRY_TO_NUMBER(MENUID) AS MENU_ID_FK,

        TRY_TO_DECIMAL(QUANTITY, 10, 2) AS QUANTITY,
        TRY_TO_DECIMAL(PRICE, 10, 2) AS PRICE,
        TRY_TO_DECIMAL(SUBTOTAL, 10, 2) AS SUBTOTAL,

        TRY_TO_TIMESTAMP_NTZ(CREATEDDATE) AS CREATED_DT,
        TRY_TO_TIMESTAMP_NTZ(MODIFIEDDATE) AS MODIFIED_DT,

        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS

    FROM STAGE_SCH.ORDERITEM

    WHERE _STG_FILE_NAME ILIKE '%day-02-order-item.csv%'

    QUALIFY ROW_NUMBER() OVER
    (
        PARTITION BY TRY_TO_NUMBER(ORDERITEMID)
        ORDER BY _COPY_DATA_TS DESC
    ) = 1

) AS SOURCE

ON TARGET.ORDER_ITEM_ID = SOURCE.ORDER_ITEM_ID


WHEN MATCHED THEN UPDATE SET

    ORDER_ID_FK = SOURCE.ORDER_ID_FK,
    MENU_ID_FK = SOURCE.MENU_ID_FK,
    QUANTITY = SOURCE.QUANTITY,
    PRICE = SOURCE.PRICE,
    SUBTOTAL = SOURCE.SUBTOTAL,

    CREATED_DT = SOURCE.CREATED_DT,
    MODIFIED_DT = SOURCE.MODIFIED_DT,

    _STG_FILE_NAME = SOURCE._STG_FILE_NAME,
    _STG_FILE_LOAD_TS = SOURCE._STG_FILE_LOAD_TS,
    _STG_FILE_MD5 = SOURCE._STG_FILE_MD5,
    _COPY_DATA_TS = CURRENT_TIMESTAMP


WHEN NOT MATCHED THEN INSERT
(
    ORDER_ITEM_ID,
    ORDER_ID_FK,
    MENU_ID_FK,
    QUANTITY,
    PRICE,
    SUBTOTAL,

    CREATED_DT,
    MODIFIED_DT,

    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)

VALUES
(
    SOURCE.ORDER_ITEM_ID,
    SOURCE.ORDER_ID_FK,
    SOURCE.MENU_ID_FK,
    SOURCE.QUANTITY,
    SOURCE.PRICE,
    SOURCE.SUBTOTAL,

    SOURCE.CREATED_DT,
    SOURCE.MODIFIED_DT,

    SOURCE._STG_FILE_NAME,
    SOURCE._STG_FILE_LOAD_TS,
    SOURCE._STG_FILE_MD5,
    CURRENT_TIMESTAMP
);


-- ============================================================
-- 15. DAY-02 CLEAN VALIDATION
-- ============================================================

SELECT COUNT(*) AS DAY02_CLEAN_COUNT
FROM CLEAN_SCH.ORDER_ITEM
WHERE _STG_FILE_NAME ILIKE '%day-02-order-item.csv%';


SELECT *
FROM CLEAN_SCH.ORDER_ITEM
WHERE _STG_FILE_NAME ILIKE '%day-02-order-item.csv%'
ORDER BY ORDER_ITEM_ID;


-- ============================================================
-- 16. STREAM VALIDATION
-- ============================================================

SELECT
    ORDER_ITEM_ID,
    ORDER_ID_FK,
    MENU_ID_FK,
    QUANTITY,
    PRICE,
    SUBTOTAL,
    METADATA$ACTION,
    METADATA$ISUPDATE
FROM CLEAN_SCH.ORDER_ITEM_STM
ORDER BY ORDER_ITEM_ID;


-- ============================================================
-- 17. FINAL VALIDATION
-- ============================================================

SELECT
    COUNT(*) AS TOTAL_ORDER_ITEMS,
    COUNT(DISTINCT ORDER_ITEM_ID) AS UNIQUE_ORDER_ITEMS,
    COUNT_IF(ORDER_ID_FK IS NULL) AS NULL_ORDER_IDS,
    COUNT_IF(MENU_ID_FK IS NULL) AS NULL_MENU_IDS,
    COUNT_IF(QUANTITY IS NULL) AS NULL_QUANTITY,
    COUNT_IF(PRICE IS NULL) AS NULL_PRICE,
    COUNT_IF(SUBTOTAL IS NULL) AS NULL_SUBTOTAL
FROM CLEAN_SCH.ORDER_ITEM;


-- ============================================================
-- 18. CHECK FOR DUPLICATE ORDER ITEM IDs
-- ============================================================

SELECT
    ORDER_ITEM_ID,
    COUNT(*) AS CNT
FROM CLEAN_SCH.ORDER_ITEM
GROUP BY ORDER_ITEM_ID
HAVING COUNT(*) > 1
ORDER BY ORDER_ITEM_ID;


-- ============================================================
-- 19. FINAL ORDER ITEM DATA
-- ============================================================

SELECT *
FROM CLEAN_SCH.ORDER_ITEM
ORDER BY ORDER_ITEM_ID;


-- ============================================================
-- END OF ORDER ITEM ENTITY TRANSACTION PIPELINE
-- ============================================================