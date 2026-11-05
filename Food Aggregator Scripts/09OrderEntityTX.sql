-- ============================================================
-- ORDERS DATA PIPELINE
-- STAGE → CLEAN → CONSUMPTION
-- INITIAL LOAD + DAY-02 DELTA
-- ============================================================

USE ROLE SYSADMIN;
USE DATABASE SANDBOX;


-- ============================================================
-- 1. STAGE LAYER
-- ============================================================

USE SCHEMA STAGE_SCH;

CREATE OR REPLACE TABLE STAGE_SCH.ORDERS (
    ORDERID TEXT COMMENT 'Primary Key (Source System)',
    CUSTOMERID TEXT COMMENT 'Customer FK (Source System)',
    RESTAURANTID TEXT COMMENT 'Restaurant FK (Source System)',
    ORDERDATE TEXT,
    TOTALAMOUNT TEXT,
    STATUS TEXT,
    PAYMENTMETHOD TEXT,
    CREATEDDATE TEXT,
    MODIFIEDDATE TEXT,

    -- Audit columns
    _STG_FILE_NAME TEXT,
    _STG_FILE_LOAD_TS TIMESTAMP,
    _STG_FILE_MD5 TEXT,
    _COPY_DATA_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
COMMENT = 'Raw Orders staging table';


-- ============================================================
-- 2. STAGE STREAM
-- ============================================================

CREATE OR REPLACE STREAM STAGE_SCH.ORDERS_STM
ON TABLE STAGE_SCH.ORDERS
APPEND_ONLY = TRUE
COMMENT = 'Append-only stream for Orders delta data';


-- ============================================================
-- 3. INITIAL FILE CHECK
-- ============================================================

LIST @STAGE_SCH.CSV_STG/intial/orders;


-- ============================================================
-- 4. INITIAL STAGE LOAD
-- ============================================================

COPY INTO STAGE_SCH.ORDERS (
    ORDERID,
    CUSTOMERID,
    RESTAURANTID,
    ORDERDATE,
    TOTALAMOUNT,
    STATUS,
    PAYMENTMETHOD,
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
        METADATA$FILENAME,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$FILE_CONTENT_KEY,
        CURRENT_TIMESTAMP
    FROM @STAGE_SCH.CSV_STG/intial/orders/orders-initial.csv T
)
FILE_FORMAT = (
    FORMAT_NAME = 'STAGE_SCH.CSV_FILE_FORMAT'
)
ON_ERROR = ABORT_STATEMENT;


-- ============================================================
-- 5. CLEAN TABLE
-- ============================================================

CREATE OR REPLACE TABLE CLEAN_SCH.ORDERS (
    ORDER_SK NUMBER AUTOINCREMENT PRIMARY KEY
        COMMENT 'Surrogate Key (EDW)',

    ORDER_ID BIGINT UNIQUE
        COMMENT 'Primary Key (Source System)',

    CUSTOMER_ID_FK BIGINT
        COMMENT 'Customer FK (Source System)',

    RESTAURANT_ID_FK BIGINT
        COMMENT 'Restaurant FK (Source System)',

    ORDER_DATE TIMESTAMP_NTZ,

    TOTAL_AMOUNT DECIMAL(10,2),

    STATUS STRING,

    PAYMENT_METHOD STRING,

    CREATED_DT TIMESTAMP_TZ,

    MODIFIED_DT TIMESTAMP_TZ,

    -- Audit columns
    _STG_FILE_NAME STRING,
    _STG_FILE_LOAD_TS TIMESTAMP_NTZ,
    _STG_FILE_MD5 STRING,
    _COPY_DATA_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
)
COMMENT = 'Clean Orders table with typed and standardized data';


-- ============================================================
-- 6. INITIAL STAGE → CLEAN
--
-- IMPORTANT:
-- Deduplication prevents duplicate ORDER_ID errors during MERGE.
-- ============================================================

MERGE INTO CLEAN_SCH.ORDERS AS TARGET

USING (
    SELECT
        TRY_TO_NUMBER(ORDERID) AS ORDER_ID,
        TRY_TO_NUMBER(CUSTOMERID) AS CUSTOMER_ID_FK,
        TRY_TO_NUMBER(RESTAURANTID) AS RESTAURANT_ID_FK,

        TRY_TO_TIMESTAMP_NTZ(ORDERDATE) AS ORDER_DATE,

        TRY_TO_DECIMAL(TOTALAMOUNT, 10, 2) AS TOTAL_AMOUNT,

        TRIM(STATUS) AS STATUS,
        TRIM(PAYMENTMETHOD) AS PAYMENT_METHOD,

        TRY_TO_TIMESTAMP_TZ(CREATEDDATE) AS CREATED_DT,
        TRY_TO_TIMESTAMP_TZ(MODIFIEDDATE) AS MODIFIED_DT,

        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS

    FROM STAGE_SCH.ORDERS

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRY_TO_NUMBER(ORDERID)
        ORDER BY _COPY_DATA_TS DESC
    ) = 1

) AS SOURCE

ON TARGET.ORDER_ID = SOURCE.ORDER_ID


WHEN MATCHED THEN UPDATE SET

    CUSTOMER_ID_FK = SOURCE.CUSTOMER_ID_FK,
    RESTAURANT_ID_FK = SOURCE.RESTAURANT_ID_FK,
    ORDER_DATE = SOURCE.ORDER_DATE,
    TOTAL_AMOUNT = SOURCE.TOTAL_AMOUNT,
    STATUS = SOURCE.STATUS,
    PAYMENT_METHOD = SOURCE.PAYMENT_METHOD,
    CREATED_DT = SOURCE.CREATED_DT,
    MODIFIED_DT = SOURCE.MODIFIED_DT,

    _STG_FILE_NAME = SOURCE._STG_FILE_NAME,
    _STG_FILE_LOAD_TS = SOURCE._STG_FILE_LOAD_TS,
    _STG_FILE_MD5 = SOURCE._STG_FILE_MD5,
    _COPY_DATA_TS = CURRENT_TIMESTAMP


WHEN NOT MATCHED THEN INSERT (
    ORDER_ID,
    CUSTOMER_ID_FK,
    RESTAURANT_ID_FK,
    ORDER_DATE,
    TOTAL_AMOUNT,
    STATUS,
    PAYMENT_METHOD,
    CREATED_DT,
    MODIFIED_DT,

    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)

VALUES (
    SOURCE.ORDER_ID,
    SOURCE.CUSTOMER_ID_FK,
    SOURCE.RESTAURANT_ID_FK,
    SOURCE.ORDER_DATE,
    SOURCE.TOTAL_AMOUNT,
    SOURCE.STATUS,
    SOURCE.PAYMENT_METHOD,
    SOURCE.CREATED_DT,
    SOURCE.MODIFIED_DT,

    SOURCE._STG_FILE_NAME,
    SOURCE._STG_FILE_LOAD_TS,
    SOURCE._STG_FILE_MD5,
    CURRENT_TIMESTAMP
);


-- ============================================================
-- 7. INITIAL CLEAN VALIDATION
-- ============================================================

SELECT COUNT(*) AS CLEAN_ORDER_COUNT
FROM CLEAN_SCH.ORDERS;

SELECT *
FROM CLEAN_SCH.ORDERS
ORDER BY ORDER_ID
LIMIT 20;


-- ============================================================
-- 8. CLEAN STREAM
--
-- IMPORTANT:
-- Create the stream AFTER the initial CLEAN load.
-- Therefore, initial records are not treated as Day-02 changes.
-- ============================================================

CREATE OR REPLACE STREAM CLEAN_SCH.ORDERS_STM
ON TABLE CLEAN_SCH.ORDERS
COMMENT = 'Stream tracking Orders INSERT, UPDATE and DELETE changes';


-- ============================================================
-- 9. DAY-02 FILE CHECK
-- ============================================================

LIST @STAGE_SCH.CSV_STG/delta/orders/day-02-orders.csv;


-- ============================================================
-- 10. DAY-02 STAGE LOAD
-- ============================================================

COPY INTO STAGE_SCH.ORDERS (
    ORDERID,
    CUSTOMERID,
    RESTAURANTID,
    ORDERDATE,
    TOTALAMOUNT,
    STATUS,
    PAYMENTMETHOD,
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
        METADATA$FILENAME,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$FILE_CONTENT_KEY,
        CURRENT_TIMESTAMP
    FROM @STAGE_SCH.CSV_STG/delta/orders/day-02-orders.csv T
)
FILE_FORMAT = (
    FORMAT_NAME = 'STAGE_SCH.CSV_FILE_FORMAT'
)
ON_ERROR = ABORT_STATEMENT;


-- ============================================================
-- 11. DAY-02 STAGE VALIDATION
-- ============================================================

SELECT COUNT(*) AS DAY02_STAGE_ROWS
FROM STAGE_SCH.ORDERS
WHERE _STG_FILE_NAME ILIKE '%day-02-orders.csv%';

SELECT *
FROM STAGE_SCH.ORDERS
WHERE _STG_FILE_NAME ILIKE '%day-02-orders.csv%'
ORDER BY TRY_TO_NUMBER(ORDERID);


-- ============================================================
-- 12. DAY-02 STAGE → CLEAN
-- ============================================================

MERGE INTO CLEAN_SCH.ORDERS AS TARGET

USING (
    SELECT
        TRY_TO_NUMBER(ORDERID) AS ORDER_ID,
        TRY_TO_NUMBER(CUSTOMERID) AS CUSTOMER_ID_FK,
        TRY_TO_NUMBER(RESTAURANTID) AS RESTAURANT_ID_FK,

        TRY_TO_TIMESTAMP_NTZ(ORDERDATE) AS ORDER_DATE,

        TRY_TO_DECIMAL(TOTALAMOUNT, 10, 2) AS TOTAL_AMOUNT,

        TRIM(STATUS) AS STATUS,
        TRIM(PAYMENTMETHOD) AS PAYMENT_METHOD,

        TRY_TO_TIMESTAMP_TZ(CREATEDDATE) AS CREATED_DT,
        TRY_TO_TIMESTAMP_TZ(MODIFIEDDATE) AS MODIFIED_DT,

        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS

    FROM STAGE_SCH.ORDERS

    WHERE _STG_FILE_NAME ILIKE '%day-02-orders.csv%'

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRY_TO_NUMBER(ORDERID)
        ORDER BY _COPY_DATA_TS DESC
    ) = 1

) AS SOURCE

ON TARGET.ORDER_ID = SOURCE.ORDER_ID


WHEN MATCHED THEN UPDATE SET

    CUSTOMER_ID_FK = SOURCE.CUSTOMER_ID_FK,
    RESTAURANT_ID_FK = SOURCE.RESTAURANT_ID_FK,
    ORDER_DATE = SOURCE.ORDER_DATE,
    TOTAL_AMOUNT = SOURCE.TOTAL_AMOUNT,
    STATUS = SOURCE.STATUS,
    PAYMENT_METHOD = SOURCE.PAYMENT_METHOD,
    CREATED_DT = SOURCE.CREATED_DT,
    MODIFIED_DT = SOURCE.MODIFIED_DT,

    _STG_FILE_NAME = SOURCE._STG_FILE_NAME,
    _STG_FILE_LOAD_TS = SOURCE._STG_FILE_LOAD_TS,
    _STG_FILE_MD5 = SOURCE._STG_FILE_MD5,
    _COPY_DATA_TS = CURRENT_TIMESTAMP


WHEN NOT MATCHED THEN INSERT (
    ORDER_ID,
    CUSTOMER_ID_FK,
    RESTAURANT_ID_FK,
    ORDER_DATE,
    TOTAL_AMOUNT,
    STATUS,
    PAYMENT_METHOD,
    CREATED_DT,
    MODIFIED_DT,

    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)

VALUES (
    SOURCE.ORDER_ID,
    SOURCE.CUSTOMER_ID_FK,
    SOURCE.RESTAURANT_ID_FK,
    SOURCE.ORDER_DATE,
    SOURCE.TOTAL_AMOUNT,
    SOURCE.STATUS,
    SOURCE.PAYMENT_METHOD,
    SOURCE.CREATED_DT,
    SOURCE.MODIFIED_DT,

    SOURCE._STG_FILE_NAME,
    SOURCE._STG_FILE_LOAD_TS,
    SOURCE._STG_FILE_MD5,
    CURRENT_TIMESTAMP
);


-- ============================================================
-- 13. DAY-02 CLEAN VALIDATION
-- ============================================================

SELECT COUNT(*) AS DAY02_CLEAN_ROWS
FROM CLEAN_SCH.ORDERS
WHERE _STG_FILE_NAME ILIKE '%day-02-orders.csv%';

SELECT *
FROM CLEAN_SCH.ORDERS
WHERE _STG_FILE_NAME ILIKE '%day-02-orders.csv%'
ORDER BY ORDER_ID;


-- ============================================================
-- 14. IMPORTANT STREAM NOTE
--
-- ORDERS IS NOT AN SCD2 ENTITY.
-- Therefore, no CONSUMPTION SCD2 table is required here.
--
-- CLEAN_SCH.ORDERS is the final cleaned transactional table.
-- ============================================================


-- ============================================================
-- 15. FINAL VALIDATION
-- ============================================================

SELECT
    COUNT(*) AS TOTAL_ORDERS,
    COUNT(DISTINCT ORDER_ID) AS UNIQUE_ORDERS,
    COUNT_IF(STATUS IS NULL) AS NULL_STATUS,
    COUNT_IF(TOTAL_AMOUNT IS NULL) AS NULL_TOTAL_AMOUNT
FROM CLEAN_SCH.ORDERS;


-- ============================================================
-- 16. CHECK DAY-02 ORDERS
-- ============================================================

SELECT
    ORDER_ID,
    CUSTOMER_ID_FK,
    RESTAURANT_ID_FK,
    ORDER_DATE,
    TOTAL_AMOUNT,
    STATUS,
    PAYMENT_METHOD,
    CREATED_DT,
    MODIFIED_DT
FROM CLEAN_SCH.ORDERS
WHERE _STG_FILE_NAME ILIKE '%day-02-orders.csv%'
ORDER BY ORDER_ID;


-- ============================================================
-- 17. CHECK FOR DUPLICATE ORDER IDs
-- ============================================================

SELECT
    ORDER_ID,
    COUNT(*) AS CNT
FROM CLEAN_SCH.ORDERS
GROUP BY ORDER_ID
HAVING COUNT(*) > 1
ORDER BY ORDER_ID;


-- ============================================================
-- END OF ORDERS PIPELINE
-- ============================================================