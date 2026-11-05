use role sysadmin;

create warehouse if not exists adhoc_wh
    comment = 'This is the adhoc-wh'
    warehouse_size = 'x-small'
    auto_resume = true
    auto_suspend = 60
     enable_query_acceleration = false 
     warehouse_type = 'standard' 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     initially_suspended = true;


create database if not exists sandbox;
use database sandbox;
create schema if not exists stage_sch;
create schema if not exists clean_sch;
create schema if not exists consumption_sch;
create schema if not exists common;

use schema stage_sch;
-- create file format to process the csv file

  create file format if not exists stage_sch.csv_file_format 
        type = 'csv' 
        compression = 'auto' 
        field_delimiter = ',' 
        record_delimiter = '\n' SANDBOX.STAGE_SCH.CSV_FILE_FORMAT
        skip_header = 1 
        field_optionally_enclosed_by = '\042' 
        null_if = ('\\N');



create stage stage_sch.csv_stg
    directory = ( enable = true )
    comment = 'this is the snowflake internal stage';


create or replace tag 
    common.pii_policy_tag 
    allowed_values 'PII','PRICE','SENSITIVE','EMAIL'
    comment = 'This is PII policy tag object';

create or replace masking policy 
    common.pii_masking_policy as (pii_text string)
    returns string -> 
    to_varchar('** PII **');

create or replace masking policy 
    common.email_masking_policy as (email_text string)
    returns string -> 
    to_varchar('** EAMIL **');

create or replace masking policy 
    common.phone_masking_policy as (phone string)
    returns string -> 
    to_varchar('** Phone **');SANDBOX.STAGE_SCH.CSV_FILE_FORMATSANDBOX.STAGE_SCH.CSV_FILE_FORMATSANDBOX.STAGE_SCH.CSV_STGSANDBOX

list @stage_sch.csv_stg;
list @stage_sch.csv_stg/intial;
drop @stage_sch.csv_stg/initial/order_items/order-item-initial-v2.csv;


SELECT
    t.$1::text AS locationid,
    t.$2::text AS city,
    t.$3::text AS state,
    t.$4::text AS zipcode,
    t.$5::text AS activeflag,
    t.$6::text AS createdate,
    t.$7::text AS modifieddate
FROM @stage_sch.csv_stg/intial/customer_address/customer_address_book_initial.csv
    (FILE_FORMAT => 'stage_sch.csv_file_format') t;
