from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sql("create database openlibrary")
tbl = """
create or replace table openlibrary.subjects_bronze 
(
    authors array<struct<key:string, name:string>>,
    availability struct<__src__:string,available_to_borrow: boolean,available_to_browse: boolean, available_to_waitlist: boolean,error_message: string,identifier: string,is_browseable: boolean, is_lendable: boolean,is_previewable: boolean, is_printdisabled: boolean, is_readable: boolean, is_restricted: boolean, isbn: string, last_loan_date: string, last_waitlist_date: string, num_waitlist: string, oclc: string, openlibrary_edition: string, openlibrary_work: string, status: string>,
    cover_edition_key string,
    cover_id bigint,
    edition_count bigint,
    first_publish_year bigint, 
    has_fulltext boolean, 
    ia string,
    ia_collection array<string>, 
    key string, 
    lending_edition string, 
    lending_identifier string,
    lendinglibrary boolean, 
    printdisabled boolean, 
    public_scan boolean,
    subject array<string>,
    title string,
    type string
)
using iceberg
PARTITIONED BY (type)
"""
spark.sql(tbl)

tbl_silver = """
create or replace table openlibrary.subjects_silver
(
    authors array<struct<key:string, name:string>>,
    availability struct<__src__:string,available_to_borrow: boolean,available_to_browse: boolean, available_to_waitlist: boolean,error_message: string,identifier: string,is_browseable: boolean, is_lendable: boolean,is_previewable: boolean, is_printdisabled: boolean, is_readable: boolean, is_restricted: boolean, isbn: string, last_loan_date: string, last_waitlist_date: string, num_waitlist: string, oclc: string, openlibrary_edition: string, openlibrary_work: string, status: string>,
    cover_edition_key string,
    cover_id bigint,
    edition_count bigint,
    first_publish_year bigint, 
    has_fulltext boolean, 
    ia string,
    ia_collection array<string>, 
    key string, 
    lending_edition string, 
    lending_identifier string,
    lendinglibrary boolean, 
    printdisabled boolean, 
    public_scan boolean,
    subject array<string>,
    title string,
    type string
)
using iceberg
PARTITIONED BY (type)
"""

spark.sql(tbl_silver)