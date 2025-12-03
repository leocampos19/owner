
use role de_case_leocampos19_role;
use database demo_db;
use warehouse case_wh;

create or replace dynamic table de_case_leocampos19_schema.opportunities_stg
    target_lag = downstream
    warehouse = case_wh
    refresh_mode = full
    initialize = on_create
as
with cleaned_opps as (
    select distinct
        trim(opportunity_id) as opportunity_id,
        stage_name,
        lost_reason_c,
        closed_lost_notes_c,
        business_issue_c,
        how_did_you_hear_about_us_c,
        account_id,
        created_date as created_ts,
        demo_time as demo_ts,
        last_sales_call_date_time as last_sales_call_ts,
        case when demo_set_date is null then null
            when regexp_substr(demo_set_date, '^[0-9]{4}') like '00%' then regexp_replace(demo_set_date, '^00', '20')
            else demo_set_date
        end::date as demo_set_date, -- Fix malformed dates like "0024-01-22"
        case when close_date is null then null
            when regexp_substr(close_date, '^[0-9]{4}') like '00%' then regexp_replace(close_date, '^00', '20')
            else close_date
        end::date as close_date, -- Fix malformed dates like "0024-01-22"
    from gtm_case.opportunities
)
select * from cleaned_opps;

-- add primary key
alter dynamic table de_case_leocampos19_schema.opportunities_stg add primary key (opportunity_id) not enforced rely;

select * from de_case_leocampos19_schema.opportunities_stg;
