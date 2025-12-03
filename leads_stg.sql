
use role de_case_leocampos19_role;
use database demo_db;
use warehouse case_wh;

create or replace dynamic table de_case_leocampos19_schema.leads_stg
    target_lag = downstream
    warehouse = case_wh
    refresh_mode = full
    initialize = on_create
as
with cleaned_leads as (
    select
        trim(lead_id) as lead_id,
        trim(converted_opportunity_id) as converted_opportunity_id,
        case 
            when form_submission_date is null then null
            when regexp_substr(form_submission_date, '^[0-9]{4}') like '00%' 
                then regexp_replace(form_submission_date, '^00', '20')
            else form_submission_date
        end::date as form_submission_date, -- Fix malformed dates like "0024-01-22"
        sales_call_count,
        sales_text_count,
        sales_email_count,
        first_sales_call_date       as first_sales_call_ts,
        first_text_sent_date        as first_text_sent_ts,
        first_meeting_booked_date   as first_meeting_booked_ts,
        last_sales_call_date        as last_sales_call_ts,
        last_sales_activity_date    as last_sales_activity_ts,
        last_sales_email_date       as last_sales_email_ts,
        (replace(regexp_replace(predicted_sales_with_owner, '[^0-9,]', ''),',', '.'))::decimal(18, 2) as predicted_sales_with_owner_amount,
        trim(marketplaces_used)     as marketplaces_used_json,
        trim(online_ordering_used)  as online_ordering_used_json,
        trim(cuisine_types)         as cuisine_types_json,
        location_count,
        connected_with_decision_maker,
        status
    from gtm_case.leads
)
select * from cleaned_leads
;

-- add primary key
alter dynamic table de_case_leocampos19_schema.leads_stg add primary key (lead_id) not enforced rely;

select * from de_case_leocampos19_schema.leads_stg;
