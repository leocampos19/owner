
use role de_case_leocampos19_role;
use database demo_db;
use warehouse case_wh;

create or replace dynamic table de_case_leocampos19_schema.f_lead_opportunity
    target_lag = downstream
    warehouse = case_wh
    refresh_mode = full
    initialize = on_create
as
with base_leads as (
    select
        l.lead_id,
        l.converted_opportunity_id,
        l.form_submission_date,
        date_trunc('month', form_submission_date) as inbound_month,
        least(l.first_sales_call_ts, l.first_text_sent_ts) as outbound_activity_ts,
        date_trunc('month', least(l.first_sales_call_ts, l.first_text_sent_ts)) as outbound_month,
        l.sales_call_count,
        l.sales_text_count,
        l.sales_email_count,
        (coalesce(l.sales_call_count,0) + coalesce(l.sales_text_count,0) + coalesce(l.sales_email_count,0)) as activity_score,
        l.predicted_sales_with_owner_amount,
        case 
            when l.form_submission_date is not null then 'Inbound'
            else 'Outbound'
        end as sales_channel,
        l.marketplaces_used_json,
        l.online_ordering_used_json,
        l.cuisine_types_json,
        l.location_count,
        l.connected_with_decision_maker,
        l.status
    from de_case_leocampos19_schema.leads_stg l
)
,opps as (
    select
        o.opportunity_id,
        o.account_id,
        o.stage_name,
        o.lost_reason_c,
        o.closed_lost_notes_c,
        o.business_issue_c,
        o.how_did_you_hear_about_us_c,
        o.created_ts,
        o.close_date,
        o.demo_ts,
        o.demo_set_date,
        o.last_sales_call_ts
    from de_case_leocampos19_schema.opportunities_stg o
)
-- 1) Advertising CAC equally per inbound lead per month
,adv_costs as (
    select distinct
        a.month,
        a.advertising_amount,
        count(*) over (partition by a.month) as total_inbound_leads
    from de_case_leocampos19_schema.expenses_advertising_stg a
        join base_leads b on a.month = b.inbound_month
            and b.sales_channel = 'Inbound'
)
,adv_allocation as (
    select
        b.lead_id,
        a.advertising_amount / nullif(a.total_inbound_leads,0) as advertising_cac
    from adv_costs a
    join base_leads b 
        on a.month = b.inbound_month
        and b.sales_channel = 'Inbound'
)
-- 2) Inbound salary CAC
,inbound_salary_base as (
    select 
        s.month,
        s.inbound_amount,
        b.lead_id,
        b.activity_score,
        sum(b.activity_score) over (partition by s.month) as monthly_activity_score
    from de_case_leocampos19_schema.expenses_salary_commissions_stg s
    join base_leads b 
        on s.month = b.inbound_month
        and b.sales_channel = 'Inbound'
)
,inbound_salary_alloc as (
    select
        lead_id,
        inbound_amount * (activity_score / nullif(monthly_activity_score,0)) as inbound_salary_cac
    from inbound_salary_base
)
-- 3) Outbound salary CAC
,outbound_salary_base as (
    select 
        s.month,
        s.outbound_amount,
        b.lead_id,
        b.activity_score,
        sum(b.activity_score) over (partition by s.month) as total_score
    from de_case_leocampos19_schema.expenses_salary_commissions_stg s
    join base_leads b 
        on s.month = b.outbound_month
        and b.sales_channel = 'Outbound'
)
,outbound_salary_alloc as (
    select
        lead_id,
        outbound_amount * (activity_score / nullif(total_score,0)) as outbound_salary_cac
    from outbound_salary_base
)
-- Final assembly
,final as (
    select
        b.lead_id,
        b.sales_channel,
        b.marketplaces_used_json,
        b.online_ordering_used_json,
        b.cuisine_types_json,
        array_size(array_distinct(ARRAY_COMPACT(SPLIT(REGEXP_REPLACE(b.marketplaces_used_json, '[\\[\\]'']', ''), ',')))) as marketplaces_count,
        case when marketplaces_count = 1 then '1' when marketplaces_count > 1 then '>1' else 'None' end as marketplaces_segment,
        array_size(array_distinct(ARRAY_COMPACT(SPLIT(REGEXP_REPLACE(b.online_ordering_used_json, '[\\[\\]'']', ''), ',')))) as online_ordering_count,
        case when online_ordering_count = 1 then '1' when online_ordering_count > 1 then '>1' else 'None' end as online_ordering_segment,
        array_size(array_distinct(ARRAY_COMPACT(SPLIT(REGEXP_REPLACE(b.cuisine_types_json, '[\\[\\]'']', ''), ',')))) as cuisine_types_count,
        b.location_count,
        case when location_count = 1 then '1'
            when location_count between 1 and 5 then '1-5'
            when location_count between 5 and 20 then '5-20'
            when location_count between 20 and 100 then '20-100'
            else '100+'
        end as location_count_segment,
        b.connected_with_decision_maker,
        b.status,
        b.converted_opportunity_id,
        o.opportunity_id,
        o.stage_name,
        o.lost_reason_c,
        o.closed_lost_notes_c,
        o.business_issue_c,
        o.how_did_you_hear_about_us_c,
        o.account_id,
        o.created_ts,
        o.close_date,
        b.form_submission_date,
        b.outbound_activity_ts,
        coalesce(b.inbound_month, b.outbound_month) as sourced_month,
        b.sales_call_count,
        b.sales_text_count,
        b.sales_email_count,
        b.activity_score,
        b.predicted_sales_with_owner_amount,
        -- LTV
        ((b.predicted_sales_with_owner_amount * 0.05) + 500) * 12 as annual_account_value,
        -- CAC
        coalesce(a.advertising_cac,0) as advertising_cac,
        coalesce(i.inbound_salary_cac,0) as inbound_salary_cac,
        coalesce(o2.outbound_salary_cac,0) as outbound_salary_cac,
        coalesce(a.advertising_cac,0) + coalesce(i.inbound_salary_cac,0) + coalesce(o2.outbound_salary_cac,0) as total_cac
    from base_leads b
        left join opps o on b.converted_opportunity_id = o.opportunity_id
        left join adv_allocation a on b.lead_id = a.lead_id
        left join inbound_salary_alloc i on b.lead_id = i.lead_id
        left join outbound_salary_alloc o2 on b.lead_id = o2.lead_id
)
select * from final;

select * from de_case_leocampos19_schema.f_lead_opportunity;

-- test for duplicates or fan outs
select lead_id, count(*)
from de_case_leocampos19_schema.f_lead_opportunity
group by 1
having count(*) > 1;
