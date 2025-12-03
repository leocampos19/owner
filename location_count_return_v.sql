
use role de_case_leocampos19_role;
use database demo_db;
use warehouse case_wh;

create or replace view de_case_leocampos19_schema.location_count_return_v as
with base as (
select
    sales_channel,
    stage_name,
    sourced_month,
    location_count_segment,
    how_did_you_hear_about_us_c,
    marketplaces_count,
    online_ordering_segment,
    count(*) as lead_total_count,
    sum(case when connected_with_decision_maker = TRUE then 1 else 0 end) as lead_connected_count,
    sum(case when status = 'Converted' then 1 else 0 end) as lead_converted_count,
    sum(case when stage_name = 'Closed Lost' then 1 else 0 end) as lost_count,
    sum(case when stage_name = 'Closed Won' then 1 else 0 end) as won_count,
    sum(case when stage_name = 'Closed Won' then location_count else 0 end) as location_count,
    sum(case when stage_name = 'Closed Won' then annual_account_value else 0 end) as total_ltv,
    sum(total_cac) as total_cac,
    sum(case when stage_name = 'Closed Won' then annual_account_value else 0 end) - sum(total_cac) as total_return
from de_case_leocampos19_schema.f_lead_opportunity
where sourced_month >= '2024-01-01' and sourced_month < '2024-07-01'
group by all
)
select
    b.sales_channel,
    --b.sourced_month,
    b.location_count_segment,
    --b.how_did_you_hear_about_us_c,
    --b.marketplaces_count,
    --b.online_ordering_segment,
    to_varchar(sum(b.lead_total_count), '999,999,999') as lead_total_count,
    --to_varchar(sum(b.lead_connected_count), '999,999,999') as lead_connected_count,
    to_varchar((sum(b.lead_connected_count) / nullif(sum(b.lead_total_count), 0)) * 100, '90"%"') as lead_connected_rate,
    --to_varchar(sum(b.lead_converted_count), '999,999,999') as lead_converted_count,
    to_varchar((sum(b.lead_converted_count) / nullif(sum(b.lead_connected_count), 0)) * 100, '90"%"') as lead_converted_rate,
    --to_varchar(sum(b.lost_count), '999,999,999') as lost_count,
    to_varchar(sum(b.won_count), '999,999,999') as won_count,
    to_varchar((sum(b.won_count) / nullif(sum(b.lead_converted_count), 0)) * 100, '90"%"') as won_rate,
    to_varchar((sum(b.won_count) / nullif(sum(b.lead_total_count), 0)) * 100, '90"%"') as lead_won_rate,
    --to_varchar(sum(b.location_count), '999,999,999') as location_count,
    --to_varchar(sum(b.total_ltv), '$999,999,999') as total_ltv,
    to_varchar(sum(b.total_cac), '$999,999,999') as total_cac,
    to_varchar(sum(b.total_return), '$999,999,999') as total_return,
    to_varchar((sum(b.total_return) / nullif(sum(b.total_cac), 0)) * 100, '99990"%"') as return_rate
from base b
where sales_channel = 'Inbound'
group by all
order by all;

select * from de_case_leocampos19_schema.location_count_return_v;
