
use role de_case_leocampos19_role;
use database demo_db;
use warehouse case_wh;

create or replace dynamic table de_case_leocampos19_schema.expenses_salary_commissions_stg
    target_lag = downstream
    warehouse = case_wh
    refresh_mode = full
    initialize = on_create
as
with cleaned as (
    select
        to_date(month, 'MON-YY') as month, -- Convert month like 'Jan-24' → date
        trim(regexp_replace(outbound_sales_team, '[0-9,]', '')) as outbound_currency,
        (replace(regexp_replace(outbound_sales_team, '[^0-9,]', ''),',','.'))::decimal(18,2) as outbound_amount,
        trim(regexp_replace(inbound_sales_team, '[0-9,]', '')) as inbound_currency,
        (replace(regexp_replace(inbound_sales_team, '[^0-9,]', ''),',','.'))::decimal(18,2) as inbound_amount
    from gtm_case.expenses_salary_and_commissions
)
select * from cleaned
;

alter dynamic table de_case_leocampos19_schema.expenses_salary_commissions_stg add primary key (month) not enforced rely;

select * from de_case_leocampos19_schema.expenses_salary_commissions_stg;
