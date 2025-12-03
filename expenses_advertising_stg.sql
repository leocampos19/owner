
use role de_case_leocampos19_role;
use database demo_db;
use warehouse case_wh;

create or replace dynamic table de_case_leocampos19_schema.expenses_advertising_stg
    target_lag = downstream
    warehouse = case_wh
    refresh_mode = full
    initialize = on_create
as
with cleaned as (
    select
        to_date(month, 'MON-YY') as month, -- Convert month like 'Jan-24' → date
        trim(regexp_replace(advertising, '[0-9,]', '')) as currency, 
        (replace(regexp_replace(advertising, '[^0-9,]', ''),',', '.'))::decimal(18, 2) AS advertising_amount -- Clean currency (US$ 55 779,40 → 55779.40)
    from gtm_case.expenses_advertising
)
select * from cleaned
;

alter dynamic table de_case_leocampos19_schema.expenses_advertising_stg add primary key (month) not enforced rely;

select * from de_case_leocampos19_schema.expenses_advertising_stg;
