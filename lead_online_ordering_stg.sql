
use role de_case_leocampos19_role;
use database demo_db;
use warehouse case_wh;

create or replace dynamic table de_case_leocampos19_schema.lead_online_ordering_stg
    target_lag = downstream
    warehouse = case_wh
    refresh_mode = full
    initialize = on_create
as
select
    SHA2(lead_id || trim(value::string), 256) as lead_online_ordering_id,
    lead_id,
    nullif(trim(regexp_replace(flat.value::string, '[\\[\\]'']', '')),'') as online_ordering
from de_case_leocampos19_schema.leads_stg,
     lateral flatten(input => split(online_ordering_used_json, ',')) as flat
;

alter dynamic table de_case_leocampos19_schema.lead_online_ordering_stg add primary key (lead_online_ordering_id) not enforced rely;

select * from de_case_leocampos19_schema.lead_online_ordering_stg;
