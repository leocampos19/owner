
use role de_case_leocampos19_role;
use database demo_db;
use warehouse case_wh;

create or replace dynamic table de_case_leocampos19_schema.lead_marketplaces_stg
    target_lag = downstream
    warehouse = case_wh
    refresh_mode = full
    initialize = on_create
as
select
    SHA2(lead_id || trim(value::string), 256) as lead_marketplace_id,
    lead_id,
    nullif(trim(regexp_replace(flat.value::string, '[\\[\\]'']', '')),'') as marketplace
from de_case_leocampos19_schema.leads_stg,
     lateral flatten(input => split(marketplaces_used_json, ',')) as flat
;

alter dynamic table de_case_leocampos19_schema.lead_marketplaces_stg add primary key (lead_marketplace_id) not enforced rely;

select * from de_case_leocampos19_schema.lead_marketplaces_stg;
