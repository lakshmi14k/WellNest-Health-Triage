-- models/staging/stg_pcos_infertility_cleaned.sql
-- Description: Clean and standardize PCOS infertility patient data
-- Source: WELLNEST.PUBLIC.RAW_PCOS_INFERTILITY_DATA

{{ config(
    materialized='table',
    alias='stg_pcos_infertility_cleaned',
    tags=['pcos', 'infertility', 'womens_health', 'cleaning']
) }}

with source_data as (
    select * 
    from {{ source('wellnest_raw', 'RAW_PCOS_INFERTILITY_DATA') }}
),

cleaned as (
    select
        -- Patient identifier
        PATIENT_FILE_NO as patient_id,
        
        -- PCOS diagnosis
        case when PCOS = 1 then true else false end as has_pcos,
        
        -- Beta-HCG levels (pregnancy hormone measured at two time points)
        I_BETA_HCG as beta_hcg_first_measurement,
        II_BETA_HCG as beta_hcg_second_measurement,
        
        -- Anti-Müllerian Hormone level
        AMH as amh_level,
        
        -- Data quality flags
        case 
            when PATIENT_FILE_NO is null then false
            when I_BETA_HCG is null or I_BETA_HCG < 0 or I_BETA_HCG > 100000 then false
            when II_BETA_HCG is null or II_BETA_HCG < 0 or II_BETA_HCG > 100000 then false
            when AMH is null or AMH < 0 or AMH > 50 then false
            when PCOS is null then false
            else true
        end as is_valid_record,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at
        
    from source_data
),

deduplicated as (
    select 
        *,
        row_number() over (
            partition by 
                patient_id, has_pcos, 
                beta_hcg_first_measurement,
                beta_hcg_second_measurement,
                amh_level
            order by patient_id
        ) as row_num
    from cleaned
    where is_valid_record = true
),

final as (
    select 
        patient_id,
        has_pcos,
        beta_hcg_first_measurement,
        beta_hcg_second_measurement,
        amh_level,
        dbt_loaded_at
    from deduplicated
    where row_num = 1
)

select * from final