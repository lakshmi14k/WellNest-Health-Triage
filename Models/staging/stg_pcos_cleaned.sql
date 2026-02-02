-- models/staging/stg_pcos_cleaned.sql
-- Description: Clean and standardize PCOS (Polycystic Ovary Syndrome) data
-- Source: WELLNEST.PUBLIC.RAW_PCOS_DATA

{{ config(
    materialized='table',
    alias='stg_pcos_cleaned',
    tags=['pcos', 'womens_health', 'hormonal', 'cleaning']
) }}

with source_data as (
    select * 
    from {{ source('wellnest_raw', 'RAW_PCOS_DATA') }}
),

cleaned as (
    select
        -- Demographics
        AGE as age,
        
        -- Hormonal levels
        ANTI_MULLERIAN_HORMONE as amh_level,
        LUTEINIZING_HORMONE as lh_level,
        FOLLICLE_STIMULATING_HORMONE as fsh_level,
        TOTAL_TESTOSTERONE as testosterone_level,
        
        -- Physical measurements
        BODY_MASS_INDEX as bmi,
        
        -- Medical history
        case when FAMILY_HISTORY = true then true else false end as has_family_history,
        
        -- Menstrual cycle pattern
        lower(trim(MENSTRUAL_CYCLE)) as menstrual_cycle_pattern,
        
        -- Data quality flags
        case 
            when AGE is null or AGE < 12 or AGE > 60 then false
            when ANTI_MULLERIAN_HORMONE is null or ANTI_MULLERIAN_HORMONE < 0 or ANTI_MULLERIAN_HORMONE > 50 then false
            when LUTEINIZING_HORMONE is null or LUTEINIZING_HORMONE < 0 or LUTEINIZING_HORMONE > 100 then false
            when FOLLICLE_STIMULATING_HORMONE is null or FOLLICLE_STIMULATING_HORMONE < 0 or FOLLICLE_STIMULATING_HORMONE > 50 then false
            when TOTAL_TESTOSTERONE is null or TOTAL_TESTOSTERONE < 0 or TOTAL_TESTOSTERONE > 1000 then false
            when BODY_MASS_INDEX is null or BODY_MASS_INDEX < 10 or BODY_MASS_INDEX > 100 then false
            when MENSTRUAL_CYCLE is null then false
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
                age, amh_level, lh_level, fsh_level,
                testosterone_level, bmi, menstrual_cycle_pattern
            order by age
        ) as row_num
    from cleaned
    where is_valid_record = true
),

final as (
    select 
        age,
        amh_level,
        lh_level,
        fsh_level,
        testosterone_level,
        bmi,
        has_family_history,
        menstrual_cycle_pattern,
        dbt_loaded_at
    from deduplicated
    where row_num = 1
)

select * from final