-- models/staging/stg_maternalhealth_cleaned.sql
-- Description: Clean and standardize maternal health risk assessment data
-- Source: WELLNEST.PUBLIC.RAW_MATERNALHEALTH_DATA

{{ config(
    materialized='table',
    alias='stg_maternalhealth_cleaned',
    tags=['maternal_health', 'cleaning']
) }}

with source_data as (
    select * 
    from {{ source('wellnest_raw', 'RAW_MATERNALHEALTH_DATA') }}
),

cleaned as (
    select
        -- Patient measurements
        AGE as age,
        SYSTOLICBP as systolic_bp,
        DIASTOLICBP as diastolic_bp,
        BS as blood_sugar,
        BODYTEMP as body_temperature,
        HEARTRATE as heart_rate,
        
        -- Risk assessment
        lower(trim(RISKLEVEL)) as risk_level,
        
        -- Data quality flags
        case 
            when AGE is null or AGE < 15 or AGE > 60 then false
            when SYSTOLICBP is null or SYSTOLICBP < 70 or SYSTOLICBP > 200 then false
            when DIASTOLICBP is null or DIASTOLICBP < 40 or DIASTOLICBP > 130 then false
            when BS is null or BS < 4 or BS > 25 then false
            when BODYTEMP is null or BODYTEMP < 95 or BODYTEMP > 105 then false
            when HEARTRATE is null or HEARTRATE < 50 or HEARTRATE > 120 then false
            when RISKLEVEL is null then false
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
                age, systolic_bp, diastolic_bp,
                blood_sugar, body_temperature, heart_rate,
                risk_level
            order by age
        ) as row_num
    from cleaned
    where is_valid_record = true
),

final as (
    select 
        age,
        systolic_bp,
        diastolic_bp,
        blood_sugar,
        body_temperature,
        heart_rate,
        risk_level,
        dbt_loaded_at
    from deduplicated
    where row_num = 1
)

select * from final