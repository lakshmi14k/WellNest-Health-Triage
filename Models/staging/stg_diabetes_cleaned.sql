-- models/staging/stg_diabetes_cleaned.sql
-- Description: Clean and standardize diabetes patient data
-- Source: WELLNEST.PUBLIC.RAW_DIABETES_DATA

{{ config(
    materialized='table',
    alias='stg_diabetes_cleaned',
    tags=['diabetes', 'cleaning']
) }}

with source_data as (
    select * 
    from {{ source('wellnest_raw', 'RAW_DIABETES_DATA') }}
),

cleaned as (
    select
        -- Standardize gender
        lower(trim(GENDER)) as gender,
        
        -- Numeric fields
        AGE as age,
        BMI as bmi,
        HBA1C_LEVEL as hba1c_level,
        BLOOD_GLUCOSE_LEVEL as blood_glucose_level,
        
        -- Binary indicators (0/1)
        case when HYPERTENSION = 1 then true else false end as has_hypertension,
        case when HEART_DISEASE = 1 then true else false end as has_heart_disease,
        case when DIABETES = 1 then true else false end as has_diabetes,
        
        -- Standardize smoking history
        lower(trim(SMOKING_HISTORY)) as smoking_history,
        
        -- Data quality flags
        case 
            when GENDER is null then false
            when AGE is null or AGE < 10 or AGE > 120 then false
            when BMI is null or BMI < 10 or BMI > 100 then false
            when HBA1C_LEVEL is null or HBA1C_LEVEL < 0 or HBA1C_LEVEL > 20 then false
            when BLOOD_GLUCOSE_LEVEL is null or BLOOD_GLUCOSE_LEVEL < 0 then false
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
                gender,
                age,
                has_hypertension,
                has_heart_disease,
                smoking_history,
                bmi,
                hba1c_level,
                blood_glucose_level,
                has_diabetes
            order by gender
        ) as row_num
    from cleaned
    where is_valid_record = true
),

final as (
    select 
        gender,
        age,
        has_hypertension,
        has_heart_disease,
        smoking_history,
        bmi,
        hba1c_level,
        blood_glucose_level,
        has_diabetes,
        dbt_loaded_at
    from deduplicated
    where row_num = 1
)

select * from final