-- models/staging/stg_diabetes_cleaned.sql
-- Description: Clean and standardize diabetes patient data
-- Source: WELLNEST.PUBLIC.RAW_DIABETES_DATA

{{ config(
    materialized='table',
    alias='stg_bloodpressure_cleaned',
    tags=['bloddpressure', 'cleaning']
) }}

with source_data as (
    select * 
    from {{ source('wellnest_raw', 'RAW_BLOODPRESSURE_DATA') }}
),

cleaned as (
    select
        -- Demographics
        trim(COUNTRY) as country,
        lower(trim(GENDER)) as gender,
        AGE as age,
        lower(trim(EDUCATION_LEVEL)) as education_level,
        lower(trim(EMPLOYMENT_STATUS)) as employment_status,
        
        -- Physical measurements
        BMI as bmi,
        CHOLESTEROL as cholesterol,
        SYSTOLIC_BP as systolic_bp,
        DIASTOLIC_BP as diastolic_bp,
        HEART_RATE as heart_rate,
        
        -- Lipid panel
        LDL as ldl,
        HDL as hdl,
        TRIGLYCERIDES as triglycerides,
        GLUCOSE as glucose,
        
        -- Lifestyle factors
        lower(trim(SMOKING_STATUS)) as smoking_status,
        lower(trim(PHYSICAL_ACTIVITY_LEVEL)) as physical_activity_level,
        ALCOHOL_INTAKE as alcohol_intake,
        SALT_INTAKE as salt_intake,
        SLEEP_DURATION as sleep_duration,
        STRESS_LEVEL as stress_level,
        
        -- Medical history (booleans)
        case when FAMILY_HISTORY = true then true else false end as has_family_history,
        case when DIABETES = true then true else false end as has_diabetes,
        
        -- Hypertension diagnosis
        trim(HYPERTENSION) as hypertension_status,
        
        -- Data quality flags
        case 
            when COUNTRY is null then false
            when AGE is null or AGE < 18 or AGE > 120 then false
            when BMI is null or BMI < 10 or BMI > 100 then false
            when SYSTOLIC_BP is null or SYSTOLIC_BP < 60 or SYSTOLIC_BP > 250 then false
            when DIASTOLIC_BP is null or DIASTOLIC_BP < 40 or DIASTOLIC_BP > 150 then false
            when CHOLESTEROL is null or CHOLESTEROL < 100 or CHOLESTEROL > 400 then false
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
                country, gender, age, bmi, cholesterol,
                systolic_bp, diastolic_bp, smoking_status,
                physical_activity_level, hypertension_status
            order by country
        ) as row_num
    from cleaned
    where is_valid_record = true
),

final as (
    select 
        country,
        gender,
        age,
        education_level,
        employment_status,
        bmi,
        cholesterol,
        systolic_bp,
        diastolic_bp,
        heart_rate,
        ldl,
        hdl,
        triglycerides,
        glucose,
        smoking_status,
        physical_activity_level,
        alcohol_intake,
        salt_intake,
        sleep_duration,
        stress_level,
        has_family_history,
        has_diabetes,
        hypertension_status,
        dbt_loaded_at
    from deduplicated
    where row_num = 1
)

select * from final