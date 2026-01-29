-- models/staging/stg_mental_health_cleaned.sql
-- Description: Clean and standardize mental health survey data
-- Source: WELLNEST.PUBLIC.RAW_MENTALHEALTH
-- Target: WELLNEST.PUBLIC.stg_mental_health_cleaned

{{ config(
    materialized='table',
    alias='stg_mental_health_cleaned',
    tags=['mental_health']
) }}

with source_data as (
    select * 
    from {{ source('wellnest_raw', 'RAW_MENTALHEALTH') }}
),

cleaned as (
    select
        -- Convert timestamp to proper format
        try_to_timestamp(TIMESTAMP, 'MM/DD/YYYY HH24:MI') as survey_timestamp,
        
        -- Standardize gender
        lower(trim(GENDER)) as gender,
        
        -- Standardize country
        trim(COUNTRY) as country,
        
        -- Standardize occupation
        trim(OCCUPATION) as occupation,
        
        -- Boolean fields
        SELF_EMPLOYED as is_self_employed,
        FAMILY_HISTORY as has_family_history,
        TREATMENT as receiving_treatment,
        COPING_STRUGGLES as has_coping_struggles,
        
        -- Clean text responses
        trim(DAYS_INDOORS) as days_indoors,
        trim(GROWING_STRESS) as growing_stress,
        trim(CHANGES_HABITS) as changes_habits,
        trim(MENTAL_HEALTH_HISTORY) as mental_health_history,
        trim(MOOD_SWINGS) as mood_swings,
        trim(WORK_INTEREST) as work_interest,
        trim(SOCIAL_WEAKNESS) as social_weakness,
        trim(MENTAL_HEALTH_INTERVIEW) as mental_health_interview,
        trim(CARE_OPTIONS) as care_options,
        
        -- Data quality flag
        case 
            when TIMESTAMP is null then false
            when GENDER is null then false
            when COUNTRY is null then false
            when SELF_EMPLOYED is null then false
            when FAMILY_HISTORY is null then false
            when TREATMENT is null then false
            when COPING_STRUGGLES is null then false
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
                survey_timestamp,
                gender,
                country,
                occupation,
                is_self_employed,
                has_family_history,
                receiving_treatment,
                days_indoors,
                growing_stress,
                changes_habits,
                mental_health_history,
                mood_swings,
                has_coping_struggles,
                work_interest,
                social_weakness,
                mental_health_interview,
                care_options
            order by survey_timestamp
        ) as row_num
    from cleaned
    where is_valid_record = true
),

final as (
    select 
        survey_timestamp,
        gender,
        country,
        occupation,
        is_self_employed,
        has_family_history,
        receiving_treatment,
        has_coping_struggles,
        days_indoors,
        growing_stress,
        changes_habits,
        mental_health_history,
        mood_swings,
        work_interest,
        social_weakness,
        mental_health_interview,
        care_options,
        dbt_loaded_at
    from deduplicated
    where row_num = 1
)

select * from final