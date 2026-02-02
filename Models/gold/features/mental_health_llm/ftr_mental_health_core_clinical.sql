-- models/gold/features/mental_health_llm/ftr_mental_health_core_clinical.sql

{{
    config(
        materialized='table',
        tags=['features', 'mental_health', 'tier1']
    )
}}

WITH base_data AS (
    SELECT * FROM {{ ref('stg_mental_health_cleaned') }}
),

core_features AS (
    SELECT
        *,
        
        -- ===== AGE CALCULATION & CATEGORIZATION =====
        
        -- Calculate age from survey timestamp (assuming survey was in 2014 based on data)
        YEAR(CURRENT_DATE()) - YEAR(SURVEY_TIMESTAMP) AS calculated_age,
        
        -- Age category for mental health risk
        CASE 
            WHEN YEAR(CURRENT_DATE()) - YEAR(SURVEY_TIMESTAMP) < 18 THEN 'adolescent'
            WHEN YEAR(CURRENT_DATE()) - YEAR(SURVEY_TIMESTAMP) BETWEEN 18 AND 25 THEN 'young_adult'
            WHEN YEAR(CURRENT_DATE()) - YEAR(SURVEY_TIMESTAMP) BETWEEN 26 AND 40 THEN 'adult'
            WHEN YEAR(CURRENT_DATE()) - YEAR(SURVEY_TIMESTAMP) BETWEEN 41 AND 60 THEN 'middle_age'
            WHEN YEAR(CURRENT_DATE()) - YEAR(SURVEY_TIMESTAMP) > 60 THEN 'senior'
            ELSE 'unknown'
        END AS age_category,
        
        -- ===== MENTAL HEALTH HISTORY FEATURES =====
        
        -- Mental health history standardization
        CASE 
            WHEN LOWER(MENTAL_HEALTH_HISTORY) = 'yes' THEN 'documented_history'
            WHEN LOWER(MENTAL_HEALTH_HISTORY) = 'no' THEN 'no_history'
            WHEN LOWER(MENTAL_HEALTH_HISTORY) = 'maybe' THEN 'possible_history'
            ELSE 'unknown'
        END AS mental_health_history_clean,
        
        -- Has documented mental health history
        CASE 
            WHEN LOWER(MENTAL_HEALTH_HISTORY) = 'yes' THEN TRUE 
            ELSE FALSE 
        END AS has_mental_health_history,
        
        -- Family history flag
        CASE 
            WHEN HAS_FAMILY_HISTORY THEN TRUE 
            ELSE FALSE 
        END AS has_family_mental_health_history,
        
        -- Combined history risk (personal + family)
        CASE 
            WHEN LOWER(MENTAL_HEALTH_HISTORY) = 'yes' AND HAS_FAMILY_HISTORY THEN 'high_genetic_risk'
            WHEN LOWER(MENTAL_HEALTH_HISTORY) = 'yes' OR HAS_FAMILY_HISTORY THEN 'moderate_risk'
            WHEN LOWER(MENTAL_HEALTH_HISTORY) = 'maybe' THEN 'possible_risk'
            ELSE 'low_risk'
        END AS genetic_history_risk,
        
        -- ===== SYMPTOM SEVERITY FEATURES =====
        
        -- Mood swings severity
        CASE 
            WHEN LOWER(MOOD_SWINGS) = 'high' THEN 'severe'
            WHEN LOWER(MOOD_SWINGS) = 'medium' THEN 'moderate'
            WHEN LOWER(MOOD_SWINGS) = 'low' THEN 'mild'
            ELSE 'none_reported'
        END AS mood_swing_severity,
        
        -- Growing stress severity
        CASE 
            WHEN LOWER(GROWING_STRESS) = 'yes' THEN 'significant_stress'
            WHEN LOWER(GROWING_STRESS) = 'maybe' THEN 'possible_stress'
            WHEN LOWER(GROWING_STRESS) = 'no' THEN 'no_stress'
            ELSE 'unknown'
        END AS stress_level,
        
        -- Coping struggles flag
        CASE 
            WHEN HAS_COPING_STRUGGLES THEN TRUE 
            ELSE FALSE 
        END AS has_coping_difficulties,
        
        -- ===== BEHAVIORAL & SOCIAL FEATURES =====
        
        -- Social functioning
        CASE 
            WHEN LOWER(SOCIAL_WEAKNESS) = 'yes' THEN 'social_impairment'
            WHEN LOWER(SOCIAL_WEAKNESS) = 'maybe' THEN 'social_difficulty'
            WHEN LOWER(SOCIAL_WEAKNESS) = 'no' THEN 'intact_social_function'
            ELSE 'unknown'
        END AS social_functioning_status,
        
        -- Work interest/motivation
        CASE 
            WHEN LOWER(WORK_INTEREST) = 'no' THEN 'anhedonia_work'
            WHEN LOWER(WORK_INTEREST) = 'maybe' THEN 'reduced_motivation'
            WHEN LOWER(WORK_INTEREST) = 'yes' THEN 'maintained_interest'
            ELSE 'unknown'
        END AS work_motivation_status,
        
        -- Behavioral changes
        CASE 
            WHEN LOWER(CHANGES_HABITS) = 'yes' THEN 'significant_changes'
            WHEN LOWER(CHANGES_HABITS) = 'maybe' THEN 'some_changes'
            WHEN LOWER(CHANGES_HABITS) = 'no' THEN 'stable_habits'
            ELSE 'unknown'
        END AS behavioral_change_status,
        
        -- Days indoors (isolation risk)
        CASE 
            WHEN LOWER(DAYS_INDOORS) IN ('go out every day', 'go out everyday') THEN 'no_isolation'
            WHEN LOWER(DAYS_INDOORS) = '1-14 days' THEN 'mild_isolation'
            WHEN LOWER(DAYS_INDOORS) = '15-30 days' THEN 'moderate_isolation'
            WHEN LOWER(DAYS_INDOORS) IN ('31-60 days', 'more than 2 months') THEN 'severe_isolation'
            ELSE 'unknown'
        END AS social_isolation_level,
        
        -- Isolation severity score
        CASE 
            WHEN LOWER(DAYS_INDOORS) IN ('go out every day', 'go out everyday') THEN 0
            WHEN LOWER(DAYS_INDOORS) = '1-14 days' THEN 1
            WHEN LOWER(DAYS_INDOORS) = '15-30 days' THEN 2
            WHEN LOWER(DAYS_INDOORS) IN ('31-60 days') THEN 3
            WHEN LOWER(DAYS_INDOORS) = 'more than 2 months' THEN 4
            ELSE NULL
        END AS isolation_severity_score,
        
        -- ===== TREATMENT & CARE FEATURES =====
        
        -- Currently receiving treatment
        CASE 
            WHEN RECEIVING_TREATMENT THEN TRUE 
            ELSE FALSE 
        END AS in_active_treatment,
        
        -- Care options awareness
        CASE 
            WHEN LOWER(CARE_OPTIONS) = 'yes' THEN 'aware_of_options'
            WHEN LOWER(CARE_OPTIONS) = 'not sure' THEN 'uncertain_about_care'
            WHEN LOWER(CARE_OPTIONS) = 'no' THEN 'unaware_of_options'
            ELSE 'unknown'
        END AS care_awareness_status,
        
        -- Treatment gap indicator
        CASE 
            WHEN NOT RECEIVING_TREATMENT AND LOWER(MENTAL_HEALTH_HISTORY) = 'yes' THEN TRUE
            ELSE FALSE
        END AS has_treatment_gap,
        
        -- ===== OCCUPATIONAL & EMPLOYMENT FEATURES =====
        
        -- Occupation category
        CASE 
            WHEN LOWER(OCCUPATION) = 'student' THEN 'student'
            WHEN LOWER(OCCUPATION) IN ('corporate', 'business') THEN 'professional'
            WHEN LOWER(OCCUPATION) = 'housewife' THEN 'homemaker'
            WHEN LOWER(OCCUPATION) = 'others' THEN 'other'
            ELSE 'unknown'
        END AS occupation_category,
        
        -- Employment status flag
        CASE 
            WHEN IS_SELF_EMPLOYED THEN 'self_employed'
            WHEN LOWER(OCCUPATION) = 'student' THEN 'student'
            WHEN LOWER(OCCUPATION) = 'housewife' THEN 'homemaker'
            ELSE 'employed'
        END AS employment_status,
        
        -- ===== WORKPLACE MENTAL HEALTH COMFORT =====
        
        -- Comfort discussing mental health at interview
        CASE 
            WHEN LOWER(MENTAL_HEALTH_INTERVIEW) = 'yes' THEN 'comfortable_discussing'
            WHEN LOWER(MENTAL_HEALTH_INTERVIEW) = 'maybe' THEN 'somewhat_comfortable'
            WHEN LOWER(MENTAL_HEALTH_INTERVIEW) = 'no' THEN 'uncomfortable_discussing'
            ELSE 'unknown'
        END AS mental_health_disclosure_comfort,
        
        -- Stigma indicator
        CASE 
            WHEN LOWER(MENTAL_HEALTH_INTERVIEW) = 'no' THEN TRUE
            ELSE FALSE
        END AS perceived_workplace_stigma,
        
        -- ===== SYMPTOM COUNT FEATURES =====
        
        -- Count of positive symptom indicators
        (
            CASE WHEN LOWER(MOOD_SWINGS) IN ('medium', 'high') THEN 1 ELSE 0 END +
            CASE WHEN LOWER(GROWING_STRESS) = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN HAS_COPING_STRUGGLES THEN 1 ELSE 0 END +
            CASE WHEN LOWER(SOCIAL_WEAKNESS) = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN LOWER(WORK_INTEREST) = 'no' THEN 1 ELSE 0 END +
            CASE WHEN LOWER(CHANGES_HABITS) = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN LOWER(DAYS_INDOORS) NOT IN ('go out every day', 'go out everyday') THEN 1 ELSE 0 END
        ) AS symptom_count,
        
        -- Severe symptom count
        (
            CASE WHEN LOWER(MOOD_SWINGS) = 'high' THEN 1 ELSE 0 END +
            CASE WHEN HAS_COPING_STRUGGLES THEN 1 ELSE 0 END +
            CASE WHEN LOWER(SOCIAL_WEAKNESS) = 'yes' THEN 1 ELSE 0 END +
            CASE WHEN LOWER(WORK_INTEREST) = 'no' THEN 1 ELSE 0 END +
            CASE WHEN LOWER(DAYS_INDOORS) IN ('31-60 days', 'more than 2 months') THEN 1 ELSE 0 END
        ) AS severe_symptom_count,
        
        -- ===== GENDER STANDARDIZATION =====
        
        CASE 
            WHEN LOWER(GENDER) IN ('male', 'm') THEN 'male'
            WHEN LOWER(GENDER) IN ('female', 'f') THEN 'female'
            ELSE 'other'
        END AS gender_clean

    FROM base_data
)

SELECT * FROM core_features