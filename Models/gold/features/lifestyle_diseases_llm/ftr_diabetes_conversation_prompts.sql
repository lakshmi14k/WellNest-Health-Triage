-- models/gold/features/ftr_diabetes_conversation_prompts.sql

{{
    config(
        materialized='table',
        tags=['features', 'lifestyle_diseases', 'tier3']
    )
}}

WITH risk_features AS (
    SELECT * FROM {{ ref('ftr_diabetes_risk_urgency') }}
),

conversation_features AS (
    SELECT
        *,
        
        -- ===== SYMPTOM INQUIRY FLAGS =====
        
        -- Should ask about diabetic symptoms
        CASE 
            WHEN diabetes_stage IN ('prediabetes', 'diabetes_controlled', 'diabetes_uncontrolled', 'diabetes_severe') 
            THEN TRUE 
            ELSE FALSE 
        END AS should_ask_diabetic_symptoms,
        
        -- Should ask about cardiovascular symptoms
        CASE 
            WHEN HAS_HEART_DISEASE OR cardiovascular_risk_score >= 5 THEN TRUE 
            ELSE FALSE 
        END AS should_ask_cardiovascular_symptoms,
        
        -- Should ask about neuropathy symptoms (if long-term diabetes)
        CASE 
            WHEN HAS_DIABETES AND HBA1C_LEVEL >= 8.0 THEN TRUE 
            ELSE FALSE 
        END AS should_ask_neuropathy_symptoms,
        
        -- ===== LIFESTYLE INQUIRY FLAGS =====
        
        -- Should ask about diet
        CASE 
            WHEN HAS_DIABETES OR diabetes_stage = 'prediabetes' OR BMI >= 25 THEN TRUE 
            ELSE FALSE 
        END AS should_ask_diet_habits,
        
        -- Should ask about physical activity
        CASE 
            WHEN BMI >= 25 OR HAS_DIABETES OR HAS_HEART_DISEASE THEN TRUE 
            ELSE FALSE 
        END AS should_ask_physical_activity,
        
        -- Should ask about medication adherence
        CASE 
            WHEN HAS_DIABETES OR HAS_HYPERTENSION OR HAS_HEART_DISEASE THEN TRUE 
            ELSE FALSE 
        END AS should_ask_medication_adherence,
        
        -- ===== SCREENING & MONITORING FLAGS =====
        
        -- Needs glucose monitoring discussion
        CASE 
            WHEN HAS_DIABETES OR diabetes_stage = 'prediabetes' THEN TRUE 
            ELSE FALSE 
        END AS needs_glucose_monitoring_discussion,
        
        -- Needs blood pressure monitoring discussion
        CASE 
            WHEN HAS_HYPERTENSION OR HAS_DIABETES OR HAS_HEART_DISEASE THEN TRUE 
            ELSE FALSE 
        END AS needs_bp_monitoring_discussion,
        
        -- Needs specialist referral consideration
        CASE 
            WHEN (HAS_DIABETES AND HBA1C_LEVEL >= 9.0) OR
                 (has_triple_diagnosis) OR
                 (HAS_HEART_DISEASE AND is_current_smoker) THEN TRUE 
            ELSE FALSE 
        END AS needs_specialist_referral_flag,
        
        -- ===== EDUCATION PRIORITY FLAGS =====
        
        -- Priority education topics (array/concatenated string)
        CONCAT_WS(', ',
            CASE WHEN diabetes_stage IN ('prediabetes', 'diabetes_controlled', 'diabetes_uncontrolled', 'diabetes_severe') 
                 THEN 'diabetes_management' END,
            CASE WHEN BMI >= 30 THEN 'weight_management' END,
            CASE WHEN is_current_smoker THEN 'smoking_cessation' END,
            CASE WHEN HAS_HYPERTENSION THEN 'blood_pressure_control' END,
            CASE WHEN HAS_HEART_DISEASE THEN 'cardiovascular_health' END,
            CASE WHEN HBA1C_LEVEL >= 8.0 THEN 'complication_prevention' END
        ) AS priority_education_topics

    FROM risk_features
)

SELECT * FROM conversation_features