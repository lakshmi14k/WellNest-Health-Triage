-- models/gold/features/womens_wellness_llm/ftr_maternal_health_core_clinical.sql

{{
    config(
        materialized='table',
        tags=['features', 'womens_wellness', 'maternal_health', 'tier1']
    )
}}

WITH base_data AS (
    SELECT * FROM {{ ref('stg_maternal_health_cleaned') }}
),

core_features AS (
    SELECT
        *,
        
        -- ===== AGE & PREGNANCY RISK FEATURES =====
        
        -- Maternal age category (pregnancy risk perspective)
        CASE 
            WHEN AGE < 18 THEN 'adolescent_high_risk'
            WHEN AGE BETWEEN 18 AND 19 THEN 'teenage_pregnancy'
            WHEN AGE BETWEEN 20 AND 34 THEN 'optimal_reproductive_age'
            WHEN AGE BETWEEN 35 AND 39 THEN 'advanced_maternal_age'
            WHEN AGE >= 40 THEN 'very_advanced_maternal_age'
            ELSE 'unknown'
        END AS maternal_age_category,
        
        -- Age risk flag
        CASE 
            WHEN AGE < 18 OR AGE >= 35 THEN TRUE 
            ELSE FALSE 
        END AS has_age_related_risk,
        
        -- ===== BLOOD PRESSURE FEATURES (Pregnancy-Specific) =====
        
        -- Blood pressure stage for pregnancy (ACOG guidelines)
        CASE 
            WHEN SYSTOLIC_BP < 120 AND DIASTOLIC_BP < 80 THEN 'normal'
            WHEN (SYSTOLIC_BP BETWEEN 120 AND 139) OR (DIASTOLIC_BP BETWEEN 80 AND 89) THEN 'elevated_monitor'
            WHEN (SYSTOLIC_BP BETWEEN 140 AND 159) OR (DIASTOLIC_BP BETWEEN 90 AND 109) THEN 'gestational_htn_stage1'
            WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN 'severe_htn_preeclampsia_risk'
            ELSE 'unknown'
        END AS pregnancy_bp_stage,
        
        -- Systolic BP category for pregnancy
        CASE 
            WHEN SYSTOLIC_BP < 90 THEN 'hypotension'
            WHEN SYSTOLIC_BP BETWEEN 90 AND 119 THEN 'normal'
            WHEN SYSTOLIC_BP BETWEEN 120 AND 139 THEN 'elevated'
            WHEN SYSTOLIC_BP BETWEEN 140 AND 159 THEN 'stage1_htn'
            WHEN SYSTOLIC_BP >= 160 THEN 'severe_htn'
            ELSE 'unknown'
        END AS systolic_category_pregnancy,
        
        -- Diastolic BP category for pregnancy
        CASE 
            WHEN DIASTOLIC_BP < 60 THEN 'hypotension'
            WHEN DIASTOLIC_BP BETWEEN 60 AND 79 THEN 'normal'
            WHEN DIASTOLIC_BP BETWEEN 80 AND 89 THEN 'elevated'
            WHEN DIASTOLIC_BP BETWEEN 90 AND 109 THEN 'stage1_htn'
            WHEN DIASTOLIC_BP >= 110 THEN 'severe_htn'
            ELSE 'unknown'
        END AS diastolic_category_pregnancy,
        
        -- Preeclampsia warning (BP >= 140/90)
        CASE 
            WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN TRUE 
            ELSE FALSE 
        END AS preeclampsia_bp_warning,
        
        -- Severe preeclampsia warning (BP >= 160/110)
        CASE 
            WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN TRUE 
            ELSE FALSE 
        END AS severe_preeclampsia_bp_warning,
        
        -- Pulse pressure
        (SYSTOLIC_BP - DIASTOLIC_BP) AS pulse_pressure,
        
        -- Mean arterial pressure (MAP)
        ROUND((DIASTOLIC_BP + (SYSTOLIC_BP - DIASTOLIC_BP) / 3.0), 1) AS mean_arterial_pressure,
        
        -- MAP risk category (high MAP associated with preeclampsia)
        CASE 
            WHEN ((DIASTOLIC_BP + (SYSTOLIC_BP - DIASTOLIC_BP) / 3.0)) < 90 THEN 'low'
            WHEN ((DIASTOLIC_BP + (SYSTOLIC_BP - DIASTOLIC_BP) / 3.0)) BETWEEN 90 AND 105 THEN 'normal'
            WHEN ((DIASTOLIC_BP + (SYSTOLIC_BP - DIASTOLIC_BP) / 3.0)) > 105 THEN 'elevated_preeclampsia_risk'
            ELSE 'unknown'
        END AS map_risk_category,
        
        -- ===== HEART RATE FEATURES (Pregnancy-Specific) =====
        
        -- Heart rate category (pregnancy increases resting HR by 10-20 bpm)
        CASE 
            WHEN HEART_RATE < 60 THEN 'bradycardia'
            WHEN HEART_RATE BETWEEN 60 AND 100 THEN 'normal_pregnancy'
            WHEN HEART_RATE BETWEEN 101 AND 110 THEN 'mildly_elevated'
            WHEN HEART_RATE BETWEEN 111 AND 120 THEN 'tachycardia_concerning'
            WHEN HEART_RATE > 120 THEN 'severe_tachycardia'
            ELSE 'unknown'
        END AS heart_rate_category_pregnancy,
        
        -- Tachycardia flag (>100 bpm, concerning in pregnancy)
        CASE 
            WHEN HEART_RATE > 100 THEN TRUE 
            ELSE FALSE 
        END AS has_tachycardia,
        
        -- ===== BLOOD SUGAR FEATURES (Gestational Diabetes Screening) =====
        
        -- Blood sugar category (assuming random/casual glucose)
        CASE 
            WHEN BLOOD_SUGAR < 140 THEN 'normal'
            WHEN BLOOD_SUGAR BETWEEN 140 AND 199 THEN 'impaired_glucose_tolerance'
            WHEN BLOOD_SUGAR >= 200 THEN 'gestational_diabetes_suspected'
            ELSE 'unknown'
        END AS blood_sugar_category,
        
        -- Gestational diabetes risk flag
        CASE 
            WHEN BLOOD_SUGAR >= 140 THEN TRUE 
            ELSE FALSE 
        END AS gestational_diabetes_risk,
        
        -- Hyperglycemia severity
        CASE 
            WHEN BLOOD_SUGAR < 140 THEN 'normal'
            WHEN BLOOD_SUGAR BETWEEN 140 AND 179 THEN 'mild_hyperglycemia'
            WHEN BLOOD_SUGAR BETWEEN 180 AND 249 THEN 'moderate_hyperglycemia'
            WHEN BLOOD_SUGAR >= 250 THEN 'severe_hyperglycemia'
            ELSE 'unknown'
        END AS hyperglycemia_severity,
        
        -- ===== BODY TEMPERATURE FEATURES =====
        
        -- Temperature category (Fahrenheit)
        CASE 
            WHEN BODY_TEMPERATURE < 97.0 THEN 'hypothermia'
            WHEN BODY_TEMPERATURE BETWEEN 97.0 AND 99.5 THEN 'normal'
            WHEN BODY_TEMPERATURE BETWEEN 99.6 AND 100.3 THEN 'low_grade_fever'
            WHEN BODY_TEMPERATURE BETWEEN 100.4 AND 102.9 THEN 'fever'
            WHEN BODY_TEMPERATURE >= 103.0 THEN 'high_fever'
            ELSE 'unknown'
        END AS temperature_category,
        
        -- Fever flag (concerning in pregnancy - infection risk)
        CASE 
            WHEN BODY_TEMPERATURE >= 100.4 THEN TRUE 
            ELSE FALSE 
        END AS has_fever,
        
        -- ===== RISK LEVEL STANDARDIZATION =====
        
        -- Risk level clean
        CASE 
            WHEN LOWER(RISK_LEVEL) = 'low risk' THEN 'low_risk'
            WHEN LOWER(RISK_LEVEL) = 'mid risk' THEN 'moderate_risk'
            WHEN LOWER(RISK_LEVEL) = 'high risk' THEN 'high_risk'
            ELSE 'unknown'
        END AS risk_level_clean,
        
        -- ===== VITAL SIGNS STABILITY FLAGS =====
        
        -- Critical vitals flag (any vital sign in danger zone)
        CASE 
            WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN TRUE
            WHEN HEART_RATE > 120 OR HEART_RATE < 50 THEN TRUE
            WHEN BODY_TEMPERATURE >= 103.0 THEN TRUE
            WHEN BLOOD_SUGAR >= 250 THEN TRUE
            ELSE FALSE
        END AS has_critical_vitals,
        
        -- Abnormal vitals count
        (
            CASE WHEN SYSTOLIC_BP >= 140 OR SYSTOLIC_BP < 90 THEN 1 ELSE 0 END +
            CASE WHEN DIASTOLIC_BP >= 90 OR DIASTOLIC_BP < 60 THEN 1 ELSE 0 END +
            CASE WHEN HEART_RATE > 100 OR HEART_RATE < 60 THEN 1 ELSE 0 END +
            CASE WHEN BODY_TEMPERATURE >= 100.4 OR BODY_TEMPERATURE < 97.0 THEN 1 ELSE 0 END +
            CASE WHEN BLOOD_SUGAR >= 140 THEN 1 ELSE 0 END
        ) AS abnormal_vitals_count,
        
        -- ===== PREECLAMPSIA INDICATORS =====
        
        -- Preeclampsia feature count (based on available data)
        (
            CASE WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN 2 ELSE 0 END +
            CASE WHEN ((DIASTOLIC_BP + (SYSTOLIC_BP - DIASTOLIC_BP) / 3.0)) > 105 THEN 1 ELSE 0 END +
            CASE WHEN AGE >= 35 OR AGE < 18 THEN 1 ELSE 0 END
        ) AS preeclampsia_indicator_count,
        
        -- Severe features present
        CASE 
            WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN TRUE 
            ELSE FALSE 
        END AS has_severe_preeclampsia_features

    FROM base_data
)

SELECT * FROM core_features