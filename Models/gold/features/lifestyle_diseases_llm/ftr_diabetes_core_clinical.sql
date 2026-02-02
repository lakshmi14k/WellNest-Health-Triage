-- models/gold/features/ftr_diabetes_core_clinical.sql

{{
    config(
        materialized='table',
        tags=['features', 'lifestyle_diseases', 'tier1']
    )
}}

WITH base_data AS (
    SELECT * FROM {{ ref('stg_diabetes_cleaned') }}
),

core_features AS (
    SELECT
        *,
        
        -- ===== DIABETES SEVERITY FEATURES =====
        
        -- HbA1c-based diabetes staging (ADA guidelines)
        CASE 
            WHEN HBA1C_LEVEL < 5.7 THEN 'normal'
            WHEN HBA1C_LEVEL BETWEEN 5.7 AND 6.4 THEN 'prediabetes'
            WHEN HBA1C_LEVEL BETWEEN 6.5 AND 7.9 THEN 'diabetes_controlled'
            WHEN HBA1C_LEVEL BETWEEN 8.0 AND 9.9 THEN 'diabetes_uncontrolled'
            WHEN HBA1C_LEVEL >= 10.0 THEN 'diabetes_severe'
            ELSE 'unknown'
        END AS diabetes_stage,
        
        -- Glucose control status (fasting glucose assumption)
        CASE 
            WHEN BLOOD_GLUCOSE_LEVEL < 100 THEN 'normal'
            WHEN BLOOD_GLUCOSE_LEVEL BETWEEN 100 AND 125 THEN 'prediabetes_range'
            WHEN BLOOD_GLUCOSE_LEVEL BETWEEN 126 AND 180 THEN 'diabetes_mild'
            WHEN BLOOD_GLUCOSE_LEVEL BETWEEN 181 AND 250 THEN 'diabetes_moderate'
            WHEN BLOOD_GLUCOSE_LEVEL > 250 THEN 'diabetes_severe'
            ELSE 'unknown'
        END AS glucose_control_status,
        
        -- Glucose-HbA1c concordance (are they telling same story?)
        CASE 
            WHEN (BLOOD_GLUCOSE_LEVEL < 126 AND HBA1C_LEVEL < 6.5) THEN 'concordant_normal'
            WHEN (BLOOD_GLUCOSE_LEVEL >= 126 AND HBA1C_LEVEL >= 6.5) THEN 'concordant_diabetic'
            WHEN (BLOOD_GLUCOSE_LEVEL >= 126 AND HBA1C_LEVEL < 6.5) THEN 'discordant_acute_high'
            WHEN (BLOOD_GLUCOSE_LEVEL < 126 AND HBA1C_LEVEL >= 6.5) THEN 'discordant_chronic_poor'
            ELSE 'unknown'
        END AS glucose_hba1c_concordance,
        
        -- ===== BMI & METABOLIC FEATURES =====
        
        -- BMI categories (WHO classification)
        CASE 
            WHEN BMI < 18.5 THEN 'underweight'
            WHEN BMI BETWEEN 18.5 AND 24.9 THEN 'normal'
            WHEN BMI BETWEEN 25.0 AND 29.9 THEN 'overweight'
            WHEN BMI BETWEEN 30.0 AND 34.9 THEN 'obese_class1'
            WHEN BMI BETWEEN 35.0 AND 39.9 THEN 'obese_class2'
            WHEN BMI >= 40.0 THEN 'obese_class3_severe'
            ELSE 'unknown'
        END AS bmi_category,
        
        -- Obesity flag (BMI >= 30)
        CASE WHEN BMI >= 30.0 THEN TRUE ELSE FALSE END AS is_obese,
        
        -- Severe obesity flag (BMI >= 40)
        CASE WHEN BMI >= 40.0 THEN TRUE ELSE FALSE END AS is_severely_obese,
        
        -- ===== CARDIOVASCULAR RISK FEATURES =====
        
        -- Cardiometabolic disease count (comorbidity burden)
        (CASE WHEN HAS_DIABETES THEN 1 ELSE 0 END +
         CASE WHEN HAS_HYPERTENSION THEN 1 ELSE 0 END +
         CASE WHEN HAS_HEART_DISEASE THEN 1 ELSE 0 END) AS cardiometabolic_disease_count,
        
        -- Multiple condition flags
        CASE 
            WHEN HAS_DIABETES AND HAS_HYPERTENSION AND HAS_HEART_DISEASE THEN TRUE 
            ELSE FALSE 
        END AS has_triple_diagnosis,
        
        CASE 
            WHEN (CASE WHEN HAS_DIABETES THEN 1 ELSE 0 END +
                  CASE WHEN HAS_HYPERTENSION THEN 1 ELSE 0 END +
                  CASE WHEN HAS_HEART_DISEASE THEN 1 ELSE 0 END) >= 2 THEN TRUE 
            ELSE FALSE 
        END AS has_multiple_conditions,
        
        -- ===== SMOKING RISK FEATURES =====
        
        -- Smoking status standardization
        CASE 
            WHEN LOWER(SMOKING_HISTORY) IN ('current', 'current smoker') THEN 'current'
            WHEN LOWER(SMOKING_HISTORY) IN ('former', 'ex-smoker', 'not current') THEN 'former'
            WHEN LOWER(SMOKING_HISTORY) IN ('never', 'non-smoker') THEN 'never'
            WHEN LOWER(SMOKING_HISTORY) IN ('ever', 'yes') THEN 'ever'
            ELSE 'no_info'
        END AS smoking_status_clean,
        
        -- Current smoker flag
        CASE 
            WHEN LOWER(SMOKING_HISTORY) IN ('current', 'current smoker') THEN TRUE 
            ELSE FALSE 
        END AS is_current_smoker,
        
        -- Any smoking history
        CASE 
            WHEN LOWER(SMOKING_HISTORY) NOT IN ('never', 'non-smoker', 'no info') THEN TRUE 
            ELSE FALSE 
        END AS has_smoking_history

    FROM base_data
)

SELECT * FROM core_features