-- models/gold/features/lifestyle_diseases/ftr_hypertension_core_clinical.sql

{{
    config(
        materialized='table',
        tags=['features', 'lifestyle_diseases', 'hypertension', 'tier1']
    )
}}

WITH base_data AS (
    SELECT * FROM {{ ref('stg_blood_pressure_cleaned') }}
),

core_features AS (
    SELECT
        *,
        
        -- ===== BLOOD PRESSURE STAGING (ACC/AHA 2017 Guidelines) =====
        
        -- Hypertension stage classification
        CASE 
            WHEN SYSTOLIC_BP < 120 AND DIASTOLIC_BP < 80 THEN 'normal'
            WHEN (SYSTOLIC_BP BETWEEN 120 AND 129) AND DIASTOLIC_BP < 80 THEN 'elevated'
            WHEN (SYSTOLIC_BP BETWEEN 130 AND 139) OR (DIASTOLIC_BP BETWEEN 80 AND 89) THEN 'stage1_hypertension'
            WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN 'stage2_hypertension'
            WHEN SYSTOLIC_BP >= 180 OR DIASTOLIC_BP >= 120 THEN 'hypertensive_crisis'
            ELSE 'unknown'
        END AS bp_stage,
        
        -- Systolic hypertension category
        CASE 
            WHEN SYSTOLIC_BP < 120 THEN 'normal'
            WHEN SYSTOLIC_BP BETWEEN 120 AND 129 THEN 'elevated'
            WHEN SYSTOLIC_BP BETWEEN 130 AND 139 THEN 'stage1'
            WHEN SYSTOLIC_BP BETWEEN 140 AND 159 THEN 'stage2_moderate'
            WHEN SYSTOLIC_BP BETWEEN 160 AND 179 THEN 'stage2_severe'
            WHEN SYSTOLIC_BP >= 180 THEN 'crisis'
            ELSE 'unknown'
        END AS systolic_category,
        
        -- Diastolic hypertension category
        CASE 
            WHEN DIASTOLIC_BP < 80 THEN 'normal'
            WHEN DIASTOLIC_BP BETWEEN 80 AND 89 THEN 'stage1'
            WHEN DIASTOLIC_BP BETWEEN 90 AND 99 THEN 'stage2_moderate'
            WHEN DIASTOLIC_BP BETWEEN 100 AND 119 THEN 'stage2_severe'
            WHEN DIASTOLIC_BP >= 120 THEN 'crisis'
            ELSE 'unknown'
        END AS diastolic_category,
        
        -- Pulse pressure (marker of arterial stiffness)
        (SYSTOLIC_BP - DIASTOLIC_BP) AS pulse_pressure,
        
        -- Pulse pressure category
        CASE 
            WHEN (SYSTOLIC_BP - DIASTOLIC_BP) < 40 THEN 'normal'
            WHEN (SYSTOLIC_BP - DIASTOLIC_BP) BETWEEN 40 AND 60 THEN 'borderline'
            WHEN (SYSTOLIC_BP - DIASTOLIC_BP) > 60 THEN 'widened_high_risk'
            ELSE 'unknown'
        END AS pulse_pressure_category,
        
        -- Isolated systolic hypertension (common in elderly)
        CASE 
            WHEN SYSTOLIC_BP >= 140 AND DIASTOLIC_BP < 90 THEN TRUE 
            ELSE FALSE 
        END AS has_isolated_systolic_htn,
        
        -- Mean arterial pressure (MAP)
        ROUND((DIASTOLIC_BP + (SYSTOLIC_BP - DIASTOLIC_BP) / 3.0), 1) AS mean_arterial_pressure,
        
        -- MAP category
        CASE 
            WHEN ((DIASTOLIC_BP + (SYSTOLIC_BP - DIASTOLIC_BP) / 3.0)) < 93 THEN 'normal'
            WHEN ((DIASTOLIC_BP + (SYSTOLIC_BP - DIASTOLIC_BP) / 3.0)) BETWEEN 93 AND 106 THEN 'elevated'
            WHEN ((DIASTOLIC_BP + (SYSTOLIC_BP - DIASTOLIC_BP) / 3.0)) > 106 THEN 'high'
            ELSE 'unknown'
        END AS map_category,
        
        -- ===== HEART RATE FEATURES =====
        
        -- Heart rate classification
        CASE 
            WHEN HEART_RATE < 60 THEN 'bradycardia'
            WHEN HEART_RATE BETWEEN 60 AND 100 THEN 'normal'
            WHEN HEART_RATE BETWEEN 101 AND 120 THEN 'mild_tachycardia'
            WHEN HEART_RATE > 120 THEN 'tachycardia'
            ELSE 'unknown'
        END AS heart_rate_category,
        
        -- ===== LIPID PANEL FEATURES =====
        
        -- Total cholesterol classification (NCEP ATP III)
        CASE 
            WHEN CHOLESTEROL < 200 THEN 'desirable'
            WHEN CHOLESTEROL BETWEEN 200 AND 239 THEN 'borderline_high'
            WHEN CHOLESTEROL >= 240 THEN 'high'
            ELSE 'unknown'
        END AS cholesterol_category,
        
        -- LDL classification
        CASE 
            WHEN LDL < 100 THEN 'optimal'
            WHEN LDL BETWEEN 100 AND 129 THEN 'near_optimal'
            WHEN LDL BETWEEN 130 AND 159 THEN 'borderline_high'
            WHEN LDL BETWEEN 160 AND 189 THEN 'high'
            WHEN LDL >= 190 THEN 'very_high'
            ELSE 'unknown'
        END AS ldl_category,
        
        -- HDL classification (higher is better)
        CASE 
            WHEN HDL < 40 THEN 'low_major_risk'
            WHEN HDL BETWEEN 40 AND 59 THEN 'borderline_low'
            WHEN HDL >= 60 THEN 'high_protective'
            ELSE 'unknown'
        END AS hdl_category,
        
        -- Triglycerides classification
        CASE 
            WHEN TRIGLYCERIDES < 150 THEN 'normal'
            WHEN TRIGLYCERIDES BETWEEN 150 AND 199 THEN 'borderline_high'
            WHEN TRIGLYCERIDES BETWEEN 200 AND 499 THEN 'high'
            WHEN TRIGLYCERIDES >= 500 THEN 'very_high'
            ELSE 'unknown'
        END AS triglycerides_category,
        
        -- Cholesterol/HDL ratio (atherogenic index)
        ROUND(CHOLESTEROL::FLOAT / NULLIF(HDL, 0), 2) AS cholesterol_hdl_ratio,
        
        -- Cholesterol ratio risk category
        CASE 
            WHEN CHOLESTEROL::FLOAT / NULLIF(HDL, 0) < 3.5 THEN 'low_risk'
            WHEN CHOLESTEROL::FLOAT / NULLIF(HDL, 0) BETWEEN 3.5 AND 5.0 THEN 'moderate_risk'
            WHEN CHOLESTEROL::FLOAT / NULLIF(HDL, 0) > 5.0 THEN 'high_risk'
            ELSE 'unknown'
        END AS cholesterol_ratio_risk,
        
        -- ===== BMI FEATURES (using same WHO classification) =====
        
        CASE 
            WHEN BMI < 18.5 THEN 'underweight'
            WHEN BMI BETWEEN 18.5 AND 24.9 THEN 'normal'
            WHEN BMI BETWEEN 25.0 AND 29.9 THEN 'overweight'
            WHEN BMI BETWEEN 30.0 AND 34.9 THEN 'obese_class1'
            WHEN BMI BETWEEN 35.0 AND 39.9 THEN 'obese_class2'
            WHEN BMI >= 40.0 THEN 'obese_class3_severe'
            ELSE 'unknown'
        END AS bmi_category,
        
        CASE WHEN BMI >= 30.0 THEN TRUE ELSE FALSE END AS is_obese,
        
        -- ===== GLUCOSE/DIABETES FEATURES =====
        
        -- Glucose status (fasting assumption)
        CASE 
            WHEN GLUCOSE < 100 THEN 'normal'
            WHEN GLUCOSE BETWEEN 100 AND 125 THEN 'prediabetes'
            WHEN GLUCOSE >= 126 THEN 'diabetes_range'
            ELSE 'unknown'
        END AS glucose_status,
        
        -- ===== LIFESTYLE RISK FACTORS =====
        
        -- Smoking status standardization
        CASE 
            WHEN LOWER(SMOKING_STATUS) = 'current' THEN 'current'
            WHEN LOWER(SMOKING_STATUS) = 'former' THEN 'former'
            WHEN LOWER(SMOKING_STATUS) = 'never' THEN 'never'
            ELSE 'unknown'
        END AS smoking_status_clean,
        
        CASE WHEN LOWER(SMOKING_STATUS) = 'current' THEN TRUE ELSE FALSE END AS is_current_smoker,
        
        -- Physical activity level standardization
        CASE 
            WHEN LOWER(PHYSICAL_ACTIVITY_LEVEL) = 'low' THEN 'sedentary'
            WHEN LOWER(PHYSICAL_ACTIVITY_LEVEL) = 'moderate' THEN 'moderate'
            WHEN LOWER(PHYSICAL_ACTIVITY_LEVEL) = 'high' THEN 'active'
            ELSE 'unknown'
        END AS activity_level_clean,
        
        CASE WHEN LOWER(PHYSICAL_ACTIVITY_LEVEL) = 'low' THEN TRUE ELSE FALSE END AS is_sedentary,
        
        -- Alcohol intake risk level
        CASE 
            WHEN ALCOHOL_INTAKE < 7 THEN 'low_moderate'
            WHEN ALCOHOL_INTAKE BETWEEN 7 AND 14 THEN 'moderate_high'
            WHEN ALCOHOL_INTAKE > 14 THEN 'excessive'
            ELSE 'unknown'
        END AS alcohol_risk_level,
        
        -- Salt intake risk
        CASE 
            WHEN SALT_INTAKE < 5 THEN 'within_guidelines'
            WHEN SALT_INTAKE BETWEEN 5 AND 10 THEN 'elevated'
            WHEN SALT_INTAKE > 10 THEN 'excessive'
            ELSE 'unknown'
        END AS salt_intake_risk,
        
        -- Sleep duration adequacy
        CASE 
            WHEN SLEEP_DURATION < 6 THEN 'insufficient'
            WHEN SLEEP_DURATION BETWEEN 6 AND 9 THEN 'adequate'
            WHEN SLEEP_DURATION > 9 THEN 'excessive'
            ELSE 'unknown'
        END AS sleep_adequacy,
        
        -- Stress level classification
        CASE 
            WHEN STRESS_LEVEL <= 3 THEN 'low'
            WHEN STRESS_LEVEL BETWEEN 4 AND 6 THEN 'moderate'
            WHEN STRESS_LEVEL BETWEEN 7 AND 8 THEN 'high'
            WHEN STRESS_LEVEL >= 9 THEN 'very_high'
            ELSE 'unknown'
        END AS stress_category,
        
        -- ===== COMORBIDITY FLAGS =====
        
        -- Hypertension status standardization
        CASE 
            WHEN LOWER(HYPERTENSION_STATUS) = 'high' THEN TRUE
            WHEN LOWER(HYPERTENSION_STATUS) = 'low' THEN FALSE
            ELSE NULL
        END AS has_hypertension_diagnosis

    FROM base_data
)

SELECT * FROM core_features