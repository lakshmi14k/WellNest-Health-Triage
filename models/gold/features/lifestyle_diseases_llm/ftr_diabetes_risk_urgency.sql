-- models/gold/features/ftr_diabetes_risk_urgency.sql

{{
    config(
        materialized='table',
        tags=['features', 'lifestyle_diseases', 'tier2']
    )
}}

WITH core_features AS (
    SELECT * FROM {{ ref('ftr_diabetes_core_clinical') }}
),

risk_urgency_features AS (
    SELECT
        *,
        
        -- ===== EMERGENCY/URGENCY INDICATORS =====
        
        -- Hyperglycemic emergency risk (DKA/HHS risk)
        CASE 
            WHEN BLOOD_GLUCOSE_LEVEL > 400 THEN 'emergency_hyperglycemia'
            WHEN BLOOD_GLUCOSE_LEVEL BETWEEN 300 AND 400 THEN 'urgent_hyperglycemia'
            WHEN BLOOD_GLUCOSE_LEVEL BETWEEN 250 AND 299 THEN 'concerning_hyperglycemia'
            ELSE 'stable'
        END AS hyperglycemia_urgency,
        
        -- Hypoglycemia risk (especially dangerous)
        CASE 
            WHEN BLOOD_GLUCOSE_LEVEL < 54 THEN 'emergency_hypoglycemia'
            WHEN BLOOD_GLUCOSE_LEVEL BETWEEN 54 AND 70 THEN 'urgent_hypoglycemia'
            ELSE 'no_hypoglycemia'
        END AS hypoglycemia_urgency,
        
        -- Overall glucose urgency flag
        CASE 
            WHEN BLOOD_GLUCOSE_LEVEL < 54 OR BLOOD_GLUCOSE_LEVEL > 400 THEN 'emergency'
            WHEN (BLOOD_GLUCOSE_LEVEL BETWEEN 54 AND 70) OR (BLOOD_GLUCOSE_LEVEL BETWEEN 300 AND 400) THEN 'urgent'
            WHEN BLOOD_GLUCOSE_LEVEL BETWEEN 250 AND 299 THEN 'needs_attention'
            ELSE 'routine'
        END AS glucose_urgency_level,
        
        -- ===== COMPOSITE RISK SCORES =====
        
        -- Cardiovascular risk score (0-10 scale)
        (
            CASE WHEN HAS_DIABETES THEN 2 ELSE 0 END +
            CASE WHEN HAS_HYPERTENSION THEN 2 ELSE 0 END +
            CASE WHEN HAS_HEART_DISEASE THEN 3 ELSE 0 END +
            CASE WHEN is_current_smoker THEN 2 ELSE 0 END +
            CASE WHEN BMI >= 30 THEN 1 ELSE 0 END
        ) AS cardiovascular_risk_score,
        
        -- Metabolic syndrome risk score (0-8 scale)
        (
            CASE WHEN BMI >= 30 THEN 2 ELSE 0 END +
            CASE WHEN HBA1C_LEVEL >= 5.7 THEN 2 ELSE 0 END +
            CASE WHEN HAS_HYPERTENSION THEN 2 ELSE 0 END +
            CASE WHEN BLOOD_GLUCOSE_LEVEL >= 100 THEN 2 ELSE 0 END
        ) AS metabolic_syndrome_score,
        
        -- Diabetes complication risk (0-12 scale)
        (
            CASE WHEN HBA1C_LEVEL >= 9.0 THEN 4
                 WHEN HBA1C_LEVEL >= 8.0 THEN 3
                 WHEN HBA1C_LEVEL >= 7.0 THEN 2
                 WHEN HBA1C_LEVEL >= 6.5 THEN 1
                 ELSE 0 END +
            CASE WHEN HAS_HYPERTENSION THEN 2 ELSE 0 END +
            CASE WHEN HAS_HEART_DISEASE THEN 3 ELSE 0 END +
            CASE WHEN is_current_smoker THEN 2 ELSE 0 END +
            CASE WHEN BMI >= 35 THEN 1 ELSE 0 END
        ) AS diabetes_complication_risk_score,
        
        -- ===== LIFESTYLE MODIFICATION URGENCY =====
        
        -- Weight management priority
        CASE 
            WHEN BMI >= 40 THEN 'critical'
            WHEN BMI >= 35 AND (HAS_DIABETES OR HAS_HYPERTENSION) THEN 'high'
            WHEN BMI >= 30 THEN 'moderate'
            WHEN BMI BETWEEN 25 AND 29.9 THEN 'low'
            ELSE 'maintenance'
        END AS weight_management_priority,
        
        -- Smoking cessation urgency
        CASE 
            WHEN is_current_smoker AND (HAS_HEART_DISEASE OR HAS_DIABETES) THEN 'critical'
            WHEN is_current_smoker AND HAS_HYPERTENSION THEN 'high'
            WHEN is_current_smoker THEN 'moderate'
            WHEN smoking_status_clean = 'former' THEN 'relapse_prevention'
            ELSE 'not_applicable'
        END AS smoking_cessation_priority,
        
        -- ===== AGE-ADJUSTED RISK =====
        
        -- Age risk category for diabetes complications
        CASE 
            WHEN AGE < 40 THEN 'young_onset_high_risk'
            WHEN AGE BETWEEN 40 AND 64 THEN 'standard_risk'
            WHEN AGE >= 65 THEN 'elderly_high_risk'
        END AS age_risk_category,
        
        -- Premature disease flag (diabetes or heart disease before 45)
        CASE 
            WHEN AGE < 45 AND (HAS_DIABETES OR HAS_HEART_DISEASE) THEN TRUE 
            ELSE FALSE 
        END AS has_premature_disease

    FROM core_features
)

SELECT * FROM risk_urgency_features