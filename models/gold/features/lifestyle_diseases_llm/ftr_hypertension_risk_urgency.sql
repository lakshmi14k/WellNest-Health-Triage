-- models/gold/features/lifestyle_diseases/ftr_hypertension_risk_urgency.sql

{{
    config(
        materialized='table',
        tags=['features', 'lifestyle_diseases', 'hypertension', 'tier2']
    )
}}

WITH core_features AS (
    SELECT * FROM {{ ref('ftr_hypertension_core_clinical') }}
),

risk_urgency_features AS (
    SELECT
        *,
        
        -- ===== EMERGENCY/URGENCY INDICATORS =====
        
        -- Hypertensive crisis detection
        CASE 
            WHEN SYSTOLIC_BP >= 180 OR DIASTOLIC_BP >= 120 THEN 'hypertensive_crisis_emergency'
            WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 100 THEN 'severe_urgency'
            WHEN bp_stage = 'stage2_hypertension' THEN 'moderate_urgency'
            WHEN bp_stage = 'stage1_hypertension' THEN 'needs_attention'
            ELSE 'routine'
        END AS bp_urgency_level,
        
        -- Crisis with symptoms flag (would need symptom data, but flag for discussion)
        CASE 
            WHEN SYSTOLIC_BP >= 180 OR DIASTOLIC_BP >= 120 THEN TRUE 
            ELSE FALSE 
        END AS potential_hypertensive_emergency,
        
        -- ===== CARDIOVASCULAR RISK SCORES =====
        
        -- Framingham-style CV risk score (0-15 scale, simplified)
        (
            -- Age risk points
            CASE 
                WHEN AGE < 40 THEN 0
                WHEN AGE BETWEEN 40 AND 49 THEN 1
                WHEN AGE BETWEEN 50 AND 59 THEN 2
                WHEN AGE BETWEEN 60 AND 69 THEN 3
                WHEN AGE >= 70 THEN 4
            END +
            -- BP risk points
            CASE 
                WHEN bp_stage IN ('normal', 'elevated') THEN 0
                WHEN bp_stage = 'stage1_hypertension' THEN 2
                WHEN bp_stage = 'stage2_hypertension' THEN 3
                WHEN bp_stage = 'hypertensive_crisis' THEN 4
            END +
            -- Cholesterol risk points
            CASE 
                WHEN cholesterol_category = 'desirable' THEN 0
                WHEN cholesterol_category = 'borderline_high' THEN 1
                WHEN cholesterol_category = 'high' THEN 2
            END +
            -- HDL protective/risk points
            CASE 
                WHEN hdl_category = 'high_protective' THEN -1
                WHEN hdl_category = 'low_major_risk' THEN 2
                ELSE 0
            END +
            -- Smoking points
            CASE WHEN is_current_smoker THEN 2 ELSE 0 END +
            -- Diabetes points
            CASE WHEN HAS_DIABETES OR glucose_status = 'diabetes_range' THEN 2 ELSE 0 END +
            -- Obesity points
            CASE WHEN is_obese THEN 1 ELSE 0 END
        ) AS cardiovascular_risk_score,
        
        -- 10-year CV risk category (based on simplified score)
        CASE 
            WHEN (
                CASE 
                    WHEN AGE < 40 THEN 0
                    WHEN AGE BETWEEN 40 AND 49 THEN 1
                    WHEN AGE BETWEEN 50 AND 59 THEN 2
                    WHEN AGE BETWEEN 60 AND 69 THEN 3
                    WHEN AGE >= 70 THEN 4
                END +
                CASE 
                    WHEN bp_stage IN ('normal', 'elevated') THEN 0
                    WHEN bp_stage = 'stage1_hypertension' THEN 2
                    WHEN bp_stage = 'stage2_hypertension' THEN 3
                    WHEN bp_stage = 'hypertensive_crisis' THEN 4
                END +
                CASE 
                    WHEN cholesterol_category = 'desirable' THEN 0
                    WHEN cholesterol_category = 'borderline_high' THEN 1
                    WHEN cholesterol_category = 'high' THEN 2
                END +
                CASE 
                    WHEN hdl_category = 'high_protective' THEN -1
                    WHEN hdl_category = 'low_major_risk' THEN 2
                    ELSE 0
                END +
                CASE WHEN is_current_smoker THEN 2 ELSE 0 END +
                CASE WHEN HAS_DIABETES OR glucose_status = 'diabetes_range' THEN 2 ELSE 0 END +
                CASE WHEN is_obese THEN 1 ELSE 0 END
            ) < 5 THEN 'low_risk'
            WHEN (
                CASE 
                    WHEN AGE < 40 THEN 0
                    WHEN AGE BETWEEN 40 AND 49 THEN 1
                    WHEN AGE BETWEEN 50 AND 59 THEN 2
                    WHEN AGE BETWEEN 60 AND 69 THEN 3
                    WHEN AGE >= 70 THEN 4
                END +
                CASE 
                    WHEN bp_stage IN ('normal', 'elevated') THEN 0
                    WHEN bp_stage = 'stage1_hypertension' THEN 2
                    WHEN bp_stage = 'stage2_hypertension' THEN 3
                    WHEN bp_stage = 'hypertensive_crisis' THEN 4
                END +
                CASE 
                    WHEN cholesterol_category = 'desirable' THEN 0
                    WHEN cholesterol_category = 'borderline_high' THEN 1
                    WHEN cholesterol_category = 'high' THEN 2
                END +
                CASE 
                    WHEN hdl_category = 'high_protective' THEN -1
                    WHEN hdl_category = 'low_major_risk' THEN 2
                    ELSE 0
                END +
                CASE WHEN is_current_smoker THEN 2 ELSE 0 END +
                CASE WHEN HAS_DIABETES OR glucose_status = 'diabetes_range' THEN 2 ELSE 0 END +
                CASE WHEN is_obese THEN 1 ELSE 0 END
            ) BETWEEN 5 AND 9 THEN 'moderate_risk'
            WHEN (
                CASE 
                    WHEN AGE < 40 THEN 0
                    WHEN AGE BETWEEN 40 AND 49 THEN 1
                    WHEN AGE BETWEEN 50 AND 59 THEN 2
                    WHEN AGE BETWEEN 60 AND 69 THEN 3
                    WHEN AGE >= 70 THEN 4
                END +
                CASE 
                    WHEN bp_stage IN ('normal', 'elevated') THEN 0
                    WHEN bp_stage = 'stage1_hypertension' THEN 2
                    WHEN bp_stage = 'stage2_hypertension' THEN 3
                    WHEN bp_stage = 'hypertensive_crisis' THEN 4
                END +
                CASE 
                    WHEN cholesterol_category = 'desirable' THEN 0
                    WHEN cholesterol_category = 'borderline_high' THEN 1
                    WHEN cholesterol_category = 'high' THEN 2
                END +
                CASE 
                    WHEN hdl_category = 'high_protective' THEN -1
                    WHEN hdl_category = 'low_major_risk' THEN 2
                    ELSE 0
                END +
                CASE WHEN is_current_smoker THEN 2 ELSE 0 END +
                CASE WHEN HAS_DIABETES OR glucose_status = 'diabetes_range' THEN 2 ELSE 0 END +
                CASE WHEN is_obese THEN 1 ELSE 0 END
            ) >= 10 THEN 'high_risk'
        END AS ten_year_cv_risk_category,
        
        -- Metabolic syndrome criteria count (need 3+ for diagnosis)
        (
            CASE WHEN bp_stage NOT IN ('normal', 'elevated') THEN 1 ELSE 0 END +
            CASE WHEN glucose_status IN ('prediabetes', 'diabetes_range') THEN 1 ELSE 0 END +
            CASE WHEN triglycerides_category IN ('high', 'very_high') THEN 1 ELSE 0 END +
            CASE WHEN hdl_category = 'low_major_risk' THEN 1 ELSE 0 END +
            CASE WHEN BMI >= 30 THEN 1 ELSE 0 END
        ) AS metabolic_syndrome_criteria_count,
        
        -- Metabolic syndrome flag
        CASE 
            WHEN (
                CASE WHEN bp_stage NOT IN ('normal', 'elevated') THEN 1 ELSE 0 END +
                CASE WHEN glucose_status IN ('prediabetes', 'diabetes_range') THEN 1 ELSE 0 END +
                CASE WHEN triglycerides_category IN ('high', 'very_high') THEN 1 ELSE 0 END +
                CASE WHEN hdl_category = 'low_major_risk' THEN 1 ELSE 0 END +
                CASE WHEN BMI >= 30 THEN 1 ELSE 0 END
            ) >= 3 THEN TRUE 
            ELSE FALSE 
        END AS has_metabolic_syndrome,
        
        -- Stroke risk factors count
        (
            CASE WHEN bp_stage IN ('stage2_hypertension', 'hypertensive_crisis') THEN 1 ELSE 0 END +
            CASE WHEN AGE >= 65 THEN 1 ELSE 0 END +
            CASE WHEN HAS_DIABETES OR glucose_status = 'diabetes_range' THEN 1 ELSE 0 END +
            CASE WHEN is_current_smoker THEN 1 ELSE 0 END +
            CASE WHEN cholesterol_category = 'high' THEN 1 ELSE 0 END +
            CASE WHEN HAS_FAMILY_HISTORY THEN 1 ELSE 0 END
        ) AS stroke_risk_factors_count,
        
        -- ===== LIFESTYLE MODIFICATION URGENCY =====
        
        -- Dietary intervention priority (DASH diet indication)
        CASE 
            WHEN bp_stage IN ('stage2_hypertension', 'hypertensive_crisis') THEN 'critical'
            WHEN bp_stage = 'stage1_hypertension' AND salt_intake_risk = 'excessive' THEN 'high'
            WHEN bp_stage = 'stage1_hypertension' THEN 'moderate'
            WHEN bp_stage = 'elevated' THEN 'preventive'
            ELSE 'maintenance'
        END AS dietary_modification_priority,
        
        -- Exercise prescription urgency
        CASE 
            WHEN is_sedentary AND bp_stage IN ('stage1_hypertension', 'stage2_hypertension') THEN 'high'
            WHEN is_sedentary AND bp_stage = 'elevated' THEN 'moderate'
            WHEN is_sedentary THEN 'preventive'
            ELSE 'maintenance'
        END AS exercise_priority,
        
        -- Weight loss priority
        CASE 
            WHEN is_obese AND bp_stage IN ('stage2_hypertension', 'hypertensive_crisis') THEN 'critical'
            WHEN is_obese AND bp_stage = 'stage1_hypertension' THEN 'high'
            WHEN BMI >= 25 AND bp_stage IN ('elevated', 'stage1_hypertension') THEN 'moderate'
            ELSE 'maintenance'
        END AS weight_loss_priority,
        
        -- Smoking cessation urgency (for hypertension)
        CASE 
            WHEN is_current_smoker AND bp_stage IN ('stage2_hypertension', 'hypertensive_crisis') THEN 'critical'
            WHEN is_current_smoker AND bp_stage = 'stage1_hypertension' THEN 'high'
            WHEN is_current_smoker THEN 'moderate'
            WHEN smoking_status_clean = 'former' THEN 'relapse_prevention'
            ELSE 'not_applicable'
        END AS smoking_cessation_priority_htn,
        
        -- Alcohol reduction priority
        CASE 
            WHEN alcohol_risk_level = 'excessive' AND bp_stage NOT IN ('normal', 'elevated') THEN 'high'
            WHEN alcohol_risk_level = 'excessive' THEN 'moderate'
            WHEN alcohol_risk_level = 'moderate_high' AND bp_stage NOT IN ('normal', 'elevated') THEN 'moderate'
            ELSE 'maintenance'
        END AS alcohol_reduction_priority,
        
        -- Stress management priority
        CASE 
            WHEN stress_category IN ('high', 'very_high') AND bp_stage NOT IN ('normal', 'elevated') THEN 'high'
            WHEN stress_category IN ('high', 'very_high') THEN 'moderate'
            ELSE 'low'
        END AS stress_management_priority,
        
        -- ===== MEDICATION THERAPY INDICATORS =====
        
        -- Likely needs pharmacotherapy (ACC/AHA guidelines)
        CASE 
            WHEN bp_stage IN ('stage2_hypertension', 'hypertensive_crisis') THEN TRUE
            WHEN bp_stage = 'stage1_hypertension' AND cardiovascular_risk_score >= 8 THEN TRUE
            WHEN bp_stage = 'stage1_hypertension' AND (HAS_DIABETES OR glucose_status = 'diabetes_range') THEN TRUE
            ELSE FALSE
        END AS likely_needs_medication,
        
        -- Medication urgency level
        CASE 
            WHEN bp_stage = 'hypertensive_crisis' THEN 'immediate'
            WHEN bp_stage = 'stage2_hypertension' THEN 'urgent'
            WHEN bp_stage = 'stage1_hypertension' AND cardiovascular_risk_score >= 8 THEN 'prompt'
            WHEN bp_stage = 'stage1_hypertension' THEN 'consider'
            ELSE 'lifestyle_first'
        END AS medication_urgency,
        
        -- ===== TARGET ORGAN DAMAGE RISK =====
        
        -- Heart damage risk score
        (
            CASE 
                WHEN bp_stage = 'hypertensive_crisis' THEN 4
                WHEN bp_stage = 'stage2_hypertension' THEN 3
                WHEN bp_stage = 'stage1_hypertension' THEN 2
                WHEN bp_stage = 'elevated' THEN 1
                ELSE 0
            END +
            CASE WHEN pulse_pressure_category = 'widened_high_risk' THEN 2 ELSE 0 END +
            CASE WHEN cholesterol_category = 'high' THEN 1 ELSE 0 END +
            CASE WHEN is_current_smoker THEN 1 ELSE 0 END
        ) AS cardiac_damage_risk_score,
        
        -- Kidney damage risk indicators
        CASE 
            WHEN bp_stage IN ('stage2_hypertension', 'hypertensive_crisis') AND (HAS_DIABETES OR glucose_status = 'diabetes_range') THEN 'high_nephropathy_risk'
            WHEN bp_stage IN ('stage2_hypertension', 'hypertensive_crisis') THEN 'moderate_nephropathy_risk'
            ELSE 'low_risk'
        END AS kidney_damage_risk,
        
        -- ===== AGE-SPECIFIC RISK =====
        
        -- Age-BP risk category
        CASE 
            WHEN AGE < 40 AND bp_stage IN ('stage1_hypertension', 'stage2_hypertension', 'hypertensive_crisis') THEN 'young_onset_critical'
            WHEN AGE BETWEEN 40 AND 64 AND bp_stage IN ('stage2_hypertension', 'hypertensive_crisis') THEN 'midlife_high_risk'
            WHEN AGE >= 65 AND bp_stage IN ('stage2_hypertension', 'hypertensive_crisis') THEN 'elderly_high_risk'
            WHEN AGE >= 65 AND has_isolated_systolic_htn THEN 'isolated_systolic_elderly'
            ELSE 'standard_management'
        END AS age_bp_risk_category

    FROM core_features
)

SELECT * FROM risk_urgency_features