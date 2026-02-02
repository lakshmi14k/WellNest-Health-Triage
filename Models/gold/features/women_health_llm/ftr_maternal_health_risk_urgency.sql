-- models/gold/features/womens_wellness_llm/ftr_maternal_health_risk_urgency.sql

{{
    config(
        materialized='table',
        tags=['features', 'womens_wellness', 'maternal_health', 'tier2']
    )
}}

WITH core_features AS (
    SELECT * FROM {{ ref('ftr_maternal_health_core_clinical') }}
),

risk_urgency_features AS (
    SELECT
        *,
        
        -- ===== HYPERTENSIVE DISORDERS RISK ASSESSMENT =====
        
        -- Preeclampsia risk score (0-10 scale)
        (
            -- BP criteria (most important)
            CASE 
                WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN 5
                WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN 3
                WHEN SYSTOLIC_BP >= 120 OR DIASTOLIC_BP >= 80 THEN 1
                ELSE 0
            END +
            -- Age risk
            CASE 
                WHEN AGE < 18 OR AGE >= 40 THEN 2
                WHEN AGE >= 35 THEN 1
                ELSE 0
            END +
            -- Other risk factors
            CASE WHEN BLOOD_SUGAR >= 140 THEN 1 ELSE 0 END +
            CASE WHEN HEART_RATE > 100 THEN 1 ELSE 0 END
        ) AS preeclampsia_risk_score,
        
        -- Preeclampsia risk category
        CASE 
            WHEN (
                CASE 
                    WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN 5
                    WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN 3
                    WHEN SYSTOLIC_BP >= 120 OR DIASTOLIC_BP >= 80 THEN 1
                    ELSE 0
                END +
                CASE 
                    WHEN AGE < 18 OR AGE >= 40 THEN 2
                    WHEN AGE >= 35 THEN 1
                    ELSE 0
                END +
                CASE WHEN BLOOD_SUGAR >= 140 THEN 1 ELSE 0 END +
                CASE WHEN HEART_RATE > 100 THEN 1 ELSE 0 END
            ) = 0 THEN 'low_risk'
            WHEN (
                CASE 
                    WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN 5
                    WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN 3
                    WHEN SYSTOLIC_BP >= 120 OR DIASTOLIC_BP >= 80 THEN 1
                    ELSE 0
                END +
                CASE 
                    WHEN AGE < 18 OR AGE >= 40 THEN 2
                    WHEN AGE >= 35 THEN 1
                    ELSE 0
                END +
                CASE WHEN BLOOD_SUGAR >= 140 THEN 1 ELSE 0 END +
                CASE WHEN HEART_RATE > 100 THEN 1 ELSE 0 END
            ) BETWEEN 1 AND 3 THEN 'moderate_risk'
            WHEN (
                CASE 
                    WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN 5
                    WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN 3
                    WHEN SYSTOLIC_BP >= 120 OR DIASTOLIC_BP >= 80 THEN 1
                    ELSE 0
                END +
                CASE 
                    WHEN AGE < 18 OR AGE >= 40 THEN 2
                    WHEN AGE >= 35 THEN 1
                    ELSE 0
                END +
                CASE WHEN BLOOD_SUGAR >= 140 THEN 1 ELSE 0 END +
                CASE WHEN HEART_RATE > 100 THEN 1 ELSE 0 END
            ) BETWEEN 4 AND 6 THEN 'high_risk'
            WHEN (
                CASE 
                    WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN 5
                    WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN 3
                    WHEN SYSTOLIC_BP >= 120 OR DIASTOLIC_BP >= 80 THEN 1
                    ELSE 0
                END +
                CASE 
                    WHEN AGE < 18 OR AGE >= 40 THEN 2
                    WHEN AGE >= 35 THEN 1
                    ELSE 0
                END +
                CASE WHEN BLOOD_SUGAR >= 140 THEN 1 ELSE 0 END +
                CASE WHEN HEART_RATE > 100 THEN 1 ELSE 0 END
            ) >= 7 THEN 'severe_risk'
        END AS preeclampsia_risk_category,
        
        -- ===== GESTATIONAL DIABETES RISK =====
        
        -- GDM risk score (0-8 scale)
        (
            CASE 
                WHEN BLOOD_SUGAR >= 200 THEN 4
                WHEN BLOOD_SUGAR >= 140 THEN 2
                ELSE 0
            END +
            CASE 
                WHEN AGE >= 35 THEN 2
                WHEN AGE >= 30 THEN 1
                ELSE 0
            END +
            CASE WHEN pregnancy_bp_stage NOT IN ('normal') THEN 1 ELSE 0 END +
            CASE WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN 1 ELSE 0 END
        ) AS gestational_diabetes_risk_score,
        
        -- GDM risk category
        CASE 
            WHEN (
                CASE 
                    WHEN BLOOD_SUGAR >= 200 THEN 4
                    WHEN BLOOD_SUGAR >= 140 THEN 2
                    ELSE 0
                END +
                CASE 
                    WHEN AGE >= 35 THEN 2
                    WHEN AGE >= 30 THEN 1
                    ELSE 0
                END +
                CASE WHEN pregnancy_bp_stage NOT IN ('normal') THEN 1 ELSE 0 END +
                CASE WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN 1 ELSE 0 END
            ) = 0 THEN 'low_risk'
            WHEN (
                CASE 
                    WHEN BLOOD_SUGAR >= 200 THEN 4
                    WHEN BLOOD_SUGAR >= 140 THEN 2
                    ELSE 0
                END +
                CASE 
                    WHEN AGE >= 35 THEN 2
                    WHEN AGE >= 30 THEN 1
                    ELSE 0
                END +
                CASE WHEN pregnancy_bp_stage NOT IN ('normal') THEN 1 ELSE 0 END +
                CASE WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN 1 ELSE 0 END
            ) BETWEEN 1 AND 3 THEN 'moderate_risk'
            WHEN (
                CASE 
                    WHEN BLOOD_SUGAR >= 200 THEN 4
                    WHEN BLOOD_SUGAR >= 140 THEN 2
                    ELSE 0
                END +
                CASE 
                    WHEN AGE >= 35 THEN 2
                    WHEN AGE >= 30 THEN 1
                    ELSE 0
                END +
                CASE WHEN pregnancy_bp_stage NOT IN ('normal') THEN 1 ELSE 0 END +
                CASE WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN 1 ELSE 0 END
            ) >= 4 THEN 'high_risk'
        END AS gestational_diabetes_risk_category,
        
        -- ===== OVERALL MATERNAL HEALTH URGENCY =====
        
        -- Maternal emergency indicators
        CASE 
            WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN 'hypertensive_emergency'
            WHEN BODY_TEMPERATURE >= 103.0 THEN 'high_fever_emergency'
            WHEN BLOOD_SUGAR >= 250 THEN 'hyperglycemic_emergency'
            WHEN HEART_RATE > 130 THEN 'severe_tachycardia_emergency'
            ELSE 'no_emergency'
        END AS maternal_emergency_type,
        
        -- Overall urgency level
        CASE 
            -- Emergency: Severe features requiring immediate evaluation
            WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN 'emergency'
            WHEN BODY_TEMPERATURE >= 103.0 THEN 'emergency'
            WHEN BLOOD_SUGAR >= 250 THEN 'emergency'
            WHEN HEART_RATE > 130 THEN 'emergency'
            
            -- Urgent: Multiple abnormal vitals or concerning features
            WHEN abnormal_vitals_count >= 3 THEN 'urgent'
            WHEN pregnancy_bp_stage = 'gestational_htn_stage1' THEN 'urgent'
            WHEN BLOOD_SUGAR BETWEEN 200 AND 249 THEN 'urgent'
            WHEN BODY_TEMPERATURE BETWEEN 101.0 AND 102.9 THEN 'urgent'
            
            -- Needs Attention: Single abnormality or borderline values
            WHEN abnormal_vitals_count >= 1 THEN 'needs_attention'
            WHEN pregnancy_bp_stage = 'elevated_monitor' THEN 'needs_attention'
            WHEN BLOOD_SUGAR BETWEEN 140 AND 199 THEN 'needs_attention'
            
            -- Routine: Normal or near-normal vitals
            ELSE 'routine'
        END AS maternal_urgency_level,
        
        -- ===== COMPLICATION RISK SCORES =====
        
        -- Maternal complication composite score (0-15 scale)
        (
            CASE 
                WHEN preeclampsia_bp_warning THEN 3
                WHEN pregnancy_bp_stage = 'elevated_monitor' THEN 1
                ELSE 0
            END +
            CASE WHEN gestational_diabetes_risk THEN 2 ELSE 0 END +
            CASE WHEN has_age_related_risk THEN 2 ELSE 0 END +
            CASE WHEN has_tachycardia THEN 2 ELSE 0 END +
            CASE WHEN has_fever THEN 2 ELSE 0 END +
            CASE WHEN abnormal_vitals_count >= 2 THEN 2 ELSE 0 END +
            CASE WHEN severe_preeclampsia_bp_warning THEN 2 ELSE 0 END
        ) AS maternal_complication_risk_score,
        
        -- Fetal risk indicators (based on maternal status)
        CASE 
            WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN 'high_fetal_risk'
            WHEN BLOOD_SUGAR >= 200 THEN 'high_fetal_risk'
            WHEN preeclampsia_bp_warning THEN 'moderate_fetal_risk'
            WHEN gestational_diabetes_risk THEN 'moderate_fetal_risk'
            ELSE 'standard_monitoring'
        END AS fetal_risk_category,
        
        -- ===== MONITORING INTENSITY REQUIREMENTS =====
        
        -- BP monitoring frequency needed
        CASE 
            WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN 'continuous_hospital'
            WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN 'twice_daily_home'
            WHEN pregnancy_bp_stage = 'elevated_monitor' THEN 'daily_home'
            ELSE 'routine_prenatal'
        END AS bp_monitoring_frequency,
        
        -- Blood sugar monitoring frequency
        CASE 
            WHEN BLOOD_SUGAR >= 200 THEN 'multiple_daily_monitoring'
            WHEN BLOOD_SUGAR >= 140 THEN 'fasting_postprandial_monitoring'
            ELSE 'routine_screening'
        END AS glucose_monitoring_frequency,
        
        -- ===== INTERVENTION PRIORITIES =====
        
        -- Medication therapy likely needed
        CASE 
            WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN TRUE
            WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN TRUE
            WHEN BLOOD_SUGAR >= 200 THEN TRUE
            ELSE FALSE
        END AS likely_needs_medication,
        
        -- Hospitalization consideration
        CASE 
            WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN 'immediate_hospitalization'
            WHEN preeclampsia_risk_score >= 7 THEN 'consider_admission'
            WHEN abnormal_vitals_count >= 3 THEN 'observation_needed'
            ELSE 'outpatient_management'
        END AS hospitalization_recommendation,
        
        -- Delivery timing consideration
        CASE 
            WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN 'expedited_delivery_consideration'
            WHEN preeclampsia_risk_score >= 8 THEN 'early_delivery_planning'
            ELSE 'routine_delivery_planning'
        END AS delivery_timing_consideration

    FROM core_features
)

SELECT * FROM risk_urgency_features