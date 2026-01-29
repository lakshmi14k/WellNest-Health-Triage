-- models/gold/features/womens_wellness_llm/ftr_maternal_health_conversation_prompts.sql

{{
    config(
        materialized='table',
        tags=['features', 'womens_wellness', 'maternal_health', 'tier3']
    )
}}

WITH risk_features AS (
    SELECT * FROM {{ ref('ftr_maternal_health_risk_urgency') }}
),

conversation_features AS (
    SELECT
        *,
        
        -- ===== PREECLAMPSIA SYMPTOM INQUIRY FLAGS =====
        
        -- Should ask about preeclampsia warning signs
        CASE 
            WHEN preeclampsia_bp_warning THEN TRUE
            WHEN preeclampsia_risk_score >= 3 THEN TRUE
            ELSE FALSE
        END AS should_ask_preeclampsia_symptoms,
        
        -- Should ask about severe headaches
        CASE 
            WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN TRUE
            WHEN preeclampsia_risk_score >= 4 THEN TRUE
            ELSE FALSE
        END AS should_ask_headaches,
        
        -- Should ask about vision changes
        CASE 
            WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN TRUE
            WHEN severe_preeclampsia_bp_warning THEN TRUE
            ELSE FALSE
        END AS should_ask_vision_changes,
        
        -- Should ask about upper abdominal pain
        CASE 
            WHEN preeclampsia_bp_warning THEN TRUE
            WHEN preeclampsia_risk_score >= 5 THEN TRUE
            ELSE FALSE
        END AS should_ask_abdominal_pain,
        
        -- Should ask about swelling/edema
        CASE 
            WHEN pregnancy_bp_stage IN ('gestational_htn_stage1', 'severe_htn_preeclampsia_risk') THEN TRUE
            WHEN preeclampsia_risk_score >= 3 THEN TRUE
            ELSE FALSE
        END AS should_ask_swelling,
        
        -- Should ask about decreased urine output
        CASE 
            WHEN preeclampsia_bp_warning THEN TRUE
            ELSE FALSE
        END AS should_ask_urine_output,
        
        -- ===== GESTATIONAL DIABETES INQUIRY FLAGS =====
        
        -- Should ask about GDM symptoms
        CASE 
            WHEN gestational_diabetes_risk THEN TRUE
            WHEN BLOOD_SUGAR >= 140 THEN TRUE
            ELSE FALSE
        END AS should_ask_gdm_symptoms,
        
        -- Should ask about excessive thirst
        CASE 
            WHEN BLOOD_SUGAR >= 180 THEN TRUE
            WHEN gestational_diabetes_risk_score >= 4 THEN TRUE
            ELSE FALSE
        END AS should_ask_excessive_thirst,
        
        -- Should ask about frequent urination
        CASE 
            WHEN BLOOD_SUGAR >= 180 THEN TRUE
            WHEN gestational_diabetes_risk_score >= 4 THEN TRUE
            ELSE FALSE
        END AS should_ask_frequent_urination,
        
        -- Should ask about blurred vision (diabetes)
        CASE 
            WHEN BLOOD_SUGAR >= 200 THEN TRUE
            ELSE FALSE
        END AS should_ask_blurred_vision_diabetes,
        
        -- Should ask about diet/nutrition
        CASE 
            WHEN gestational_diabetes_risk THEN TRUE
            WHEN BLOOD_SUGAR >= 140 THEN TRUE
            ELSE FALSE
        END AS should_ask_diet_nutrition,
        
        -- ===== INFECTION & FEVER INQUIRY =====
        
        -- Should ask about infection symptoms
        CASE 
            WHEN has_fever THEN TRUE
            WHEN BODY_TEMPERATURE >= 100.4 THEN TRUE
            ELSE FALSE
        END AS should_ask_infection_symptoms,
        
        -- Should ask about urinary symptoms
        CASE 
            WHEN has_fever THEN TRUE
            ELSE FALSE
        END AS should_ask_urinary_symptoms,
        
        -- Should ask about vaginal discharge/symptoms
        CASE 
            WHEN has_fever THEN TRUE
            WHEN BODY_TEMPERATURE >= 100.4 THEN TRUE
            ELSE FALSE
        END AS should_ask_vaginal_symptoms,
        
        -- Should ask about recent exposures
        CASE 
            WHEN BODY_TEMPERATURE >= 101.0 THEN TRUE
            ELSE FALSE
        END AS should_ask_exposure_history,
        
        -- ===== CARDIOVASCULAR INQUIRY =====
        
        -- Should ask about chest pain/discomfort
        CASE 
            WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN TRUE
            WHEN has_tachycardia THEN TRUE
            WHEN HEART_RATE > 120 THEN TRUE
            ELSE FALSE
        END AS should_ask_chest_symptoms,
        
        -- Should ask about palpitations
        CASE 
            WHEN HEART_RATE > 100 THEN TRUE
            WHEN has_tachycardia THEN TRUE
            ELSE FALSE
        END AS should_ask_palpitations,
        
        -- Should ask about shortness of breath
        CASE 
            WHEN HEART_RATE > 110 THEN TRUE
            WHEN SYSTOLIC_BP >= 160 THEN TRUE
            ELSE FALSE
        END AS should_ask_shortness_breath,
        
        -- Should ask about dizziness/fainting
        CASE 
            WHEN SYSTOLIC_BP < 90 OR DIASTOLIC_BP < 60 THEN TRUE
            WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN TRUE
            ELSE FALSE
        END AS should_ask_dizziness,
        
        -- ===== FETAL WELLBEING INQUIRY =====
        
        -- Should ask about fetal movement
        CASE 
            WHEN maternal_urgency_level IN ('emergency', 'urgent') THEN TRUE
            WHEN preeclampsia_bp_warning THEN TRUE
            WHEN gestational_diabetes_risk AND BLOOD_SUGAR >= 200 THEN TRUE
            ELSE FALSE
        END AS should_ask_fetal_movement,
        
        -- Should ask about contractions
        CASE 
            WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN TRUE
            WHEN has_fever THEN TRUE
            ELSE FALSE
        END AS should_ask_contractions,
        
        -- Should ask about vaginal bleeding
        CASE 
            WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN TRUE
            WHEN abnormal_vitals_count >= 2 THEN TRUE
            ELSE FALSE
        END AS should_ask_vaginal_bleeding,
        
        -- Should ask about fluid leakage
        CASE 
            WHEN has_fever THEN TRUE
            WHEN abnormal_vitals_count >= 2 THEN TRUE
            ELSE FALSE
        END AS should_ask_fluid_leakage,
        
        -- ===== PREGNANCY HISTORY INQUIRY =====
        
        -- Should ask about gestational age
        CASE 
            WHEN maternal_urgency_level IN ('emergency', 'urgent') THEN TRUE
            WHEN preeclampsia_bp_warning THEN TRUE
            ELSE FALSE
        END AS should_ask_gestational_age,
        
        -- Should ask about previous pregnancy complications
        CASE 
            WHEN preeclampsia_risk_score >= 3 THEN TRUE
            WHEN gestational_diabetes_risk THEN TRUE
            WHEN has_age_related_risk THEN TRUE
            ELSE FALSE
        END AS should_ask_previous_complications,
        
        -- Should ask about current pregnancy complications
        CASE 
            WHEN abnormal_vitals_count >= 1 THEN TRUE
            ELSE FALSE
        END AS should_ask_current_complications,
        
        -- Should ask about multiple pregnancy
        CASE 
            WHEN preeclampsia_bp_warning THEN TRUE
            WHEN gestational_diabetes_risk THEN TRUE
            ELSE FALSE
        END AS should_ask_multiple_pregnancy,
        
        -- ===== MEDICATION & TREATMENT INQUIRY =====
        
        -- Should ask about current medications
        CASE 
            WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN TRUE
            WHEN BLOOD_SUGAR >= 140 THEN TRUE
            WHEN abnormal_vitals_count >= 2 THEN TRUE
            ELSE FALSE
        END AS should_ask_current_medications,
        
        -- Should ask about prenatal vitamins
        CASE 
            WHEN gestational_diabetes_risk THEN TRUE
            WHEN has_age_related_risk THEN TRUE
            ELSE FALSE
        END AS should_ask_prenatal_vitamins,
        
        -- Should ask about medication adherence
        CASE 
            WHEN pregnancy_bp_stage IN ('gestational_htn_stage1', 'severe_htn_preeclampsia_risk') THEN TRUE
            WHEN BLOOD_SUGAR >= 200 THEN TRUE
            ELSE FALSE
        END AS should_ask_medication_adherence,
        
        -- Should ask about herbal/supplements
        CASE 
            WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN TRUE
            WHEN gestational_diabetes_risk THEN TRUE
            ELSE FALSE
        END AS should_ask_supplements,
        
        -- ===== LIFESTYLE & SELF-CARE INQUIRY =====
        
        -- Should ask about physical activity
        CASE 
            WHEN gestational_diabetes_risk THEN TRUE
            WHEN pregnancy_bp_stage = 'elevated_monitor' THEN TRUE
            ELSE FALSE
        END AS should_ask_physical_activity,
        
        -- Should ask about stress levels
        CASE 
            WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN TRUE
            WHEN has_tachycardia THEN TRUE
            ELSE FALSE
        END AS should_ask_stress_levels,
        
        -- Should ask about sleep quality
        CASE 
            WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN TRUE
            ELSE FALSE
        END AS should_ask_sleep_quality,
        
        -- Should ask about work/activity level
        CASE 
            WHEN preeclampsia_bp_warning THEN TRUE
            WHEN abnormal_vitals_count >= 2 THEN TRUE
            ELSE FALSE
        END AS should_ask_work_activity,
        
        -- ===== MONITORING & FOLLOW-UP FLAGS =====
        
        -- Should ask about home BP monitoring
        CASE 
            WHEN SYSTOLIC_BP >= 120 OR DIASTOLIC_BP >= 80 THEN TRUE
            ELSE FALSE
        END AS should_ask_home_bp_monitoring,
        
        -- Should ask about glucose monitoring
        CASE 
            WHEN BLOOD_SUGAR >= 140 THEN TRUE
            WHEN gestational_diabetes_risk THEN TRUE
            ELSE FALSE
        END AS should_ask_glucose_monitoring,
        
        -- Should ask about recent prenatal visits
        CASE 
            WHEN abnormal_vitals_count >= 1 THEN TRUE
            ELSE FALSE
        END AS should_ask_prenatal_visits,
        
        -- Should ask about specialist care
        CASE 
            WHEN preeclampsia_risk_score >= 5 THEN TRUE
            WHEN gestational_diabetes_risk_score >= 4 THEN TRUE
            WHEN has_age_related_risk THEN TRUE
            ELSE FALSE
        END AS should_ask_specialist_care,
        
        -- ===== REFERRAL & EMERGENCY FLAGS =====
        
        -- Needs immediate OB evaluation
        CASE 
            WHEN maternal_urgency_level = 'emergency' THEN TRUE
            WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN TRUE
            WHEN BODY_TEMPERATURE >= 103.0 THEN TRUE
            WHEN BLOOD_SUGAR >= 250 THEN TRUE
            ELSE FALSE
        END AS needs_immediate_ob_evaluation,
        
        -- Needs urgent OB appointment
        CASE 
            WHEN maternal_urgency_level = 'urgent' THEN TRUE
            WHEN pregnancy_bp_stage = 'gestational_htn_stage1' THEN TRUE
            WHEN BLOOD_SUGAR BETWEEN 200 AND 249 THEN TRUE
            WHEN abnormal_vitals_count >= 3 THEN TRUE
            ELSE FALSE
        END AS needs_urgent_ob_appointment,
        
        -- Needs maternal-fetal medicine referral
        CASE 
            WHEN preeclampsia_risk_score >= 7 THEN TRUE
            WHEN gestational_diabetes_risk_score >= 6 THEN TRUE
            WHEN has_age_related_risk AND abnormal_vitals_count >= 2 THEN TRUE
            ELSE FALSE
        END AS needs_mfm_referral,
        
        -- Needs hospitalization
        CASE 
            WHEN hospitalization_recommendation = 'immediate_hospitalization' THEN TRUE
            WHEN severe_preeclampsia_bp_warning THEN TRUE
            WHEN BODY_TEMPERATURE >= 103.0 THEN TRUE
            ELSE FALSE
        END AS needs_hospitalization_flag,
        
        -- Needs emergency department
        CASE 
            WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN TRUE
            WHEN BODY_TEMPERATURE >= 103.0 THEN TRUE
            WHEN BLOOD_SUGAR >= 250 THEN TRUE
            WHEN has_critical_vitals THEN TRUE
            ELSE FALSE
        END AS needs_emergency_department,
        
        -- ===== EDUCATION PRIORITY TOPICS =====
        
        -- Priority education topics
        -- Priority education topics
        COALESCE(
            NULLIF(CONCAT_WS(', ',
                CASE WHEN preeclampsia_bp_warning THEN 'preeclampsia_warning_signs' END,
                CASE WHEN gestational_diabetes_risk THEN 'gestational_diabetes_management' END,
                CASE WHEN pregnancy_bp_stage = 'elevated_monitor' THEN 'blood_pressure_monitoring' END,
                CASE WHEN has_age_related_risk THEN 'high_risk_pregnancy_care' END,
                CASE WHEN BLOOD_SUGAR >= 140 THEN 'glucose_control_diet' END,
                CASE WHEN has_tachycardia THEN 'cardiovascular_health_pregnancy' END,
                CASE WHEN abnormal_vitals_count >= 2 THEN 'when_to_call_doctor' END,
                CASE WHEN preeclampsia_risk_score >= 5 THEN 'delivery_planning' END
            ), ''),
            'routine_prenatal_education'
        ) AS priority_education_topics,
        
        -- Self-monitoring focus areas
        COALESCE(
            NULLIF(CONCAT_WS(', ',
                CASE WHEN SYSTOLIC_BP >= 120 OR DIASTOLIC_BP >= 80 THEN 'home_blood_pressure' END,
                CASE WHEN gestational_diabetes_risk THEN 'blood_glucose_testing' END,
                CASE WHEN preeclampsia_bp_warning THEN 'daily_symptom_check' END,
                CASE WHEN maternal_urgency_level IN ('urgent', 'needs_attention') THEN 'fetal_movement_counting' END,
                CASE WHEN BLOOD_SUGAR >= 140 THEN 'carbohydrate_counting' END
            ), ''),
            'routine_prenatal_monitoring'
        ) AS selfmonitoring_focus_areas,
        
        -- Lifestyle modification priorities
        COALESCE(
            NULLIF(CONCAT_WS(', ',
                CASE WHEN pregnancy_bp_stage != 'normal' THEN 'sodium_restriction' END,
                CASE WHEN gestational_diabetes_risk THEN 'dietary_modifications' END,
                CASE WHEN SYSTOLIC_BP >= 140 OR DIASTOLIC_BP >= 90 THEN 'activity_modification' END,
                CASE WHEN has_tachycardia THEN 'stress_reduction' END,
                CASE WHEN BLOOD_SUGAR >= 140 THEN 'meal_planning' END,
                CASE WHEN abnormal_vitals_count >= 2 THEN 'adequate_rest' END
            ), ''),
            'healthy_pregnancy_lifestyle'
        ) AS lifestyle_modification_priorities,
        -- ===== URGENCY COMMUNICATION FLAGS =====
        
        -- Communication tone
        CASE 
            WHEN maternal_urgency_level = 'emergency' THEN 'urgent_directive_immediate_action'
            WHEN maternal_urgency_level = 'urgent' THEN 'serious_concerned_prompt_action'
            WHEN maternal_urgency_level = 'needs_attention' THEN 'informative_supportive_followup'
            ELSE 'reassuring_educational'
        END AS communication_tone,
        
        -- Follow-up timeframe
        CASE 
            WHEN maternal_urgency_level = 'emergency' THEN 'immediate_now'
            WHEN maternal_urgency_level = 'urgent' THEN 'within_24_hours'
            WHEN maternal_urgency_level = 'needs_attention' THEN 'within_1_week'
            ELSE 'routine_prenatal_schedule'
        END AS recommended_followup_timeframe,
        
        -- ===== CARE COORDINATION FLAGS =====
        
        -- Needs multidisciplinary care
        CASE 
            WHEN preeclampsia_risk_score >= 5 AND gestational_diabetes_risk_score >= 4 THEN TRUE
            WHEN maternal_complication_risk_score >= 10 THEN TRUE
            ELSE FALSE
        END AS needs_multidisciplinary_care,
        
        -- Monitoring intensity needed
        CASE 
            WHEN maternal_urgency_level = 'emergency' THEN 'continuous_inpatient'
            WHEN preeclampsia_risk_score >= 7 THEN 'intensive_outpatient'
            WHEN maternal_urgency_level = 'urgent' THEN 'frequent_monitoring'
            WHEN abnormal_vitals_count >= 1 THEN 'increased_monitoring'
            ELSE 'routine_prenatal'
        END AS monitoring_intensity_needed,
        
        -- ===== CONVERSATION FLOW GUIDANCE =====
        
        -- Start conversation with
        CASE 
            WHEN maternal_urgency_level = 'emergency' THEN 'emergency_assessment'
            WHEN severe_preeclampsia_bp_warning THEN 'preeclampsia_screening'
            WHEN has_fever THEN 'infection_assessment'
            WHEN preeclampsia_bp_warning OR gestational_diabetes_risk THEN 'risk_assessment'
            ELSE 'routine_check_in'
        END AS conversation_starting_point,
        
        -- Primary clinical concern
        CASE 
            WHEN SYSTOLIC_BP >= 160 OR DIASTOLIC_BP >= 110 THEN 'severe_hypertension_preeclampsia'
            WHEN BODY_TEMPERATURE >= 103.0 THEN 'maternal_fever_sepsis_risk'
            WHEN BLOOD_SUGAR >= 250 THEN 'severe_hyperglycemia'
            WHEN preeclampsia_risk_score >= 7 THEN 'preeclampsia_high_risk'
            WHEN gestational_diabetes_risk_score >= 6 THEN 'gestational_diabetes_management'
            WHEN pregnancy_bp_stage = 'gestational_htn_stage1' THEN 'gestational_hypertension'
            WHEN abnormal_vitals_count >= 3 THEN 'multiple_abnormalities'
            WHEN has_age_related_risk THEN 'advanced_maternal_age_monitoring'
            ELSE 'routine_prenatal_care'
        END AS primary_clinical_concern,
        
        -- Risk stratification for care planning
        CASE 
            WHEN maternal_complication_risk_score >= 10 THEN 'very_high_risk'
            WHEN maternal_complication_risk_score BETWEEN 7 AND 9 THEN 'high_risk'
            WHEN maternal_complication_risk_score BETWEEN 4 AND 6 THEN 'moderate_risk'
            WHEN maternal_complication_risk_score BETWEEN 1 AND 3 THEN 'low_moderate_risk'
            ELSE 'low_risk'
        END AS overall_risk_stratification

    FROM risk_features
)

SELECT * FROM conversation_features