-- models/gold/features/lifestyle_diseases/ftr_hypertension_conversation_prompts.sql

{{
    config(
        materialized='table',
        tags=['features', 'lifestyle_diseases', 'hypertension', 'tier3']
    )
}}

WITH risk_features AS (
    SELECT * FROM {{ ref('ftr_hypertension_risk_urgency') }}
),

conversation_features AS (
    SELECT
        *,
        
        -- ===== SYMPTOM INQUIRY FLAGS =====
        
        -- Should ask about hypertensive emergency symptoms
        CASE 
            WHEN bp_urgency_level = 'hypertensive_crisis_emergency' THEN TRUE 
            ELSE FALSE 
        END AS should_ask_crisis_symptoms,
        
        -- Should ask about cardiovascular symptoms
        CASE 
            WHEN bp_stage IN ('stage2_hypertension', 'hypertensive_crisis') THEN TRUE
            WHEN cardiovascular_risk_score >= 8 THEN TRUE
            ELSE FALSE
        END AS should_ask_cardiovascular_symptoms,
        
        -- Should ask about stroke warning signs
        CASE 
            WHEN stroke_risk_factors_count >= 3 THEN TRUE
            WHEN bp_stage = 'hypertensive_crisis' THEN TRUE
            ELSE FALSE
        END AS should_ask_stroke_symptoms,
        
        -- Should ask about kidney symptoms
        CASE 
            WHEN kidney_damage_risk IN ('high_nephropathy_risk', 'moderate_nephropathy_risk') THEN TRUE 
            ELSE FALSE 
        END AS should_ask_kidney_symptoms,
        
        -- Should ask about headaches/vision changes
        CASE 
            WHEN bp_stage IN ('stage2_hypertension', 'hypertensive_crisis') THEN TRUE 
            ELSE FALSE 
        END AS should_ask_headache_vision,
        
        -- Should ask about chest pain/palpitations
        CASE 
            WHEN cardiac_damage_risk_score >= 5 THEN TRUE
            WHEN bp_stage = 'hypertensive_crisis' THEN TRUE
            ELSE FALSE
        END AS should_ask_chest_symptoms,
        
        -- ===== LIFESTYLE INQUIRY FLAGS =====
        
        -- Should ask about diet/sodium intake
        CASE 
            WHEN bp_stage NOT IN ('normal', 'elevated') THEN TRUE
            WHEN salt_intake_risk IN ('elevated', 'excessive') THEN TRUE
            ELSE FALSE
        END AS should_ask_diet_sodium,
        
        -- Should ask about physical activity
        CASE 
            WHEN is_sedentary THEN TRUE
            WHEN bp_stage NOT IN ('normal') THEN TRUE
            ELSE FALSE
        END AS should_ask_exercise_habits,
        
        -- Should ask about stress/sleep
        CASE 
            WHEN stress_category IN ('high', 'very_high') THEN TRUE
            WHEN sleep_adequacy = 'insufficient' THEN TRUE
            WHEN bp_stage NOT IN ('normal', 'elevated') THEN TRUE
            ELSE FALSE
        END AS should_ask_stress_sleep,
        
        -- Should ask about alcohol consumption
        CASE 
            WHEN alcohol_risk_level IN ('moderate_high', 'excessive') THEN TRUE
            WHEN bp_stage NOT IN ('normal', 'elevated') THEN TRUE
            ELSE FALSE
        END AS should_ask_alcohol_use,
        
        -- Should ask about weight/BMI
        CASE 
            WHEN is_obese THEN TRUE
            WHEN BMI >= 25 AND bp_stage NOT IN ('normal', 'elevated') THEN TRUE
            ELSE FALSE
        END AS should_ask_weight_management,
        
        -- ===== MEDICATION & ADHERENCE FLAGS =====
        
        -- Should ask about current BP medications
        CASE 
            WHEN has_hypertension_diagnosis THEN TRUE
            WHEN bp_stage NOT IN ('normal', 'elevated') THEN TRUE
            ELSE FALSE
        END AS should_ask_current_medications,
        
        -- Should ask about medication adherence
        CASE 
            WHEN has_hypertension_diagnosis AND bp_stage NOT IN ('normal', 'elevated') THEN TRUE 
            ELSE FALSE 
        END AS should_ask_medication_adherence,
        
        -- Should ask about medication side effects
        CASE 
            WHEN has_hypertension_diagnosis THEN TRUE 
            ELSE FALSE 
        END AS should_ask_medication_sideeffects,
        
        -- ===== MONITORING & SCREENING FLAGS =====
        
        -- Needs home BP monitoring discussion
        CASE 
            WHEN bp_stage NOT IN ('normal') THEN TRUE 
            ELSE FALSE 
        END AS needs_home_bp_monitoring,
        
        -- Needs lipid panel discussion
        CASE 
            WHEN cholesterol_category IN ('borderline_high', 'high') THEN TRUE
            WHEN cardiovascular_risk_score >= 7 THEN TRUE
            ELSE FALSE
        END AS needs_lipid_management_discussion,
        
        -- Needs diabetes screening discussion
        CASE 
            WHEN glucose_status IN ('prediabetes', 'diabetes_range') AND NOT HAS_DIABETES THEN TRUE
            WHEN has_metabolic_syndrome AND NOT HAS_DIABETES THEN TRUE
            ELSE FALSE
        END AS needs_diabetes_screening,
        
        -- Needs kidney function testing discussion
        CASE 
            WHEN kidney_damage_risk IN ('high_nephropathy_risk', 'moderate_nephropathy_risk') THEN TRUE 
            ELSE FALSE 
        END AS needs_kidney_function_testing,
        
        -- ===== REFERRAL & SPECIALIST FLAGS =====
        
        -- Needs specialist referral consideration
        CASE 
            WHEN bp_stage = 'hypertensive_crisis' THEN TRUE
            WHEN bp_stage = 'stage2_hypertension' AND cardiovascular_risk_score >= 10 THEN TRUE
            WHEN age_bp_risk_category = 'young_onset_critical' THEN TRUE
            WHEN has_metabolic_syndrome AND bp_stage = 'stage2_hypertension' THEN TRUE
            ELSE FALSE
        END AS needs_specialist_referral_flag,
        
        -- Needs emergency care flag
        CASE 
            WHEN potential_hypertensive_emergency THEN TRUE 
            ELSE FALSE 
        END AS needs_emergency_care_flag,
        
        -- Needs cardiologist referral
        CASE 
            WHEN cardiac_damage_risk_score >= 6 THEN TRUE
            WHEN age_bp_risk_category = 'young_onset_critical' THEN TRUE
            ELSE FALSE
        END AS consider_cardiology_referral,
        
        -- ===== EDUCATION PRIORITY TOPICS =====
        
        -- Priority education topics
        CONCAT_WS(', ',
            CASE WHEN bp_stage NOT IN ('normal') THEN 'blood_pressure_basics' END,
            CASE WHEN dietary_modification_priority IN ('critical', 'high') THEN 'DASH_diet' END,
            CASE WHEN salt_intake_risk IN ('elevated', 'excessive') THEN 'sodium_reduction' END,
            CASE WHEN exercise_priority IN ('high', 'moderate') THEN 'exercise_prescription' END,
            CASE WHEN weight_loss_priority IN ('critical', 'high') THEN 'weight_management' END,
            CASE WHEN is_current_smoker THEN 'smoking_cessation' END,
            CASE WHEN stress_management_priority IN ('high', 'moderate') THEN 'stress_reduction' END,
            CASE WHEN likely_needs_medication THEN 'medication_education' END,
            CASE WHEN has_metabolic_syndrome THEN 'metabolic_syndrome' END,
            CASE WHEN cardiovascular_risk_score >= 8 THEN 'cardiovascular_risk_reduction' END,
            CASE WHEN needs_home_bp_monitoring THEN 'home_bp_monitoring' END
        ) AS priority_education_topics,
        
        -- Lifestyle modification focus areas
        CONCAT_WS(', ',
            CASE WHEN dietary_modification_priority IN ('critical', 'high', 'moderate') THEN 'diet' END,
            CASE WHEN exercise_priority IN ('high', 'moderate') THEN 'exercise' END,
            CASE WHEN weight_loss_priority IN ('critical', 'high', 'moderate') THEN 'weight' END,
            CASE WHEN smoking_cessation_priority_htn IN ('critical', 'high', 'moderate') THEN 'smoking' END,
            CASE WHEN alcohol_reduction_priority IN ('high', 'moderate') THEN 'alcohol' END,
            CASE WHEN stress_management_priority IN ('high', 'moderate') THEN 'stress' END,
            CASE WHEN sleep_adequacy = 'insufficient' THEN 'sleep' END
        ) AS lifestyle_modification_focus,
        
        -- ===== URGENCY COMMUNICATION FLAGS =====
        
        -- Communication tone
        CASE 
            WHEN bp_urgency_level = 'hypertensive_crisis_emergency' THEN 'urgent_immediate_action'
            WHEN bp_urgency_level = 'severe_urgency' THEN 'serious_prompt_action'
            WHEN bp_urgency_level = 'moderate_urgency' THEN 'concerned_timely_action'
            WHEN bp_urgency_level = 'needs_attention' THEN 'informative_preventive'
            ELSE 'supportive_maintenance'
        END AS communication_tone

    FROM risk_features
)

SELECT * FROM conversation_features
