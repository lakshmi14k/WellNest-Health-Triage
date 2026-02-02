-- models/gold/features/womens_wellness_llm/ftr_pcos_conversation_prompts.sql

{{
    config(
        materialized='table',
        tags=['features', 'womens_wellness', 'pcos', 'tier3']
    )
}}

WITH risk_features AS (
    SELECT * FROM {{ ref('ftr_pcos_risk_urgency') }}
),

conversation_features AS (
    SELECT
        *,
        
        -- ===== MENSTRUAL & REPRODUCTIVE SYMPTOM INQUIRY =====
        
        -- Should ask about menstrual cycle details
        CASE 
            WHEN has_irregular_cycles THEN TRUE
            WHEN rotterdam_criteria_count >= 1 THEN TRUE
            ELSE FALSE
        END AS should_ask_menstrual_details,
        
        -- Should ask about cycle length
        CASE 
            WHEN has_irregular_cycles THEN TRUE
            ELSE FALSE
        END AS should_ask_cycle_length,
        
        -- Should ask about heavy periods
        CASE 
            WHEN has_irregular_cycles THEN TRUE
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            ELSE FALSE
        END AS should_ask_heavy_bleeding,
        
        -- Should ask about missed periods
        CASE 
            WHEN has_irregular_cycles THEN TRUE
            ELSE FALSE
        END AS should_ask_missed_periods,
        
        -- Should ask about fertility concerns
        CASE 
            WHEN infertility_risk_score >= 5 THEN TRUE
            WHEN has_irregular_cycles AND in_peak_pcos_age THEN TRUE
            ELSE FALSE
        END AS should_ask_fertility_concerns,
        
        -- Should ask about ovulation symptoms
        CASE 
            WHEN has_irregular_cycles THEN TRUE
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            ELSE FALSE
        END AS should_ask_ovulation_symptoms,
        
        -- ===== HYPERANDROGENISM SYMPTOM INQUIRY =====
        
        -- Should ask about hirsutism (excess hair growth)
        CASE 
            WHEN has_hyperandrogenism THEN TRUE
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            ELSE FALSE
        END AS should_ask_hirsutism,
        
        -- Should ask about acne
        CASE 
            WHEN has_hyperandrogenism THEN TRUE
            WHEN TESTOSTERONE_LEVEL > 70 THEN TRUE
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            ELSE FALSE
        END AS should_ask_acne,
        
        -- Should ask about hair loss/thinning
        CASE 
            WHEN has_hyperandrogenism THEN TRUE
            WHEN TESTOSTERONE_LEVEL > 100 THEN TRUE
            ELSE FALSE
        END AS should_ask_hair_loss,
        
        -- Should ask about voice changes
        CASE 
            WHEN TESTOSTERONE_LEVEL > 150 THEN TRUE
            WHEN hyperandrogenism_severity = 'severe_hyperandrogenism' THEN TRUE
            ELSE FALSE
        END AS should_ask_voice_changes,
        
        -- Should ask about skin changes
        CASE 
            WHEN has_hyperandrogenism THEN TRUE
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            ELSE FALSE
        END AS should_ask_skin_changes,
        
        -- ===== METABOLIC SYMPTOM INQUIRY =====
        
        -- Should ask about weight changes
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            WHEN is_overweight_or_obese THEN TRUE
            ELSE FALSE
        END AS should_ask_weight_changes,
        
        -- Should ask about difficulty losing weight
        CASE 
            WHEN meets_pcos_rotterdam_criteria AND is_obese THEN TRUE
            WHEN pcos_severity_score >= 8 THEN TRUE
            ELSE FALSE
        END AS should_ask_weight_loss_difficulty,
        
        -- Should ask about fatigue
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            WHEN metabolic_syndrome_risk_score >= 5 THEN TRUE
            ELSE FALSE
        END AS should_ask_fatigue,
        
        -- Should ask about darkened skin patches (acanthosis nigricans)
        CASE 
            WHEN is_obese AND meets_pcos_rotterdam_criteria THEN TRUE
            WHEN metabolic_syndrome_risk_score >= 6 THEN TRUE
            ELSE FALSE
        END AS should_ask_skin_darkening,
        
        -- Should ask about cravings/appetite
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            WHEN is_overweight_or_obese THEN TRUE
            ELSE FALSE
        END AS should_ask_cravings,
        
        -- ===== METABOLIC HEALTH SCREENING =====
        
        -- Should ask about diabetes symptoms
        CASE 
            WHEN type2_diabetes_risk IN ('high_risk', 'very_high_risk') THEN TRUE
            WHEN metabolic_syndrome_risk_score >= 7 THEN TRUE
            ELSE FALSE
        END AS should_ask_diabetes_symptoms,
        
        -- Should ask about blood sugar testing history
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            WHEN is_obese THEN TRUE
            ELSE FALSE
        END AS should_ask_glucose_testing_history,
        
        -- Should ask about cholesterol/lipid testing
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            WHEN cardiovascular_risk_category IN ('high_risk', 'very_high_risk') THEN TRUE
            ELSE FALSE
        END AS should_ask_lipid_testing,
        
        -- Should ask about blood pressure
        CASE 
            WHEN meets_pcos_rotterdam_criteria AND is_obese THEN TRUE
            WHEN cardiovascular_risk_category = 'very_high_risk' THEN TRUE
            ELSE FALSE
        END AS should_ask_blood_pressure,
        
        -- ===== EMOTIONAL & PSYCHOLOGICAL INQUIRY =====
        
        -- Should ask about mood/depression
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            WHEN pcos_severity_score >= 8 THEN TRUE
            ELSE FALSE
        END AS should_ask_mood_depression,
        
        -- Should ask about anxiety
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            WHEN has_irregular_cycles THEN TRUE
            ELSE FALSE
        END AS should_ask_anxiety,
        
        -- Should ask about body image concerns
        CASE 
            WHEN has_hyperandrogenism THEN TRUE
            WHEN is_obese AND meets_pcos_rotterdam_criteria THEN TRUE
            ELSE FALSE
        END AS should_ask_body_image,
        
        -- Should ask about self-esteem
        CASE 
            WHEN pcos_severity_score >= 10 THEN TRUE
            WHEN hyperandrogenism_severity IN ('moderate_hyperandrogenism', 'severe_hyperandrogenism') THEN TRUE
            ELSE FALSE
        END AS should_ask_selfesteem,
        
        -- ===== FAMILY HISTORY & GENETICS =====
        
        -- Should ask about family PCOS history
        CASE 
            WHEN rotterdam_criteria_count >= 1 THEN TRUE
            ELSE FALSE
        END AS should_ask_family_pcos_history,
        
        -- Should ask about family diabetes history
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            WHEN type2_diabetes_risk IN ('high_risk', 'very_high_risk') THEN TRUE
            ELSE FALSE
        END AS should_ask_family_diabetes_history,
        
        -- Should ask about family cardiovascular history
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            ELSE FALSE
        END AS should_ask_family_cardiovascular_history,
        
        -- ===== LIFESTYLE & DIET INQUIRY =====
        
        -- Should ask about diet habits
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            WHEN is_overweight_or_obese THEN TRUE
            WHEN metabolic_syndrome_risk_score >= 4 THEN TRUE
            ELSE FALSE
        END AS should_ask_diet_habits,
        
        -- Should ask about physical activity
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            WHEN is_overweight_or_obese THEN TRUE
            ELSE FALSE
        END AS should_ask_physical_activity,
        
        -- Should ask about sleep quality
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            WHEN is_obese THEN TRUE
            ELSE FALSE
        END AS should_ask_sleep_quality,
        
        -- Should ask about stress levels
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            ELSE FALSE
        END AS should_ask_stress_levels,
        
        -- ===== TREATMENT HISTORY INQUIRY =====
        
        -- Should ask about current medications
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            WHEN rotterdam_criteria_count >= 1 THEN TRUE
            ELSE FALSE
        END AS should_ask_current_medications,
        
        -- Should ask about birth control use
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            WHEN has_irregular_cycles THEN TRUE
            ELSE FALSE
        END AS should_ask_birth_control,
        
        -- Should ask about fertility treatments
        CASE 
            WHEN infertility_risk_score >= 5 THEN TRUE
            WHEN has_irregular_cycles AND AGE >= 30 THEN TRUE
            ELSE FALSE
        END AS should_ask_fertility_treatments,
        
        -- Should ask about supplements
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            ELSE FALSE
        END AS should_ask_supplements,
        
        -- Should ask about previous diagnoses
        CASE 
            WHEN rotterdam_criteria_count >= 1 THEN TRUE
            ELSE FALSE
        END AS should_ask_previous_diagnoses,
        
        -- ===== SCREENING & TESTING FLAGS =====
        
        -- Needs comprehensive hormonal panel
        CASE 
            WHEN rotterdam_criteria_count >= 1 AND NOT meets_pcos_rotterdam_criteria THEN TRUE
            ELSE FALSE
        END AS needs_hormonal_panel_screening,
        
        -- Needs pelvic ultrasound
        CASE 
            WHEN rotterdam_criteria_count >= 1 THEN TRUE
            WHEN AMH_LEVEL > 7.0 THEN TRUE
            ELSE FALSE
        END AS needs_pelvic_ultrasound,
        
        -- Needs metabolic screening
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            WHEN metabolic_syndrome_risk_score >= 5 THEN TRUE
            ELSE FALSE
        END AS needs_metabolic_screening,
        
        -- Needs glucose tolerance test
        CASE 
            WHEN type2_diabetes_risk IN ('high_risk', 'very_high_risk') THEN TRUE
            WHEN meets_pcos_rotterdam_criteria AND is_obese THEN TRUE
            ELSE FALSE
        END AS needs_glucose_tolerance_test,
        
        -- Needs lipid panel
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            WHEN cardiovascular_risk_category IN ('high_risk', 'very_high_risk') THEN TRUE
            ELSE FALSE
        END AS needs_lipid_panel,
        
        -- ===== REFERRAL FLAGS =====
        
        -- Needs endocrinologist referral
        CASE 
            WHEN pcos_severity_score >= 10 THEN TRUE
            WHEN TESTOSTERONE_LEVEL > 200 THEN TRUE
            WHEN meets_pcos_rotterdam_criteria AND metabolic_syndrome_risk_score >= 7 THEN TRUE
            ELSE FALSE
        END AS needs_endocrinologist_referral,
        
        -- Needs reproductive endocrinologist referral
        CASE 
            WHEN infertility_risk_score >= 8 THEN TRUE
            WHEN has_irregular_cycles AND AGE >= 35 THEN TRUE
            ELSE FALSE
        END AS needs_reproductive_endo_referral,
        
        -- Needs gynecologist referral
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN TRUE
            WHEN has_irregular_cycles THEN TRUE
            ELSE FALSE
        END AS needs_gynecologist_referral,
        
        -- Needs nutritionist/dietitian referral
        CASE 
            WHEN meets_pcos_rotterdam_criteria AND is_obese THEN TRUE
            WHEN weight_management_priority IN ('critical_priority', 'high_priority') THEN TRUE
            ELSE FALSE
        END AS needs_nutritionist_referral,
        
        -- Needs mental health referral
        CASE 
            WHEN pcos_severity_score >= 12 THEN TRUE
            WHEN hyperandrogenism_severity = 'severe_hyperandrogenism' THEN TRUE
            ELSE FALSE
        END AS needs_mental_health_referral,
        
        -- ===== EDUCATION PRIORITY TOPICS =====
        
        -- Priority education topics
        COALESCE(
            NULLIF(CONCAT_WS(', ',
                CASE WHEN meets_pcos_rotterdam_criteria THEN 'pcos_overview_education' END,
                CASE WHEN has_irregular_cycles THEN 'menstrual_cycle_regulation' END,
                CASE WHEN has_hyperandrogenism THEN 'hyperandrogenism_management' END,
                CASE WHEN is_overweight_or_obese THEN 'weight_management_pcos' END,
                CASE WHEN type2_diabetes_risk IN ('high_risk', 'very_high_risk') THEN 'diabetes_prevention' END,
                CASE WHEN infertility_risk_score >= 5 THEN 'fertility_in_pcos' END,
                CASE WHEN cardiovascular_risk_category IN ('high_risk', 'very_high_risk') THEN 'cardiovascular_health' END,
                CASE WHEN metabolic_syndrome_risk_score >= 5 THEN 'metabolic_syndrome_prevention' END
            ), ''),
            'hormonal_health_education'
        ) AS priority_education_topics,
        
        -- Lifestyle modification focus
        COALESCE(
            NULLIF(CONCAT_WS(', ',
                CASE WHEN is_overweight_or_obese THEN 'nutrition_optimization' END,
                CASE WHEN meets_pcos_rotterdam_criteria THEN 'regular_exercise' END,
                CASE WHEN is_obese THEN 'sustainable_weight_loss' END,
                CASE WHEN metabolic_syndrome_risk_score >= 5 THEN 'insulin_resistance_management' END,
                CASE WHEN meets_pcos_rotterdam_criteria THEN 'stress_management' END,
                CASE WHEN is_obese THEN 'sleep_hygiene' END
            ), ''),
            'healthy_lifestyle_maintenance'
        ) AS lifestyle_modification_focus,
        
        -- Treatment modality recommendations
        COALESCE(
            NULLIF(CONCAT_WS(', ',
                CASE WHEN has_irregular_cycles AND meets_pcos_rotterdam_criteria THEN 'hormonal_contraceptives' END,
                CASE WHEN is_obese AND meets_pcos_rotterdam_criteria THEN 'metformin_consideration' END,
                CASE WHEN hyperandrogenism_severity IN ('moderate_hyperandrogenism', 'severe_hyperandrogenism') THEN 'antiandrogen_therapy' END,
                CASE WHEN infertility_risk_score >= 8 THEN 'fertility_medications' END,
                CASE WHEN weight_management_priority = 'critical_priority' THEN 'medical_weight_management' END
            ), ''),
            'lifestyle_first_approach'
        ) AS recommended_treatment_modalities,
        
        -- ===== URGENCY COMMUNICATION FLAGS =====
        
        -- Communication tone
        CASE 
            WHEN pcos_urgency_level = 'needs_prompt_evaluation' THEN 'concerned_supportive_timely'
            WHEN pcos_urgency_level = 'needs_specialist_evaluation' THEN 'informative_encouraging_referral'
            WHEN pcos_urgency_level = 'needs_evaluation' THEN 'educational_supportive'
            ELSE 'reassuring_informative'
        END AS communication_tone,
        
        -- Follow-up timeframe
        CASE 
            WHEN pcos_urgency_level = 'needs_prompt_evaluation' THEN 'within_2_weeks'
            WHEN pcos_urgency_level = 'needs_specialist_evaluation' THEN 'within_1_month'
            WHEN pcos_urgency_level = 'needs_evaluation' THEN 'within_3_months'
            ELSE 'routine_annual'
        END AS recommended_followup_timeframe,
        
        -- ===== CARE COORDINATION FLAGS =====
        
        -- Needs multidisciplinary care
        CASE 
            WHEN meets_pcos_rotterdam_criteria AND is_obese AND metabolic_syndrome_risk_score >= 7 THEN TRUE
            WHEN pcos_severity_score >= 12 THEN TRUE
            ELSE FALSE
        END AS needs_multidisciplinary_care,
        
        -- Monitoring intensity needed
        CASE 
            WHEN pcos_severity_score >= 12 THEN 'intensive_monitoring'
            WHEN meets_pcos_rotterdam_criteria AND type2_diabetes_risk = 'very_high_risk' THEN 'frequent_monitoring'
            WHEN meets_pcos_rotterdam_criteria THEN 'regular_monitoring'
            WHEN rotterdam_criteria_count >= 1 THEN 'periodic_monitoring'
            ELSE 'routine_screening'
        END AS monitoring_intensity_needed,
        
        -- ===== CONVERSATION FLOW GUIDANCE =====
        
        -- Start conversation with
        CASE 
            WHEN meets_pcos_rotterdam_criteria THEN 'pcos_diagnosis_discussion'
            WHEN rotterdam_criteria_count >= 1 THEN 'symptom_assessment'
            WHEN has_pcos_family_history THEN 'risk_screening'
            ELSE 'hormonal_health_checkup'
        END AS conversation_starting_point,
        
        -- Primary clinical concern
        CASE 
            WHEN TESTOSTERONE_LEVEL > 200 THEN 'severe_hyperandrogenism'
            WHEN pcos_severity_score >= 12 THEN 'severe_pcos_symptoms'
            WHEN meets_pcos_rotterdam_criteria AND infertility_risk_score >= 8 THEN 'pcos_with_fertility_concerns'
            WHEN meets_pcos_rotterdam_criteria AND type2_diabetes_risk = 'very_high_risk' THEN 'pcos_metabolic_complications'
            WHEN meets_pcos_rotterdam_criteria THEN 'confirmed_pcos_management'
            WHEN rotterdam_criteria_count = 1 THEN 'possible_pcos_screening'
            WHEN has_irregular_cycles THEN 'menstrual_irregularity'
            WHEN has_hyperandrogenism THEN 'hyperandrogenic_symptoms'
            ELSE 'hormonal_wellness_checkup'
        END AS primary_clinical_concern,
        
        -- Overall risk stratification
        CASE 
            WHEN pcos_severity_score >= 12 THEN 'very_high_complexity'
            WHEN pcos_severity_score >= 8 THEN 'high_complexity'
            WHEN meets_pcos_rotterdam_criteria THEN 'moderate_complexity'
            WHEN rotterdam_criteria_count >= 1 THEN 'low_moderate_complexity'
            ELSE 'low_complexity'
        END AS overall_complexity_level

    FROM risk_features
)

SELECT * FROM conversation_features