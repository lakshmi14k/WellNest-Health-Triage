-- models/gold/features/mental_health_llm/ftr_mental_health_conversation_prompts.sql

{{
    config(
        materialized='table',
        tags=['features', 'mental_health', 'tier3']
    )
}}

WITH risk_features AS (
    SELECT * FROM {{ ref('ftr_mental_health_risk_urgency') }}
),

conversation_features AS (
    SELECT
        *,
        
        -- ===== SYMPTOM INQUIRY FLAGS =====
        
        -- Should ask about depressive symptoms
        CASE 
            WHEN depression_severity IN ('moderate_depression', 'moderately_severe_depression', 'severe_depression') THEN TRUE
            WHEN depression_risk_score >= 5 THEN TRUE
            ELSE FALSE
        END AS should_ask_depressive_symptoms,
        
        -- Should ask about anxiety symptoms
        CASE 
            WHEN anxiety_severity IN ('moderate_anxiety', 'severe_anxiety') THEN TRUE
            WHEN anxiety_risk_score >= 5 THEN TRUE
            ELSE FALSE
        END AS should_ask_anxiety_symptoms,
        
        -- Should ask about sleep patterns
        CASE 
            WHEN depression_risk_score >= 5 OR anxiety_risk_score >= 5 THEN TRUE
            WHEN LOWER(CHANGES_HABITS) = 'yes' THEN TRUE
            ELSE FALSE
        END AS should_ask_sleep_patterns,
        
        -- Should ask about appetite/eating changes
        CASE 
            WHEN depression_risk_score >= 5 THEN TRUE
            WHEN LOWER(CHANGES_HABITS) = 'yes' THEN TRUE
            ELSE FALSE
        END AS should_ask_appetite_changes,
        
        -- Should ask about concentration/memory
        CASE 
            WHEN LOWER(WORK_INTEREST) IN ('no', 'maybe') THEN TRUE
            WHEN depression_risk_score >= 5 OR anxiety_risk_score >= 5 THEN TRUE
            ELSE FALSE
        END AS should_ask_concentration,
        
        -- Should ask about energy levels
        CASE 
            WHEN LOWER(WORK_INTEREST) = 'no' THEN TRUE
            WHEN depression_risk_score >= 5 THEN TRUE
            WHEN isolation_severity_score >= 2 THEN TRUE
            ELSE FALSE
        END AS should_ask_energy_levels,
        
        -- ===== SAFETY ASSESSMENT FLAGS =====
        
        -- Should screen for suicidal ideation
        CASE 
            WHEN needs_suicide_risk_screening THEN TRUE
            WHEN depression_severity = 'severe_depression' THEN TRUE
            WHEN severe_symptom_count >= 4 THEN TRUE
            ELSE FALSE
        END AS should_screen_suicidal_ideation,
        
        -- Should assess self-harm history
        CASE 
            WHEN depression_severity IN ('moderately_severe_depression', 'severe_depression') THEN TRUE
            WHEN has_mental_health_history AND HAS_COPING_STRUGGLES THEN TRUE
            ELSE FALSE
        END AS should_assess_selfharm_history,
        
        -- Should evaluate safety plan need
        CASE 
            WHEN needs_suicide_risk_screening THEN TRUE
            WHEN depression_risk_score >= 15 THEN TRUE
            ELSE FALSE
        END AS should_evaluate_safety_plan,
        
        -- ===== SOCIAL & RELATIONSHIP INQUIRY =====
        
        -- Should ask about social support
        CASE 
            WHEN social_isolation_level IN ('moderate_isolation', 'severe_isolation') THEN TRUE
            WHEN LOWER(SOCIAL_WEAKNESS) = 'yes' THEN TRUE
            WHEN symptom_count >= 3 THEN TRUE
            ELSE FALSE
        END AS should_ask_social_support,
        
        -- Should ask about relationships
        CASE 
            WHEN LOWER(SOCIAL_WEAKNESS) IN ('yes', 'maybe') THEN TRUE
            WHEN social_isolation_level NOT IN ('no_isolation', 'unknown') THEN TRUE
            ELSE FALSE
        END AS should_ask_relationships,
        
        -- Should ask about family dynamics
        CASE 
            WHEN HAS_FAMILY_HISTORY THEN TRUE
            WHEN age_category IN ('adolescent', 'young_adult') THEN TRUE
            ELSE FALSE
        END AS should_ask_family_dynamics,
        
        -- ===== OCCUPATIONAL & FUNCTIONAL INQUIRY =====
        
        -- Should ask about work/school functioning
        CASE 
            WHEN LOWER(WORK_INTEREST) IN ('no', 'maybe') THEN TRUE
            WHEN functional_impairment_score >= 4 THEN TRUE
            ELSE FALSE
        END AS should_ask_work_functioning,
        
        -- Should ask about daily activities
        CASE 
            WHEN LOWER(CHANGES_HABITS) = 'yes' THEN TRUE
            WHEN functional_impairment_category IN ('moderate_impairment', 'severe_impairment') THEN TRUE
            ELSE FALSE
        END AS should_ask_daily_activities,
        
        -- Should ask about motivation
        CASE 
            WHEN LOWER(WORK_INTEREST) = 'no' THEN TRUE
            WHEN depression_risk_score >= 5 THEN TRUE
            ELSE FALSE
        END AS should_ask_motivation,
        
        -- ===== LIFESTYLE & COPING INQUIRY =====
        
        -- Should ask about exercise/physical activity
        CASE 
            WHEN isolation_severity_score >= 1 THEN TRUE
            WHEN depression_risk_score >= 5 THEN TRUE
            ELSE FALSE
        END AS should_ask_physical_activity,
        
        -- Should ask about substance use
        CASE 
            WHEN HAS_COPING_STRUGGLES THEN TRUE
            WHEN depression_severity IN ('moderate_depression', 'moderately_severe_depression', 'severe_depression') THEN TRUE
            WHEN anxiety_severity IN ('moderate_anxiety', 'severe_anxiety') THEN TRUE
            ELSE FALSE
        END AS should_ask_substance_use,
        
        -- Should ask about coping strategies
        CASE 
            WHEN HAS_COPING_STRUGGLES THEN TRUE
            WHEN stress_level = 'significant_stress' THEN TRUE
            WHEN symptom_count >= 3 THEN TRUE
            ELSE FALSE
        END AS should_ask_coping_strategies,
        
        -- Should ask about hobbies/interests
        CASE 
            WHEN LOWER(WORK_INTEREST) = 'no' THEN TRUE
            WHEN depression_risk_score >= 5 THEN TRUE
            ELSE FALSE
        END AS should_ask_hobbies_interests,
        
        -- ===== TREATMENT HISTORY & CURRENT CARE =====
        
        -- Should ask about past treatment
        CASE 
            WHEN has_mental_health_history THEN TRUE
            WHEN has_treatment_gap THEN TRUE
            ELSE FALSE
        END AS should_ask_past_treatment,
        
        -- Should ask about current medications
        CASE 
            WHEN in_active_treatment THEN TRUE
            WHEN has_mental_health_history THEN TRUE
            ELSE FALSE
        END AS should_ask_current_medications,
        
        -- Should ask about therapy experience
        CASE 
            WHEN in_active_treatment THEN TRUE
            WHEN has_mental_health_history THEN TRUE
            WHEN therapy_priority IN ('high_priority', 'moderate_priority') THEN TRUE
            ELSE FALSE
        END AS should_ask_therapy_experience,
        
        -- Should ask about medication side effects
        CASE 
            WHEN in_active_treatment THEN TRUE
            ELSE FALSE
        END AS should_ask_medication_sideeffects,
        
        -- Should ask about treatment satisfaction
        CASE 
            WHEN in_active_treatment THEN TRUE
            ELSE FALSE
        END AS should_ask_treatment_satisfaction,
        
        -- ===== BARRIERS TO CARE INQUIRY =====
        
        -- Should ask about access to care
        CASE 
            WHEN care_awareness_status IN ('uncertain_about_care', 'unaware_of_options') THEN TRUE
            WHEN has_treatment_gap THEN TRUE
            ELSE FALSE
        END AS should_ask_care_access,
        
        -- Should ask about financial barriers
        CASE 
            WHEN NOT in_active_treatment AND symptom_count >= 3 THEN TRUE
            WHEN care_awareness_status = 'unaware_of_options' THEN TRUE
            ELSE FALSE
        END AS should_ask_financial_barriers,
        
        -- Should ask about stigma concerns
        CASE 
            WHEN perceived_workplace_stigma THEN TRUE
            WHEN mental_health_disclosure_comfort = 'uncomfortable_discussing' THEN TRUE
            WHEN NOT in_active_treatment AND symptom_count >= 3 THEN TRUE
            ELSE FALSE
        END AS should_ask_stigma_concerns,
        
        -- Should ask about time/scheduling barriers
        CASE 
            WHEN NOT in_active_treatment AND occupation_category IN ('professional', 'student') THEN TRUE
            ELSE FALSE
        END AS should_ask_scheduling_barriers,
        
        -- ===== SPECIFIC CONDITION INQUIRY =====
        
        -- Should explore panic symptoms
        CASE 
            WHEN anxiety_severity IN ('moderate_anxiety', 'severe_anxiety') THEN TRUE
            WHEN LOWER(MOOD_SWINGS) = 'high' AND anxiety_risk_score >= 5 THEN TRUE
            ELSE FALSE
        END AS should_explore_panic_symptoms,
        
        -- Should explore social anxiety
        CASE 
            WHEN LOWER(SOCIAL_WEAKNESS) = 'yes' THEN TRUE
            WHEN anxiety_risk_score >= 5 AND social_isolation_level != 'no_isolation' THEN TRUE
            ELSE FALSE
        END AS should_explore_social_anxiety,
        
        -- Should explore trauma history
        CASE 
            WHEN severe_symptom_count >= 3 THEN TRUE
            WHEN has_mental_health_history AND HAS_COPING_STRUGGLES THEN TRUE
            ELSE FALSE
        END AS should_explore_trauma_history,
        
        -- Should explore mood episodes
        CASE 
            WHEN LOWER(MOOD_SWINGS) = 'high' THEN TRUE
            WHEN has_mental_health_history THEN TRUE
            ELSE FALSE
        END AS should_explore_mood_episodes,
        
        -- ===== REFERRAL & RESOURCES FLAGS =====
        
        -- Needs immediate crisis referral
        CASE 
            WHEN mental_health_urgency_level = 'emergency' THEN TRUE
            WHEN needs_suicide_risk_screening AND NOT in_active_treatment THEN TRUE
            ELSE FALSE
        END AS needs_crisis_referral,
        
        -- Needs psychiatrist referral
        CASE 
            WHEN medication_evaluation_priority IN ('psychiatric_evaluation_urgent', 'psychiatric_evaluation_needed') THEN TRUE
            WHEN depression_severity = 'severe_depression' AND NOT in_active_treatment THEN TRUE
            ELSE FALSE
        END AS needs_psychiatrist_referral,
        
        -- Needs therapist/counselor referral
        CASE 
            WHEN therapy_priority IN ('high_priority', 'moderate_priority') AND NOT in_active_treatment THEN TRUE
            WHEN has_treatment_gap THEN TRUE
            ELSE FALSE
        END AS needs_therapist_referral,
        
        -- Needs support group referral
        CASE 
            WHEN social_isolation_level IN ('moderate_isolation', 'severe_isolation') THEN TRUE
            WHEN LOWER(SOCIAL_WEAKNESS) = 'yes' THEN TRUE
            ELSE FALSE
        END AS needs_support_group_referral,
        
        -- Needs workplace accommodation discussion
        CASE 
            WHEN functional_impairment_category IN ('moderate_impairment', 'severe_impairment') THEN TRUE
            WHEN LOWER(WORK_INTEREST) = 'no' AND occupation_category IN ('professional', 'student') THEN TRUE
            ELSE FALSE
        END AS needs_workplace_accommodation,
        
        -- ===== EDUCATION PRIORITY TOPICS =====
        
        -- Priority education topics
        CONCAT_WS(', ',
            CASE WHEN depression_risk_score >= 5 THEN 'depression_psychoeducation' END,
            CASE WHEN anxiety_risk_score >= 5 THEN 'anxiety_management' END,
            CASE WHEN stress_level = 'significant_stress' THEN 'stress_reduction_techniques' END,
            CASE WHEN HAS_COPING_STRUGGLES THEN 'coping_skills_training' END,
            CASE WHEN social_isolation_level != 'no_isolation' THEN 'social_connection_strategies' END,
            CASE WHEN LOWER(WORK_INTEREST) = 'no' THEN 'behavioral_activation' END,
            CASE WHEN functional_impairment_score >= 4 THEN 'daily_functioning_skills' END,
            CASE WHEN perceived_workplace_stigma THEN 'mental_health_stigma_education' END,
            CASE WHEN care_awareness_status = 'unaware_of_options' THEN 'treatment_options_overview' END,
            CASE WHEN NOT in_active_treatment AND symptom_count >= 3 THEN 'when_to_seek_help' END
        ) AS priority_education_topics,
        
        -- Self-care focus areas
        CONCAT_WS(', ',
            CASE WHEN isolation_severity_score >= 1 THEN 'physical_activity' END,
            CASE WHEN depression_risk_score >= 5 THEN 'sleep_hygiene' END,
            CASE WHEN stress_level = 'significant_stress' THEN 'relaxation_techniques' END,
            CASE WHEN social_isolation_level != 'no_isolation' THEN 'social_engagement' END,
            CASE WHEN LOWER(CHANGES_HABITS) = 'yes' THEN 'routine_establishment' END,
            CASE WHEN LOWER(WORK_INTEREST) = 'no' THEN 'pleasurable_activities' END
        ) AS selfcare_focus_areas,
        
        -- Therapy modality recommendations
        CONCAT_WS(', ',
            CASE WHEN depression_risk_score >= 10 THEN 'CBT_for_depression' END,
            CASE WHEN anxiety_risk_score >= 10 THEN 'CBT_for_anxiety' END,
            CASE WHEN social_isolation_level IN ('moderate_isolation', 'severe_isolation') THEN 'interpersonal_therapy' END,
            CASE WHEN HAS_COPING_STRUGGLES THEN 'DBT_skills' END,
            CASE WHEN stress_level = 'significant_stress' THEN 'mindfulness_based_therapy' END,
            CASE WHEN has_comorbid_depression_anxiety THEN 'integrated_treatment' END
        ) AS recommended_therapy_modalities,
        
        -- ===== URGENCY COMMUNICATION FLAGS =====
        
        -- Communication tone
        CASE 
            WHEN mental_health_urgency_level = 'emergency' THEN 'urgent_compassionate_immediate'
            WHEN mental_health_urgency_level = 'urgent' THEN 'serious_supportive_prompt'
            WHEN mental_health_urgency_level = 'needs_attention' THEN 'concerned_encouraging'
            WHEN mental_health_urgency_level = 'routine' THEN 'supportive_informative'
            ELSE 'positive_preventive'
        END AS communication_tone,
        
        -- Follow-up urgency
        CASE 
            WHEN mental_health_urgency_level = 'emergency' THEN 'immediate_within_24hrs'
            WHEN mental_health_urgency_level = 'urgent' THEN 'within_1_week'
            WHEN mental_health_urgency_level = 'needs_attention' THEN 'within_2_weeks'
            WHEN mental_health_urgency_level = 'routine' THEN 'within_1_month'
            ELSE 'as_needed'
        END AS recommended_followup_timeframe,
        
        -- ===== TREATMENT PLAN COMPLEXITY =====
        
        -- Treatment plan complexity
        CASE 
            WHEN case_complexity_score >= 4 THEN 'complex_multidisciplinary'
            WHEN case_complexity_score = 3 THEN 'moderate_coordinated_care'
            WHEN case_complexity_score BETWEEN 1 AND 2 THEN 'standard_single_provider'
            ELSE 'basic_self_help'
        END AS treatment_plan_complexity,
        
        -- Monitoring intensity needed
        CASE 
            WHEN needs_suicide_risk_screening THEN 'intensive_monitoring'
            WHEN mental_health_urgency_level IN ('emergency', 'urgent') THEN 'frequent_monitoring'
            WHEN depression_risk_score >= 10 OR anxiety_risk_score >= 10 THEN 'regular_monitoring'
            WHEN symptom_count >= 2 THEN 'periodic_monitoring'
            ELSE 'self_monitoring'
        END AS monitoring_intensity_needed,
        
        -- ===== CONVERSATION FLOW GUIDANCE =====
        
        -- Start conversation with
        CASE 
            WHEN needs_suicide_risk_screening THEN 'safety_assessment'
            WHEN mental_health_urgency_level = 'emergency' THEN 'crisis_intervention'
            WHEN in_active_treatment THEN 'treatment_review'
            WHEN has_mental_health_history THEN 'history_exploration'
            ELSE 'symptom_screening'
        END AS conversation_starting_point,
        
        -- Primary concern to address
        CASE 
            WHEN needs_suicide_risk_screening THEN 'immediate_safety'
            WHEN depression_risk_score > anxiety_risk_score AND depression_risk_score >= 10 THEN 'severe_depression'
            WHEN anxiety_risk_score > depression_risk_score AND anxiety_risk_score >= 10 THEN 'severe_anxiety'
            WHEN has_comorbid_depression_anxiety THEN 'comorbid_conditions'
            WHEN functional_impairment_category = 'severe_impairment' THEN 'functional_decline'
            WHEN has_treatment_gap THEN 'treatment_discontinuation'
            WHEN social_isolation_level = 'severe_isolation' THEN 'social_withdrawal'
            ELSE 'symptom_management'
        END AS primary_clinical_concern

    FROM risk_features
)

SELECT * FROM conversation_features