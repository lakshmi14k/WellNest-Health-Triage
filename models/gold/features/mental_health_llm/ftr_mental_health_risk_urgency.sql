-- models/gold/features/mental_health_llm/ftr_mental_health_risk_urgency.sql

{{
    config(
        materialized='table',
        tags=['features', 'mental_health', 'tier2']
    )
}}

WITH core_features AS (
    SELECT * FROM {{ ref('ftr_mental_health_core_clinical') }}
),

risk_urgency_features AS (
    SELECT
        *,
        
        -- ===== DEPRESSION RISK ASSESSMENT =====
        
        -- Depression symptom cluster score (PHQ-9 style)
        (
            -- Anhedonia (loss of interest)
            CASE WHEN LOWER(WORK_INTEREST) = 'no' THEN 3
                 WHEN LOWER(WORK_INTEREST) = 'maybe' THEN 2
                 ELSE 0 END +
            -- Mood symptoms
            CASE WHEN LOWER(MOOD_SWINGS) = 'high' THEN 3
                 WHEN LOWER(MOOD_SWINGS) = 'medium' THEN 2
                 WHEN LOWER(MOOD_SWINGS) = 'low' THEN 1
                 ELSE 0 END +
            -- Social withdrawal
            CASE WHEN isolation_severity_score >= 3 THEN 3
                 WHEN isolation_severity_score = 2 THEN 2
                 WHEN isolation_severity_score = 1 THEN 1
                 ELSE 0 END +
            -- Behavioral changes
            CASE WHEN LOWER(CHANGES_HABITS) = 'yes' THEN 2
                 WHEN LOWER(CHANGES_HABITS) = 'maybe' THEN 1
                 ELSE 0 END +
            -- Coping difficulties
            CASE WHEN HAS_COPING_STRUGGLES THEN 3 ELSE 0 END
        ) AS depression_risk_score,
        
        -- Depression severity category (0-18 scale)
        CASE 
            WHEN (
                CASE WHEN LOWER(WORK_INTEREST) = 'no' THEN 3
                     WHEN LOWER(WORK_INTEREST) = 'maybe' THEN 2
                     ELSE 0 END +
                CASE WHEN LOWER(MOOD_SWINGS) = 'high' THEN 3
                     WHEN LOWER(MOOD_SWINGS) = 'medium' THEN 2
                     WHEN LOWER(MOOD_SWINGS) = 'low' THEN 1
                     ELSE 0 END +
                CASE WHEN isolation_severity_score >= 3 THEN 3
                     WHEN isolation_severity_score = 2 THEN 2
                     WHEN isolation_severity_score = 1 THEN 1
                     ELSE 0 END +
                CASE WHEN LOWER(CHANGES_HABITS) = 'yes' THEN 2
                     WHEN LOWER(CHANGES_HABITS) = 'maybe' THEN 1
                     ELSE 0 END +
                CASE WHEN HAS_COPING_STRUGGLES THEN 3 ELSE 0 END
            ) = 0 THEN 'minimal_depression'
            WHEN (
                CASE WHEN LOWER(WORK_INTEREST) = 'no' THEN 3
                     WHEN LOWER(WORK_INTEREST) = 'maybe' THEN 2
                     ELSE 0 END +
                CASE WHEN LOWER(MOOD_SWINGS) = 'high' THEN 3
                     WHEN LOWER(MOOD_SWINGS) = 'medium' THEN 2
                     WHEN LOWER(MOOD_SWINGS) = 'low' THEN 1
                     ELSE 0 END +
                CASE WHEN isolation_severity_score >= 3 THEN 3
                     WHEN isolation_severity_score = 2 THEN 2
                     WHEN isolation_severity_score = 1 THEN 1
                     ELSE 0 END +
                CASE WHEN LOWER(CHANGES_HABITS) = 'yes' THEN 2
                     WHEN LOWER(CHANGES_HABITS) = 'maybe' THEN 1
                     ELSE 0 END +
                CASE WHEN HAS_COPING_STRUGGLES THEN 3 ELSE 0 END
            ) BETWEEN 1 AND 4 THEN 'mild_depression'
            WHEN (
                CASE WHEN LOWER(WORK_INTEREST) = 'no' THEN 3
                     WHEN LOWER(WORK_INTEREST) = 'maybe' THEN 2
                     ELSE 0 END +
                CASE WHEN LOWER(MOOD_SWINGS) = 'high' THEN 3
                     WHEN LOWER(MOOD_SWINGS) = 'medium' THEN 2
                     WHEN LOWER(MOOD_SWINGS) = 'low' THEN 1
                     ELSE 0 END +
                CASE WHEN isolation_severity_score >= 3 THEN 3
                     WHEN isolation_severity_score = 2 THEN 2
                     WHEN isolation_severity_score = 1 THEN 1
                     ELSE 0 END +
                CASE WHEN LOWER(CHANGES_HABITS) = 'yes' THEN 2
                     WHEN LOWER(CHANGES_HABITS) = 'maybe' THEN 1
                     ELSE 0 END +
                CASE WHEN HAS_COPING_STRUGGLES THEN 3 ELSE 0 END
            ) BETWEEN 5 AND 9 THEN 'moderate_depression'
            WHEN (
                CASE WHEN LOWER(WORK_INTEREST) = 'no' THEN 3
                     WHEN LOWER(WORK_INTEREST) = 'maybe' THEN 2
                     ELSE 0 END +
                CASE WHEN LOWER(MOOD_SWINGS) = 'high' THEN 3
                     WHEN LOWER(MOOD_SWINGS) = 'medium' THEN 2
                     WHEN LOWER(MOOD_SWINGS) = 'low' THEN 1
                     ELSE 0 END +
                CASE WHEN isolation_severity_score >= 3 THEN 3
                     WHEN isolation_severity_score = 2 THEN 2
                     WHEN isolation_severity_score = 1 THEN 1
                     ELSE 0 END +
                CASE WHEN LOWER(CHANGES_HABITS) = 'yes' THEN 2
                     WHEN LOWER(CHANGES_HABITS) = 'maybe' THEN 1
                     ELSE 0 END +
                CASE WHEN HAS_COPING_STRUGGLES THEN 3 ELSE 0 END
            ) BETWEEN 10 AND 14 THEN 'moderately_severe_depression'
            WHEN (
                CASE WHEN LOWER(WORK_INTEREST) = 'no' THEN 3
                     WHEN LOWER(WORK_INTEREST) = 'maybe' THEN 2
                     ELSE 0 END +
                CASE WHEN LOWER(MOOD_SWINGS) = 'high' THEN 3
                     WHEN LOWER(MOOD_SWINGS) = 'medium' THEN 2
                     WHEN LOWER(MOOD_SWINGS) = 'low' THEN 1
                     ELSE 0 END +
                CASE WHEN isolation_severity_score >= 3 THEN 3
                     WHEN isolation_severity_score = 2 THEN 2
                     WHEN isolation_severity_score = 1 THEN 1
                     ELSE 0 END +
                CASE WHEN LOWER(CHANGES_HABITS) = 'yes' THEN 2
                     WHEN LOWER(CHANGES_HABITS) = 'maybe' THEN 1
                     ELSE 0 END +
                CASE WHEN HAS_COPING_STRUGGLES THEN 3 ELSE 0 END
            ) >= 15 THEN 'severe_depression'
        END AS depression_severity,
        
        -- ===== ANXIETY RISK ASSESSMENT =====
        
        -- Anxiety symptom cluster score (GAD-7 style)
        (
            -- Stress/worry
            CASE WHEN LOWER(GROWING_STRESS) = 'yes' THEN 3
                 WHEN LOWER(GROWING_STRESS) = 'maybe' THEN 2
                 ELSE 0 END +
            -- Coping difficulties (inability to control worry)
            CASE WHEN HAS_COPING_STRUGGLES THEN 3 ELSE 0 END +
            -- Social anxiety
            CASE WHEN LOWER(SOCIAL_WEAKNESS) = 'yes' THEN 3
                 WHEN LOWER(SOCIAL_WEAKNESS) = 'maybe' THEN 2
                 ELSE 0 END +
            -- Mood instability (irritability/restlessness)
            CASE WHEN LOWER(MOOD_SWINGS) IN ('medium', 'high') THEN 2 ELSE 0 END
        ) AS anxiety_risk_score,
        
        -- Anxiety severity category (0-14 scale)
        CASE 
            WHEN (
                CASE WHEN LOWER(GROWING_STRESS) = 'yes' THEN 3
                     WHEN LOWER(GROWING_STRESS) = 'maybe' THEN 2
                     ELSE 0 END +
                CASE WHEN HAS_COPING_STRUGGLES THEN 3 ELSE 0 END +
                CASE WHEN LOWER(SOCIAL_WEAKNESS) = 'yes' THEN 3
                     WHEN LOWER(SOCIAL_WEAKNESS) = 'maybe' THEN 2
                     ELSE 0 END +
                CASE WHEN LOWER(MOOD_SWINGS) IN ('medium', 'high') THEN 2 ELSE 0 END
            ) = 0 THEN 'minimal_anxiety'
            WHEN (
                CASE WHEN LOWER(GROWING_STRESS) = 'yes' THEN 3
                     WHEN LOWER(GROWING_STRESS) = 'maybe' THEN 2
                     ELSE 0 END +
                CASE WHEN HAS_COPING_STRUGGLES THEN 3 ELSE 0 END +
                CASE WHEN LOWER(SOCIAL_WEAKNESS) = 'yes' THEN 3
                     WHEN LOWER(SOCIAL_WEAKNESS) = 'maybe' THEN 2
                     ELSE 0 END +
                CASE WHEN LOWER(MOOD_SWINGS) IN ('medium', 'high') THEN 2 ELSE 0 END
            ) BETWEEN 1 AND 4 THEN 'mild_anxiety'
            WHEN (
                CASE WHEN LOWER(GROWING_STRESS) = 'yes' THEN 3
                     WHEN LOWER(GROWING_STRESS) = 'maybe' THEN 2
                     ELSE 0 END +
                CASE WHEN HAS_COPING_STRUGGLES THEN 3 ELSE 0 END +
                CASE WHEN LOWER(SOCIAL_WEAKNESS) = 'yes' THEN 3
                     WHEN LOWER(SOCIAL_WEAKNESS) = 'maybe' THEN 2
                     ELSE 0 END +
                CASE WHEN LOWER(MOOD_SWINGS) IN ('medium', 'high') THEN 2 ELSE 0 END
            ) BETWEEN 5 AND 9 THEN 'moderate_anxiety'
            WHEN (
                CASE WHEN LOWER(GROWING_STRESS) = 'yes' THEN 3
                     WHEN LOWER(GROWING_STRESS) = 'maybe' THEN 2
                     ELSE 0 END +
                CASE WHEN HAS_COPING_STRUGGLES THEN 3 ELSE 0 END +
                CASE WHEN LOWER(SOCIAL_WEAKNESS) = 'yes' THEN 3
                     WHEN LOWER(SOCIAL_WEAKNESS) = 'maybe' THEN 2
                     ELSE 0 END +
                CASE WHEN LOWER(MOOD_SWINGS) IN ('medium', 'high') THEN 2 ELSE 0 END
            ) >= 10 THEN 'severe_anxiety'
        END AS anxiety_severity,
        
        -- ===== OVERALL MENTAL HEALTH URGENCY =====
        
        -- Combined urgency level
        CASE 
            -- Emergency: Severe symptoms + severe isolation + treatment gap
            WHEN severe_symptom_count >= 4 AND isolation_severity_score >= 3 AND NOT in_active_treatment THEN 'emergency'
            
            -- Urgent: Moderate-severe depression or anxiety + not in treatment
            WHEN depression_risk_score >= 10 AND NOT in_active_treatment THEN 'urgent'
            WHEN anxiety_risk_score >= 10 AND NOT in_active_treatment THEN 'urgent'
            
            -- Needs attention: Moderate symptoms or treatment gap with history
            WHEN depression_risk_score BETWEEN 5 AND 9 THEN 'needs_attention'
            WHEN anxiety_risk_score BETWEEN 5 AND 9 THEN 'needs_attention'
            WHEN has_treatment_gap THEN 'needs_attention'
            
            -- Routine: Mild symptoms or in treatment
            WHEN symptom_count >= 2 THEN 'routine'
            
            ELSE 'monitoring'
        END AS mental_health_urgency_level,
        
        -- ===== FUNCTIONAL IMPAIRMENT SCORE =====
        
        -- Overall functional impairment (0-10 scale)
        (
            -- Work/productivity impairment
            CASE WHEN LOWER(WORK_INTEREST) = 'no' THEN 3
                 WHEN LOWER(WORK_INTEREST) = 'maybe' THEN 2
                 ELSE 0 END +
            -- Social impairment
            CASE WHEN social_isolation_level = 'severe_isolation' THEN 3
                 WHEN social_isolation_level = 'moderate_isolation' THEN 2
                 WHEN social_isolation_level = 'mild_isolation' THEN 1
                 ELSE 0 END +
            -- Daily functioning
            CASE WHEN LOWER(CHANGES_HABITS) = 'yes' THEN 2
                 WHEN LOWER(CHANGES_HABITS) = 'maybe' THEN 1
                 ELSE 0 END +
            -- Coping capacity
            CASE WHEN HAS_COPING_STRUGGLES THEN 2 ELSE 0 END
        ) AS functional_impairment_score,
        
        -- Functional impairment category
        CASE 
            WHEN (
                CASE WHEN LOWER(WORK_INTEREST) = 'no' THEN 3
                     WHEN LOWER(WORK_INTEREST) = 'maybe' THEN 2
                     ELSE 0 END +
                CASE WHEN social_isolation_level = 'severe_isolation' THEN 3
                     WHEN social_isolation_level = 'moderate_isolation' THEN 2
                     WHEN social_isolation_level = 'mild_isolation' THEN 1
                     ELSE 0 END +
                CASE WHEN LOWER(CHANGES_HABITS) = 'yes' THEN 2
                     WHEN LOWER(CHANGES_HABITS) = 'maybe' THEN 1
                     ELSE 0 END +
                CASE WHEN HAS_COPING_STRUGGLES THEN 2 ELSE 0 END
            ) = 0 THEN 'no_impairment'
            WHEN (
                CASE WHEN LOWER(WORK_INTEREST) = 'no' THEN 3
                     WHEN LOWER(WORK_INTEREST) = 'maybe' THEN 2
                     ELSE 0 END +
                CASE WHEN social_isolation_level = 'severe_isolation' THEN 3
                     WHEN social_isolation_level = 'moderate_isolation' THEN 2
                     WHEN social_isolation_level = 'mild_isolation' THEN 1
                     ELSE 0 END +
                CASE WHEN LOWER(CHANGES_HABITS) = 'yes' THEN 2
                     WHEN LOWER(CHANGES_HABITS) = 'maybe' THEN 1
                     ELSE 0 END +
                CASE WHEN HAS_COPING_STRUGGLES THEN 2 ELSE 0 END
            ) BETWEEN 1 AND 3 THEN 'mild_impairment'
            WHEN (
                CASE WHEN LOWER(WORK_INTEREST) = 'no' THEN 3
                     WHEN LOWER(WORK_INTEREST) = 'maybe' THEN 2
                     ELSE 0 END +
                CASE WHEN social_isolation_level = 'severe_isolation' THEN 3
                     WHEN social_isolation_level = 'moderate_isolation' THEN 2
                     WHEN social_isolation_level = 'mild_isolation' THEN 1
                     ELSE 0 END +
                CASE WHEN LOWER(CHANGES_HABITS) = 'yes' THEN 2
                     WHEN LOWER(CHANGES_HABITS) = 'maybe' THEN 1
                     ELSE 0 END +
                CASE WHEN HAS_COPING_STRUGGLES THEN 2 ELSE 0 END
            ) BETWEEN 4 AND 6 THEN 'moderate_impairment'
            WHEN (
                CASE WHEN LOWER(WORK_INTEREST) = 'no' THEN 3
                     WHEN LOWER(WORK_INTEREST) = 'maybe' THEN 2
                     ELSE 0 END +
                CASE WHEN social_isolation_level = 'severe_isolation' THEN 3
                     WHEN social_isolation_level = 'moderate_isolation' THEN 2
                     WHEN social_isolation_level = 'mild_isolation' THEN 1
                     ELSE 0 END +
                CASE WHEN LOWER(CHANGES_HABITS) = 'yes' THEN 2
                     WHEN LOWER(CHANGES_HABITS) = 'maybe' THEN 1
                     ELSE 0 END +
                CASE WHEN HAS_COPING_STRUGGLES THEN 2 ELSE 0 END
            ) >= 7 THEN 'severe_impairment'
        END AS functional_impairment_category,
        
        -- ===== TREATMENT PRIORITY =====
        
        -- Therapy recommendation priority
        CASE 
            WHEN depression_risk_score >= 10 OR anxiety_risk_score >= 10 THEN 'high_priority'
            WHEN depression_risk_score >= 5 OR anxiety_risk_score >= 5 THEN 'moderate_priority'
            WHEN symptom_count >= 3 THEN 'low_priority'
            ELSE 'preventive'
        END AS therapy_priority,
        
        -- Medication evaluation priority
        CASE 
            WHEN depression_risk_score >= 15 THEN 'psychiatric_evaluation_urgent'
            WHEN depression_risk_score >= 10 OR anxiety_risk_score >= 10 THEN 'psychiatric_evaluation_needed'
            WHEN depression_risk_score >= 5 OR anxiety_risk_score >= 5 THEN 'consider_evaluation'
            ELSE 'not_indicated'
        END AS medication_evaluation_priority,
        
        -- ===== SUICIDE RISK SCREENING INDICATORS =====
        
        -- High-risk screening needed (based on available data)
        CASE 
            WHEN severe_symptom_count >= 4 AND isolation_severity_score >= 3 THEN TRUE
            WHEN depression_risk_score >= 15 THEN TRUE
            ELSE FALSE
        END AS needs_suicide_risk_screening,
        
        -- ===== COMORBIDITY & COMPLEXITY =====
        
        -- Both depression and anxiety present
        CASE 
            WHEN depression_risk_score >= 5 AND anxiety_risk_score >= 5 THEN TRUE 
            ELSE FALSE 
        END AS has_comorbid_depression_anxiety,
        
        -- Complexity score (0-5 scale)
        (
            CASE WHEN has_mental_health_history THEN 1 ELSE 0 END +
            CASE WHEN HAS_FAMILY_HISTORY THEN 1 ELSE 0 END +
            CASE WHEN has_treatment_gap THEN 1 ELSE 0 END +
            CASE WHEN functional_impairment_score >= 7 THEN 1 ELSE 0 END +
            CASE WHEN perceived_workplace_stigma THEN 1 ELSE 0 END
        ) AS case_complexity_score

    FROM core_features
)

SELECT * FROM risk_urgency_features