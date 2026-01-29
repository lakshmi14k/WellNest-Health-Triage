-- models/gold/features/womens_wellness_llm/ftr_pcos_risk_urgency.sql

{{
    config(
        materialized='table',
        tags=['features', 'womens_wellness', 'pcos', 'tier2']
    )
}}

WITH core_features AS (
    SELECT * FROM {{ ref('ftr_pcos_core_clinical') }}
),

risk_urgency_features AS (
    SELECT
        *,
        
        -- ===== PCOS SEVERITY SCORING =====
        
        -- PCOS severity score (0-15 scale)
        (
            -- Menstrual irregularity (0-3)
            CASE WHEN has_irregular_cycles THEN 3 ELSE 0 END +
            
            -- Hyperandrogenism severity (0-4)
            CASE 
                WHEN TESTOSTERONE_LEVEL > 150 THEN 4
                WHEN TESTOSTERONE_LEVEL > 100 THEN 3
                WHEN TESTOSTERONE_LEVEL > 70 THEN 2
                ELSE 0
            END +
            
            -- LH/FSH ratio (0-3)
            CASE 
                WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) >= 3.0 THEN 3
                WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) >= 2.0 THEN 2
                WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) >= 1.5 THEN 1
                ELSE 0
            END +
            
            -- AMH elevation (0-3)
            CASE 
                WHEN AMH_LEVEL > 10.0 THEN 3
                WHEN AMH_LEVEL > 7.0 THEN 2
                WHEN AMH_LEVEL > 4.0 THEN 1
                ELSE 0
            END +
            
            -- Metabolic component (0-2)
            CASE 
                WHEN BMI >= 35 THEN 2
                WHEN BMI >= 30 THEN 1
                ELSE 0
            END
        ) AS pcos_severity_score,
        
        -- PCOS severity category
        CASE 
            WHEN (
                CASE WHEN has_irregular_cycles THEN 3 ELSE 0 END +
                CASE 
                    WHEN TESTOSTERONE_LEVEL > 150 THEN 4
                    WHEN TESTOSTERONE_LEVEL > 100 THEN 3
                    WHEN TESTOSTERONE_LEVEL > 70 THEN 2
                    ELSE 0
                END +
                CASE 
                    WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) >= 3.0 THEN 3
                    WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) >= 2.0 THEN 2
                    WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) >= 1.5 THEN 1
                    ELSE 0
                END +
                CASE 
                    WHEN AMH_LEVEL > 10.0 THEN 3
                    WHEN AMH_LEVEL > 7.0 THEN 2
                    WHEN AMH_LEVEL > 4.0 THEN 1
                    ELSE 0
                END +
                CASE 
                    WHEN BMI >= 35 THEN 2
                    WHEN BMI >= 30 THEN 1
                    ELSE 0
                END
            ) = 0 THEN 'no_pcos'
            WHEN (
                CASE WHEN has_irregular_cycles THEN 3 ELSE 0 END +
                CASE 
                    WHEN TESTOSTERONE_LEVEL > 150 THEN 4
                    WHEN TESTOSTERONE_LEVEL > 100 THEN 3
                    WHEN TESTOSTERONE_LEVEL > 70 THEN 2
                    ELSE 0
                END +
                CASE 
                    WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) >= 3.0 THEN 3
                    WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) >= 2.0 THEN 2
                    WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) >= 1.5 THEN 1
                    ELSE 0
                END +
                CASE 
                    WHEN AMH_LEVEL > 10.0 THEN 3
                    WHEN AMH_LEVEL > 7.0 THEN 2
                    WHEN AMH_LEVEL > 4.0 THEN 1
                    ELSE 0
                END +
                CASE 
                    WHEN BMI >= 35 THEN 2
                    WHEN BMI >= 30 THEN 1
                    ELSE 0
                END
            ) BETWEEN 1 AND 4 THEN 'mild_pcos'
            WHEN (
                CASE WHEN has_irregular_cycles THEN 3 ELSE 0 END +
                CASE 
                    WHEN TESTOSTERONE_LEVEL > 150 THEN 4
                    WHEN TESTOSTERONE_LEVEL > 100 THEN 3
                    WHEN TESTOSTERONE_LEVEL > 70 THEN 2
                    ELSE 0
                END +
                CASE 
                    WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) >= 3.0 THEN 3
                    WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) >= 2.0 THEN 2
                    WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) >= 1.5 THEN 1
                    ELSE 0
                END +
                CASE 
                    WHEN AMH_LEVEL > 10.0 THEN 3
                    WHEN AMH_LEVEL > 7.0 THEN 2
                    WHEN AMH_LEVEL > 4.0 THEN 1
                    ELSE 0
                END +
                CASE 
                    WHEN BMI >= 35 THEN 2
                    WHEN BMI >= 30 THEN 1
                    ELSE 0
                END
            ) BETWEEN 5 AND 9 THEN 'moderate_pcos'
            WHEN (
                CASE WHEN has_irregular_cycles THEN 3 ELSE 0 END +
                CASE 
                    WHEN TESTOSTERONE_LEVEL > 150 THEN 4
                    WHEN TESTOSTERONE_LEVEL > 100 THEN 3
                    WHEN TESTOSTERONE_LEVEL > 70 THEN 2
                    ELSE 0
                END +
                CASE 
                    WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) >= 3.0 THEN 3
                    WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) >= 2.0 THEN 2
                    WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) >= 1.5 THEN 1
                    ELSE 0
                END +
                CASE 
                    WHEN AMH_LEVEL > 10.0 THEN 3
                    WHEN AMH_LEVEL > 7.0 THEN 2
                    WHEN AMH_LEVEL > 4.0 THEN 1
                    ELSE 0
                END +
                CASE 
                    WHEN BMI >= 35 THEN 2
                    WHEN BMI >= 30 THEN 1
                    ELSE 0
                END
            ) >= 10 THEN 'severe_pcos'
        END AS pcos_severity_category,
        
        -- ===== METABOLIC SYNDROME RISK (Common with PCOS) =====
        
        -- Metabolic syndrome risk score (0-10 scale)
        (
            CASE WHEN BMI >= 30 THEN 3
                 WHEN BMI >= 25 THEN 1
                 ELSE 0 END +
            CASE WHEN has_hyperandrogenism THEN 2 ELSE 0 END +
            CASE WHEN has_irregular_cycles THEN 2 ELSE 0 END +
            CASE WHEN meets_pcos_rotterdam_criteria THEN 3 ELSE 0 END
        ) AS metabolic_syndrome_risk_score,
        
        -- Metabolic risk category
        CASE 
            WHEN (
                CASE WHEN BMI >= 30 THEN 3
                     WHEN BMI >= 25 THEN 1
                     ELSE 0 END +
                CASE WHEN has_hyperandrogenism THEN 2 ELSE 0 END +
                CASE WHEN has_irregular_cycles THEN 2 ELSE 0 END +
                CASE WHEN meets_pcos_rotterdam_criteria THEN 3 ELSE 0 END
            ) = 0 THEN 'low_risk'
            WHEN (
                CASE WHEN BMI >= 30 THEN 3
                     WHEN BMI >= 25 THEN 1
                     ELSE 0 END +
                CASE WHEN has_hyperandrogenism THEN 2 ELSE 0 END +
                CASE WHEN has_irregular_cycles THEN 2 ELSE 0 END +
                CASE WHEN meets_pcos_rotterdam_criteria THEN 3 ELSE 0 END
            ) BETWEEN 1 AND 4 THEN 'moderate_risk'
            WHEN (
                CASE WHEN BMI >= 30 THEN 3
                     WHEN BMI >= 25 THEN 1
                     ELSE 0 END +
                CASE WHEN has_hyperandrogenism THEN 2 ELSE 0 END +
                CASE WHEN has_irregular_cycles THEN 2 ELSE 0 END +
                CASE WHEN meets_pcos_rotterdam_criteria THEN 3 ELSE 0 END
            ) >= 5 THEN 'high_risk'
        END AS metabolic_risk_category,
        
        -- ===== FERTILITY RISK ASSESSMENT =====
        
        -- Infertility risk score (0-12 scale)
        (
            CASE WHEN has_irregular_cycles THEN 4 ELSE 0 END +
            CASE WHEN AMH_LEVEL < 1.0 THEN 4
                 WHEN AMH_LEVEL > 10.0 THEN 3
                 WHEN AMH_LEVEL > 7.0 THEN 2
                 ELSE 0 END +
            CASE WHEN AGE >= 35 THEN 2
                 WHEN AGE >= 40 THEN 4
                 ELSE 0 END +
            CASE WHEN BMI >= 35 THEN 2
                 WHEN BMI < 18.5 THEN 2
                 ELSE 0 END
        ) AS infertility_risk_score,
        
        -- Fertility risk category
        CASE 
            WHEN (
                CASE WHEN has_irregular_cycles THEN 4 ELSE 0 END +
                CASE WHEN AMH_LEVEL < 1.0 THEN 4
                     WHEN AMH_LEVEL > 10.0 THEN 3
                     WHEN AMH_LEVEL > 7.0 THEN 2
                     ELSE 0 END +
                CASE WHEN AGE >= 35 THEN 2
                     WHEN AGE >= 40 THEN 4
                     ELSE 0 END +
                CASE WHEN BMI >= 35 THEN 2
                     WHEN BMI < 18.5 THEN 2
                     ELSE 0 END
            ) = 0 THEN 'low_fertility_risk'
            WHEN (
                CASE WHEN has_irregular_cycles THEN 4 ELSE 0 END +
                CASE WHEN AMH_LEVEL < 1.0 THEN 4
                     WHEN AMH_LEVEL > 10.0 THEN 3
                     WHEN AMH_LEVEL > 7.0 THEN 2
                     ELSE 0 END +
                CASE WHEN AGE >= 35 THEN 2
                     WHEN AGE >= 40 THEN 4
                     ELSE 0 END +
                CASE WHEN BMI >= 35 THEN 2
                     WHEN BMI < 18.5 THEN 2
                     ELSE 0 END
            ) BETWEEN 1 AND 4 THEN 'moderate_fertility_risk'
            WHEN (
                CASE WHEN has_irregular_cycles THEN 4 ELSE 0 END +
                CASE WHEN AMH_LEVEL < 1.0 THEN 4
                     WHEN AMH_LEVEL > 10.0 THEN 3
                     WHEN AMH_LEVEL > 7.0 THEN 2
                     ELSE 0 END +
                CASE WHEN AGE >= 35 THEN 2
                     WHEN AGE >= 40 THEN 4
                     ELSE 0 END +
                CASE WHEN BMI >= 35 THEN 2
                     WHEN BMI < 18.5 THEN 2
                     ELSE 0 END
            ) >= 5 THEN 'high_fertility_risk'
        END AS fertility_risk_category,
        
        -- ===== LONG-TERM COMPLICATION RISKS =====
        
        -- Type 2 diabetes risk (PCOS increases risk 4-fold)
        CASE 
            WHEN meets_pcos_rotterdam_criteria AND BMI >= 30 THEN 'very_high_risk'
            WHEN meets_pcos_rotterdam_criteria OR BMI >= 30 THEN 'high_risk'
            WHEN is_overweight_or_obese THEN 'moderate_risk'
            ELSE 'average_risk'
        END AS type2_diabetes_risk,
        
        -- Cardiovascular disease risk
        CASE 
            WHEN meets_pcos_rotterdam_criteria AND BMI >= 30 AND has_hyperandrogenism THEN 'very_high_risk'
            WHEN meets_pcos_rotterdam_criteria AND (BMI >= 30 OR has_hyperandrogenism) THEN 'high_risk'
            WHEN meets_pcos_rotterdam_criteria THEN 'moderate_risk'
            ELSE 'average_risk'
        END AS cardiovascular_risk_category,
        
        -- Endometrial cancer risk (due to anovulation)
        CASE 
            WHEN has_irregular_cycles AND meets_pcos_rotterdam_criteria THEN 'elevated_risk'
            WHEN has_irregular_cycles THEN 'moderate_risk'
            ELSE 'average_risk'
        END AS endometrial_cancer_risk,
        
        -- ===== OVERALL URGENCY LEVEL =====
        
        -- PCOS urgency level (not typically urgent, but guides care timing)
        CASE 
            -- Urgent: Severe symptoms affecting quality of life
            WHEN pcos_severity_score >= 12 THEN 'needs_prompt_evaluation'
            WHEN TESTOSTERONE_LEVEL > 200 THEN 'needs_prompt_evaluation'
            
            -- Needs attention: Confirmed or likely PCOS
            WHEN meets_pcos_rotterdam_criteria THEN 'needs_specialist_evaluation'
            WHEN rotterdam_criteria_count = 1 AND has_pcos_family_history THEN 'needs_evaluation'
            
            -- Routine: Monitoring or screening
            WHEN rotterdam_criteria_count = 1 THEN 'routine_monitoring'
            WHEN has_pcos_family_history THEN 'screening_recommended'
            
            ELSE 'routine'
        END AS pcos_urgency_level,
        
        -- ===== TREATMENT PRIORITIES =====
        
        -- Weight management priority
        CASE 
            WHEN BMI >= 35 AND meets_pcos_rotterdam_criteria THEN 'critical_priority'
            WHEN BMI >= 30 AND meets_pcos_rotterdam_criteria THEN 'high_priority'
            WHEN BMI >= 25 AND meets_pcos_rotterdam_criteria THEN 'moderate_priority'
            WHEN is_overweight_or_obese THEN 'recommended'
            ELSE 'not_indicated'
        END AS weight_management_priority,
        
        -- Hormonal treatment consideration
        CASE 
            WHEN meets_pcos_rotterdam_criteria AND has_irregular_cycles THEN 'likely_needs_treatment'
            WHEN hyperandrogenism_severity IN ('moderate_hyperandrogenism', 'severe_hyperandrogenism') THEN 'likely_needs_treatment'
            WHEN rotterdam_criteria_count >= 1 THEN 'consider_treatment'
            ELSE 'not_indicated'
        END AS hormonal_treatment_indication,
        
        -- Fertility treatment consideration
        CASE 
            WHEN infertility_risk_score >= 8 THEN 'high_priority_referral'
            WHEN infertility_risk_score >= 5 THEN 'consider_fertility_specialist'
            WHEN has_irregular_cycles AND in_peak_pcos_age THEN 'fertility_counseling_recommended'
            ELSE 'routine_fertility_education'
        END AS fertility_treatment_priority

    FROM core_features
)

SELECT * FROM risk_urgency_features