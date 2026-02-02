-- models/gold/features/womens_wellness_llm/ftr_pcos_core_clinical.sql

{{
    config(
        materialized='table',
        tags=['features', 'womens_wellness', 'pcos', 'tier1']
    )
}}

WITH base_data AS (
    SELECT * FROM {{ ref('stg_pcos_cleaned') }}
),

core_features AS (
    SELECT
        *,
        
        -- ===== AGE & REPRODUCTIVE STAGE =====
        
        -- Reproductive age category
        CASE 
            WHEN AGE < 18 THEN 'adolescent'
            WHEN AGE BETWEEN 18 AND 25 THEN 'young_reproductive'
            WHEN AGE BETWEEN 26 AND 35 THEN 'prime_reproductive'
            WHEN AGE BETWEEN 36 AND 45 THEN 'late_reproductive'
            WHEN AGE > 45 THEN 'perimenopausal_or_older'
            ELSE 'unknown'
        END AS reproductive_age_category,
        
        -- Peak PCOS diagnosis age flag
        CASE 
            WHEN AGE BETWEEN 18 AND 35 THEN TRUE 
            ELSE FALSE 
        END AS in_peak_pcos_age,
        
        -- ===== HORMONAL FEATURES - LH & FSH =====
        
        -- LH level classification (normal range: 1.9-12.5 mIU/mL for reproductive age)
        CASE 
            WHEN LH_LEVEL < 1.9 THEN 'low'
            WHEN LH_LEVEL BETWEEN 1.9 AND 12.5 THEN 'normal'
            WHEN LH_LEVEL BETWEEN 12.6 AND 20.0 THEN 'elevated_mild'
            WHEN LH_LEVEL > 20.0 THEN 'elevated_high'
            ELSE 'unknown'
        END AS lh_level_category,
        
        -- FSH level classification (normal range: 1.4-9.9 mIU/mL for follicular phase)
        CASE 
            WHEN FSH_LEVEL < 1.4 THEN 'low'
            WHEN FSH_LEVEL BETWEEN 1.4 AND 9.9 THEN 'normal'
            WHEN FSH_LEVEL BETWEEN 10.0 AND 20.0 THEN 'elevated_mild'
            WHEN FSH_LEVEL > 20.0 THEN 'elevated_high'
            ELSE 'unknown'
        END AS fsh_level_category,
        
        -- LH/FSH Ratio (KEY PCOS INDICATOR - ratio >2:1 suggests PCOS)
        ROUND(LH_LEVEL / NULLIF(FSH_LEVEL, 0), 2) AS lh_fsh_ratio,
        
        -- LH/FSH ratio interpretation
        CASE 
            WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) < 1.0 THEN 'low_ratio'
            WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) BETWEEN 1.0 AND 1.9 THEN 'normal_ratio'
            WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) BETWEEN 2.0 AND 2.9 THEN 'elevated_pcos_suggestive'
            WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) >= 3.0 THEN 'very_elevated_pcos_likely'
            ELSE 'unknown'
        END AS lh_fsh_ratio_category,
        
        -- PCOS hormonal pattern flag (LH/FSH ratio >= 2)
        CASE 
            WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) >= 2.0 THEN TRUE 
            ELSE FALSE 
        END AS has_pcos_lh_fsh_pattern,
        
        -- ===== AMH FEATURES (Anti-Müllerian Hormone) =====
        
        -- AMH level classification (normal: 1.0-4.0 ng/mL, high AMH associated with PCOS)
        CASE 
            WHEN AMH_LEVEL < 1.0 THEN 'low_ovarian_reserve'
            WHEN AMH_LEVEL BETWEEN 1.0 AND 4.0 THEN 'normal'
            WHEN AMH_LEVEL BETWEEN 4.1 AND 7.0 THEN 'elevated_pcos_risk'
            WHEN AMH_LEVEL > 7.0 THEN 'very_elevated_pcos_likely'
            ELSE 'unknown'
        END AS amh_level_category,
        
        -- PCOS AMH indicator (AMH > 4.0 ng/mL)
        CASE 
            WHEN AMH_LEVEL > 4.0 THEN TRUE 
            ELSE FALSE 
        END AS has_elevated_amh_pcos,
        
        -- Ovarian reserve status
        CASE 
            WHEN AMH_LEVEL < 1.0 THEN 'diminished_ovarian_reserve'
            WHEN AMH_LEVEL BETWEEN 1.0 AND 2.0 THEN 'low_normal'
            WHEN AMH_LEVEL BETWEEN 2.1 AND 4.0 THEN 'normal'
            WHEN AMH_LEVEL > 4.0 THEN 'polycystic_ovary_pattern'
            ELSE 'unknown'
        END AS ovarian_reserve_status,
        
        -- ===== TESTOSTERONE FEATURES =====
        
        -- Testosterone level classification (normal female: 10-70 ng/dL)
        CASE 
            WHEN TESTOSTERONE_LEVEL < 10 THEN 'low'
            WHEN TESTOSTERONE_LEVEL BETWEEN 10 AND 70 THEN 'normal'
            WHEN TESTOSTERONE_LEVEL BETWEEN 71 AND 150 THEN 'mildly_elevated'
            WHEN TESTOSTERONE_LEVEL BETWEEN 151 AND 300 THEN 'moderately_elevated'
            WHEN TESTOSTERONE_LEVEL > 300 THEN 'severely_elevated'
            ELSE 'unknown'
        END AS testosterone_category,
        
        -- Hyperandrogenism flag (testosterone > 70 ng/dL)
        CASE 
            WHEN TESTOSTERONE_LEVEL > 70 THEN TRUE 
            ELSE FALSE 
        END AS has_hyperandrogenism,
        
        -- Severity of hyperandrogenism
        CASE 
            WHEN TESTOSTERONE_LEVEL <= 70 THEN 'no_hyperandrogenism'
            WHEN TESTOSTERONE_LEVEL BETWEEN 71 AND 100 THEN 'mild_hyperandrogenism'
            WHEN TESTOSTERONE_LEVEL BETWEEN 101 AND 150 THEN 'moderate_hyperandrogenism'
            WHEN TESTOSTERONE_LEVEL > 150 THEN 'severe_hyperandrogenism'
            ELSE 'unknown'
        END AS hyperandrogenism_severity,
        
        -- ===== BMI FEATURES (Metabolic Component) =====
        
        -- BMI category (WHO classification)
        CASE 
            WHEN BMI < 18.5 THEN 'underweight'
            WHEN BMI BETWEEN 18.5 AND 24.9 THEN 'normal'
            WHEN BMI BETWEEN 25.0 AND 29.9 THEN 'overweight'
            WHEN BMI BETWEEN 30.0 AND 34.9 THEN 'obese_class1'
            WHEN BMI BETWEEN 35.0 AND 39.9 THEN 'obese_class2'
            WHEN BMI >= 40.0 THEN 'obese_class3_severe'
            ELSE 'unknown'
        END AS bmi_category,
        
        -- Obesity flag (BMI >= 30, worsens PCOS)
        CASE 
            WHEN BMI >= 30.0 THEN TRUE 
            ELSE FALSE 
        END AS is_obese,
        
        -- Overweight or obese flag
        CASE 
            WHEN BMI >= 25.0 THEN TRUE 
            ELSE FALSE 
        END AS is_overweight_or_obese,
        
        -- ===== MENSTRUAL CYCLE FEATURES =====
        
        -- Menstrual cycle pattern standardization
        CASE 
            WHEN LOWER(MENSTRUAL_CYCLE_PATTERN) = 'regular' THEN 'regular'
            WHEN LOWER(MENSTRUAL_CYCLE_PATTERN) = 'irregular' THEN 'irregular'
            ELSE 'unknown'
        END AS menstrual_pattern_clean,
        
        -- Oligomenorrhea/amenorrhea flag (irregular cycles - PCOS criterion)
        CASE 
            WHEN LOWER(MENSTRUAL_CYCLE_PATTERN) = 'irregular' THEN TRUE 
            ELSE FALSE 
        END AS has_irregular_cycles,
        
        -- ===== FAMILY HISTORY =====
        
        -- Family history flag
        CASE 
            WHEN HAS_FAMILY_HISTORY THEN TRUE 
            ELSE FALSE 
        END AS has_pcos_family_history,
        
        -- ===== ROTTERDAM CRITERIA COMPONENTS =====
        -- PCOS diagnosis requires 2 of 3: irregular cycles, hyperandrogenism, polycystic ovaries (via ultrasound/AMH)
        
        -- Criterion 1: Irregular menstrual cycles
        CASE 
            WHEN LOWER(MENSTRUAL_CYCLE_PATTERN) = 'irregular' THEN 1 
            ELSE 0 
        END AS rotterdam_criterion_irregular_cycles,
        
        -- Criterion 2: Hyperandrogenism (clinical or biochemical)
        CASE 
            WHEN TESTOSTERONE_LEVEL > 70 THEN 1 
            ELSE 0 
        END AS rotterdam_criterion_hyperandrogenism,
        
        -- Criterion 3: Polycystic ovary morphology (using AMH as proxy)
        CASE 
            WHEN AMH_LEVEL > 4.0 THEN 1 
            ELSE 0 
        END AS rotterdam_criterion_pco_morphology,
        
        -- Rotterdam criteria count (0-3)
        (
            CASE WHEN LOWER(MENSTRUAL_CYCLE_PATTERN) = 'irregular' THEN 1 ELSE 0 END +
            CASE WHEN TESTOSTERONE_LEVEL > 70 THEN 1 ELSE 0 END +
            CASE WHEN AMH_LEVEL > 4.0 THEN 1 ELSE 0 END
        ) AS rotterdam_criteria_count,
        
        -- PCOS diagnosis flag (2 or more Rotterdam criteria)
        CASE 
            WHEN (
                CASE WHEN LOWER(MENSTRUAL_CYCLE_PATTERN) = 'irregular' THEN 1 ELSE 0 END +
                CASE WHEN TESTOSTERONE_LEVEL > 70 THEN 1 ELSE 0 END +
                CASE WHEN AMH_LEVEL > 4.0 THEN 1 ELSE 0 END
            ) >= 2 THEN TRUE 
            ELSE FALSE 
        END AS meets_pcos_rotterdam_criteria,
        
        -- ===== PCOS PHENOTYPE CLASSIFICATION =====
        
        -- PCOS phenotype (if diagnosed)
        CASE 
            -- Phenotype A: Classic PCOS (all 3 criteria)
            WHEN (
                CASE WHEN LOWER(MENSTRUAL_CYCLE_PATTERN) = 'irregular' THEN 1 ELSE 0 END +
                CASE WHEN TESTOSTERONE_LEVEL > 70 THEN 1 ELSE 0 END +
                CASE WHEN AMH_LEVEL > 4.0 THEN 1 ELSE 0 END
            ) = 3 THEN 'phenotype_a_classic'
            
            -- Phenotype B: Hyperandrogenic + Irregular cycles (no PCO)
            WHEN LOWER(MENSTRUAL_CYCLE_PATTERN) = 'irregular' 
                 AND TESTOSTERONE_LEVEL > 70 
                 AND AMH_LEVEL <= 4.0 THEN 'phenotype_b_anovulatory_hyperandrogenic'
            
            -- Phenotype C: PCO + Hyperandrogenic (regular cycles)
            WHEN LOWER(MENSTRUAL_CYCLE_PATTERN) = 'regular' 
                 AND TESTOSTERONE_LEVEL > 70 
                 AND AMH_LEVEL > 4.0 THEN 'phenotype_c_ovulatory_hyperandrogenic'
            
            -- Phenotype D: PCO + Irregular cycles (no hyperandrogenism)
            WHEN LOWER(MENSTRUAL_CYCLE_PATTERN) = 'irregular' 
                 AND TESTOSTERONE_LEVEL <= 70 
                 AND AMH_LEVEL > 4.0 THEN 'phenotype_d_mild_pcos'
            
            ELSE 'no_pcos_phenotype'
        END AS pcos_phenotype,
        
        -- ===== HORMONAL IMBALANCE INDICATORS =====
        
        -- Multiple hormonal abnormalities count
        (
            CASE WHEN LH_LEVEL > 12.5 THEN 1 ELSE 0 END +
            CASE WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) >= 2.0 THEN 1 ELSE 0 END +
            CASE WHEN AMH_LEVEL > 4.0 THEN 1 ELSE 0 END +
            CASE WHEN TESTOSTERONE_LEVEL > 70 THEN 1 ELSE 0 END
        ) AS hormonal_abnormality_count,
        
        -- Severe hormonal imbalance flag
        CASE 
            WHEN (
                CASE WHEN LH_LEVEL > 12.5 THEN 1 ELSE 0 END +
                CASE WHEN (LH_LEVEL / NULLIF(FSH_LEVEL, 0)) >= 2.0 THEN 1 ELSE 0 END +
                CASE WHEN AMH_LEVEL > 4.0 THEN 1 ELSE 0 END +
                CASE WHEN TESTOSTERONE_LEVEL > 70 THEN 1 ELSE 0 END
            ) >= 3 THEN TRUE 
            ELSE FALSE 
        END AS has_severe_hormonal_imbalance

    FROM base_data
)

SELECT * FROM core_features