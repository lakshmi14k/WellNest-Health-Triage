select * from public.diabetes_training_prompts;
select * from diabetes_training_prompts;

-- ==================================================================
-- DIABETES CONVERSATIONAL TRAINING DATA GENERATOR
-- Converts structured features → text prompts for fine-tuning
-- ==================================================================

-- ==================================================================
-- DIABETES CONVERSATIONAL TRAINING DATA GENERATOR
-- Converts structured features → text prompts for fine-tuning
-- ==================================================================
-- ==================================================================
USE DATABASE wellnest;
USE SCHEMA public;
CREATE OR REPLACE VIEW public.diabetes_training_prompts AS

WITH base_features AS (
    SELECT * FROM public.finetuning_diabetes_v1
),

prompt_components AS (
    SELECT 
        patient_id,
        condition_type,
        clinical_urgency_level,  -- This is already calculated in the base view
        gender,
        bmi_category,
        is_current_smoker,
        is_obese,
        diabetes_stage,
        hba1c_level,
        glucose_control_status,
        glucose_hba1c_concordance,
        has_diabetes,
        cardiometabolic_disease_count,
        has_triple_diagnosis,
        has_multiple_conditions,
        has_heart_disease,
        has_hypertension,
        cardiovascular_risk_score,
        diabetes_complication_risk_score,
        metabolic_syndrome_score,
        has_premature_disease,
        glucose_urgency_level,
        hyperglycemia_urgency,
        hypoglycemia_urgency,
        should_ask_diabetic_symptoms,
        should_ask_cardiovascular_symptoms,
        should_ask_neuropathy_symptoms,
        should_ask_diet_habits,
        should_ask_physical_activity,
        should_ask_medication_adherence,
        needs_glucose_monitoring,
        needs_bp_monitoring,
        needs_specialist_referral,
        smoking_cessation_priority,
        weight_management_priority,
        education_topics,
        
        -- ==============================================================
        -- COMPONENT 1: Patient Demographics & Context
        -- ==============================================================
        CONCAT(
            'Patient is a ', gender, 
            ' with BMI of ', bmi_category,
            CASE WHEN is_current_smoker THEN ', currently smokes' ELSE '' END,
            CASE WHEN is_obese THEN ', classified as obese' ELSE '' END,
            '.'
        ) as patient_context,
        
        -- ==============================================================
        -- COMPONENT 2: Clinical Presentation
        -- ==============================================================
        CONCAT(
            'Clinical findings: ',
            'Diabetes stage: ', diabetes_stage, '. ',
            'HbA1c level: ', hba1c_level, '%. ',
            'Glucose control: ', glucose_control_status, '. ',
            CASE 
                WHEN glucose_hba1c_concordance = 'concordant' 
                THEN 'Lab values are concordant. '
                ELSE 'Lab values show discordance. '
            END,
            CASE WHEN has_diabetes THEN 'Confirmed diabetes diagnosis. ' ELSE '' END
        ) as clinical_presentation,
        
        -- ==============================================================
        -- COMPONENT 3: Comorbidities
        -- ==============================================================
        CASE 
            WHEN cardiometabolic_disease_count = 0 THEN 'No significant comorbidities.'
            WHEN has_triple_diagnosis THEN 'Patient has triple diagnosis (diabetes, hypertension, heart disease).'
            WHEN has_multiple_conditions THEN CONCAT(
                'Patient has multiple conditions: ',
                CASE WHEN has_heart_disease THEN 'heart disease, ' ELSE '' END,
                CASE WHEN has_hypertension THEN 'hypertension, ' ELSE '' END,
                'diabetes.'
            )
            ELSE CONCAT(
                CASE WHEN has_heart_disease THEN 'History of heart disease. ' ELSE '' END,
                CASE WHEN has_hypertension THEN 'History of hypertension. ' ELSE '' END
            )
        END as comorbidities,
        
        -- ==============================================================
        -- COMPONENT 4: Risk Assessment
        -- ==============================================================
        CONCAT(
            'Risk assessment: ',
            'Cardiovascular risk score: ', cardiovascular_risk_score, '. ',
            'Diabetes complication risk: ', diabetes_complication_risk_score, '. ',
            'Metabolic syndrome score: ', metabolic_syndrome_score, '. ',
            CASE WHEN has_premature_disease THEN 'Premature disease present. ' ELSE '' END,
            'Glucose urgency: ', glucose_urgency_level, '.'
        ) as risk_assessment,
        
        -- ==============================================================
        -- COMPONENT 5: Urgency Indicators
        -- ==============================================================
        CASE 
            WHEN hyperglycemia_urgency IN ('severe', 'critical', 'emergency') 
            THEN CONCAT('URGENT: Severe hyperglycemia detected (', hyperglycemia_urgency, ').')
            WHEN hypoglycemia_urgency IN ('severe', 'critical', 'emergency')
            THEN CONCAT('URGENT: Severe hypoglycemia detected (', hypoglycemia_urgency, ').')
            WHEN glucose_urgency_level = 'urgent'
            THEN 'Glucose levels require urgent medical attention.'
            ELSE 'Glucose levels are at routine monitoring status.'
        END as urgency_note,
        
        -- ==============================================================
        -- COMPONENT 6: Symptom Inquiry Guidance
        -- ==============================================================
        CONCAT(
            'Key symptoms to assess: ',
            CASE WHEN should_ask_diabetic_symptoms THEN 'classic diabetic symptoms (polyuria, polydipsia, weight loss), ' ELSE '' END,
            CASE WHEN should_ask_cardiovascular_symptoms THEN 'cardiovascular symptoms (chest pain, shortness of breath), ' ELSE '' END,
            CASE WHEN should_ask_neuropathy_symptoms THEN 'neuropathy symptoms (tingling, numbness, pain), ' ELSE '' END,
            CASE WHEN should_ask_diet_habits THEN 'dietary habits, ' ELSE '' END,
            CASE WHEN should_ask_physical_activity THEN 'physical activity level, ' ELSE '' END,
            CASE WHEN should_ask_medication_adherence THEN 'medication adherence' ELSE '' END
        ) as symptom_inquiry,
        
        -- ==============================================================
        -- COMPONENT 7: Management Recommendations
        -- ==============================================================
        CONCAT(
            'Recommended management approach: ',
            CASE WHEN needs_glucose_monitoring THEN 'Initiate glucose monitoring discussion. ' ELSE '' END,
            CASE WHEN needs_bp_monitoring THEN 'Blood pressure monitoring needed. ' ELSE '' END,
            CASE WHEN needs_specialist_referral THEN 'Consider specialist referral. ' ELSE '' END,
            CASE 
                WHEN smoking_cessation_priority = 'high' THEN 'High priority: smoking cessation counseling. '
                WHEN smoking_cessation_priority = 'medium' THEN 'Medium priority: smoking cessation support. '
                ELSE ''
            END,
            CASE 
                WHEN weight_management_priority = 'high' THEN 'High priority: weight management intervention. '
                WHEN weight_management_priority = 'medium' THEN 'Moderate priority: weight management support. '
                ELSE ''
            END
        ) as management_recommendations,
        
        -- ==============================================================
        -- COMPONENT 8: Patient Education Topics
        -- ==============================================================
        CONCAT(
            'Priority education topics: ',
            COALESCE(education_topics, 'General diabetes management')
        ) as education_guidance,
        
        -- ==============================================================
        -- COMPONENT 9: Triage Classification (Ground Truth)
        -- ==============================================================
        CASE 
            WHEN clinical_urgency_level = 'emergency' 
            THEN 'This patient requires IMMEDIATE EMERGENCY evaluation. Direct to emergency department immediately.'
            WHEN clinical_urgency_level = 'urgent'
            THEN 'This patient requires URGENT medical evaluation within 24-48 hours. Schedule appointment promptly and provide interim guidance.'
            WHEN clinical_urgency_level = 'routine'
            THEN 'This patient can be managed with ROUTINE follow-up. Schedule regular monitoring and provide lifestyle counseling.'
            ELSE 'Further assessment needed to determine appropriate care level.'
        END as triage_recommendation
        
    FROM base_features
)

SELECT 
    patient_id,
    condition_type,
    clinical_urgency_level,  -- Use the one from prompt_components
    
    -- Assembled user prompt (what patient tells AI)
    CONCAT(
        patient_context, ' ',
        clinical_presentation, ' ',
        comorbidities, ' ',
        urgency_note
    ) as user_prompt,
    
    -- Assembled assistant response (what AI should say)
    CONCAT(
        risk_assessment, '\n\n',
        symptom_inquiry, '\n\n',
        management_recommendations, '\n\n',
        education_guidance, '\n\n',
        triage_recommendation
    ) as assistant_response,
    
    -- Ground truth label
    clinical_urgency_level as ground_truth_urgency,
    
    -- Individual components (for debugging/validation)
    patient_context,
    clinical_presentation,
    comorbidities,
    risk_assessment,
    symptom_inquiry,
    management_recommendations,
    education_guidance,
    triage_recommendation

FROM prompt_components;


select * from diabetes_training_prompts;



-- ==================================================================
-- PART C: TRAINING DATA QUALITY VALIDATION
-- Ensures data quality before fine-tuning
-- ==================================================================

-- ============================================================
-- VALIDATION 1: Completeness Checks
-- ============================================================

-- Check for NULL or empty prompts
SELECT 
    'NULL_CHECK' as validation_type,
    COUNT(*) as total_records,
    SUM(CASE WHEN user_prompt IS NULL OR user_prompt = '' THEN 1 ELSE 0 END) as null_user_prompts,
    SUM(CASE WHEN assistant_response IS NULL OR assistant_response = '' THEN 1 ELSE 0 END) as null_assistant_responses,
    SUM(CASE WHEN ground_truth_urgency IS NULL THEN 1 ELSE 0 END) as null_urgency_labels,
    ROUND(SUM(CASE WHEN user_prompt IS NULL OR user_prompt = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_null_user,
    ROUND(SUM(CASE WHEN assistant_response IS NULL OR assistant_response = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_null_assistant
FROM public.diabetes_training_prompts;

-- ============================================================
-- VALIDATION 2: Length Distribution (Token Analysis)
-- ============================================================

SELECT 
    'LENGTH_STATS' as validation_type,
    
    -- Character lengths
    ROUND(AVG(LENGTH(user_prompt)), 0) as avg_user_chars,
    ROUND(AVG(LENGTH(assistant_response)), 0) as avg_assistant_chars,
    MIN(LENGTH(user_prompt)) as min_user_chars,
    MAX(LENGTH(user_prompt)) as max_user_chars,
    MIN(LENGTH(assistant_response)) as min_assistant_chars,
    MAX(LENGTH(assistant_response)) as max_assistant_chars,
    
    -- Estimated tokens (1 token ≈ 4 chars)
    ROUND(AVG(LENGTH(user_prompt)) / 4, 0) as avg_user_tokens,
    ROUND(AVG(LENGTH(assistant_response)) / 4, 0) as avg_assistant_tokens,
    ROUND(AVG(LENGTH(user_prompt) + LENGTH(assistant_response)) / 4, 0) as avg_total_tokens,
    
    -- Flag if any exceed typical limits (2048 tokens = 8192 chars)
    SUM(CASE WHEN LENGTH(user_prompt) > 8192 THEN 1 ELSE 0 END) as user_exceeds_limit,
    SUM(CASE WHEN LENGTH(assistant_response) > 8192 THEN 1 ELSE 0 END) as assistant_exceeds_limit
    
FROM public.diabetes_training_prompts;

-- ============================================================
-- VALIDATION 3: Length Distribution by Urgency
-- ============================================================

SELECT 
    ground_truth_urgency,
    COUNT(*) as count,
    ROUND(AVG(LENGTH(user_prompt)), 0) as avg_user_chars,
    ROUND(AVG(LENGTH(assistant_response)), 0) as avg_assistant_chars,
    ROUND(AVG(LENGTH(user_prompt)) / 4, 0) as avg_user_tokens,
    ROUND(AVG(LENGTH(assistant_response)) / 4, 0) as avg_assistant_tokens
FROM public.diabetes_training_prompts
GROUP BY ground_truth_urgency
ORDER BY count DESC;

-- ============================================================
-- VALIDATION 4: Class Balance (Urgency Distribution)
-- ============================================================

SELECT 
    'CLASS_BALANCE' as validation_type,
    ground_truth_urgency,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    
    -- Flag severe imbalance (any class < 5% or > 80%)
    CASE 
        WHEN COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () < 5 THEN 'UNDERREPRESENTED'
        WHEN COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () > 80 THEN 'OVERREPRESENTED'
        ELSE 'BALANCED'
    END as balance_status
    
FROM public.diabetes_training_prompts
GROUP BY ground_truth_urgency
ORDER BY count DESC;

-- ============================================================
-- VALIDATION 5: Content Quality - Check for Placeholder Text
-- ============================================================

SELECT 
    'PLACEHOLDER_CHECK' as validation_type,
    SUM(CASE WHEN user_prompt LIKE '%null%' OR user_prompt LIKE '%NULL%' THEN 1 ELSE 0 END) as contains_null_text,
    SUM(CASE WHEN assistant_response LIKE '%null%' OR assistant_response LIKE '%NULL%' THEN 1 ELSE 0 END) as response_contains_null,
    SUM(CASE WHEN user_prompt LIKE '%undefined%' THEN 1 ELSE 0 END) as contains_undefined,
    SUM(CASE WHEN assistant_response LIKE '%[missing]%' THEN 1 ELSE 0 END) as contains_missing_flag,
    SUM(CASE WHEN user_prompt = '' OR LENGTH(user_prompt) < 50 THEN 1 ELSE 0 END) as suspiciously_short_user,
    SUM(CASE WHEN assistant_response = '' OR LENGTH(assistant_response) < 100 THEN 1 ELSE 0 END) as suspiciously_short_assistant
FROM public.diabetes_training_prompts;

-- ============================================================
-- VALIDATION 6: Duplicate Detection
-- ============================================================

WITH duplicates AS (
    SELECT 
        user_prompt,
        COUNT(*) as duplicate_count
    FROM public.diabetes_training_prompts
    GROUP BY user_prompt
    HAVING COUNT(*) > 1
)
SELECT 
    'DUPLICATE_CHECK' as validation_type,
    COUNT(*) as num_duplicate_prompts,
    SUM(duplicate_count) as total_duplicate_records,
    ROUND(SUM(duplicate_count) * 100.0 / (SELECT COUNT(*) FROM public.diabetes_training_prompts), 2) as pct_duplicates
FROM duplicates;

-- ============================================================
-- VALIDATION 7: Component Coverage Analysis
-- ============================================================

SELECT 
    'COMPONENT_COVERAGE' as validation_type,
    
    -- Check if all components are populated
    COUNT(*) as total_records,
    SUM(CASE WHEN patient_context IS NOT NULL AND LENGTH(patient_context) > 10 THEN 1 ELSE 0 END) as has_patient_context,
    SUM(CASE WHEN clinical_presentation IS NOT NULL AND LENGTH(clinical_presentation) > 20 THEN 1 ELSE 0 END) as has_clinical_presentation,
    SUM(CASE WHEN comorbidities IS NOT NULL AND LENGTH(comorbidities) > 10 THEN 1 ELSE 0 END) as has_comorbidities,
    SUM(CASE WHEN risk_assessment IS NOT NULL AND LENGTH(risk_assessment) > 20 THEN 1 ELSE 0 END) as has_risk_assessment,
    SUM(CASE WHEN symptom_inquiry IS NOT NULL AND LENGTH(symptom_inquiry) > 20 THEN 1 ELSE 0 END) as has_symptom_inquiry,
    SUM(CASE WHEN management_recommendations IS NOT NULL AND LENGTH(management_recommendations) > 20 THEN 1 ELSE 0 END) as has_management_recs,
    SUM(CASE WHEN education_guidance IS NOT NULL AND LENGTH(education_guidance) > 10 THEN 1 ELSE 0 END) as has_education,
    SUM(CASE WHEN triage_recommendation IS NOT NULL AND LENGTH(triage_recommendation) > 20 THEN 1 ELSE 0 END) as has_triage
    
FROM public.diabetes_training_prompts;

-- ============================================================
-- VALIDATION 8: Sample Quality Inspection
-- ============================================================

-- View 10 random samples for manual review
SELECT 
    patient_id,
    ground_truth_urgency,
    LENGTH(user_prompt) as user_len,
    LENGTH(assistant_response) as assistant_len,
    LEFT(user_prompt, 150) as user_preview,
    LEFT(assistant_response, 150) as assistant_preview
FROM public.diabetes_training_prompts
ORDER BY RANDOM()
LIMIT 10;

-- ============================================================
-- VALIDATION 9: Urgency Keyword Consistency Check
-- ============================================================

-- Verify that urgency labels match the content
SELECT 
    ground_truth_urgency,
    COUNT(*) as total,
    SUM(CASE WHEN assistant_response LIKE '%EMERGENCY%' THEN 1 ELSE 0 END) as contains_emergency_keyword,
    SUM(CASE WHEN assistant_response LIKE '%URGENT%' THEN 1 ELSE 0 END) as contains_urgent_keyword,
    SUM(CASE WHEN assistant_response LIKE '%ROUTINE%' THEN 1 ELSE 0 END) as contains_routine_keyword,
    
    -- Consistency ratio
    ROUND(
        SUM(CASE 
            WHEN ground_truth_urgency = 'emergency' AND assistant_response LIKE '%EMERGENCY%' THEN 1
            WHEN ground_truth_urgency = 'urgent' AND assistant_response LIKE '%URGENT%' THEN 1
            WHEN ground_truth_urgency = 'routine' AND assistant_response LIKE '%ROUTINE%' THEN 1
            ELSE 0
        END) * 100.0 / COUNT(*), 2
    ) as consistency_percentage
    
FROM public.diabetes_training_prompts
GROUP BY ground_truth_urgency;

-- ============================================================
-- VALIDATION 10: Final Summary Report
-- ============================================================

SELECT 
    '=== TRAINING DATA QUALITY SUMMARY ===' as report_section
UNION ALL
SELECT CONCAT('Total Training Examples: ', COUNT(*)) FROM public.diabetes_training_prompts
UNION ALL
SELECT CONCAT('Avg User Prompt Tokens: ', ROUND(AVG(LENGTH(user_prompt))/4, 0)) FROM public.diabetes_training_prompts
UNION ALL
SELECT CONCAT('Avg Assistant Tokens: ', ROUND(AVG(LENGTH(assistant_response))/4, 0)) FROM public.diabetes_training_prompts
UNION ALL
SELECT CONCAT('Emergency Cases: ', SUM(CASE WHEN ground_truth_urgency = 'emergency' THEN 1 ELSE 0 END), 
              ' (', ROUND(SUM(CASE WHEN ground_truth_urgency = 'emergency' THEN 1 ELSE 0 END)*100.0/COUNT(*), 1), '%)')
FROM public.diabetes_training_prompts
UNION ALL
SELECT CONCAT('Urgent Cases: ', SUM(CASE WHEN ground_truth_urgency = 'urgent' THEN 1 ELSE 0 END),
              ' (', ROUND(SUM(CASE WHEN ground_truth_urgency = 'urgent' THEN 1 ELSE 0 END)*100.0/COUNT(*), 1), '%)')
FROM public.diabetes_training_prompts
UNION ALL
SELECT CONCAT('Routine Cases: ', SUM(CASE WHEN ground_truth_urgency = 'routine' THEN 1 ELSE 0 END),
              ' (', ROUND(SUM(CASE WHEN ground_truth_urgency = 'routine' THEN 1 ELSE 0 END)*100.0/COUNT(*), 1), '%)')
FROM public.diabetes_training_prompts
UNION ALL
SELECT '✅ VALIDATION COMPLETE' as report_section;