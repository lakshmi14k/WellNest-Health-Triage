

USE DATABASE wellnest;
USE SCHEMA public;


CREATE OR REPLACE VIEW finetuning_hypertension_v1 AS
SELECT 
    'HTN_' || ROW_NUMBER() OVER (ORDER BY DBT_LOADED_AT) as patient_id,
    'hypertension' as condition_type,
    GENDER as gender,
    CASE 
        WHEN BMI < 18.5 THEN 'Underweight'
        WHEN BMI >= 18.5 AND BMI < 25 THEN 'Normal'
        WHEN BMI >= 25 AND BMI < 30 THEN 'Overweight'
        WHEN BMI >= 30 AND BMI < 35 THEN 'Obese Class I'
        WHEN BMI >= 35 AND BMI < 40 THEN 'Obese Class II'
        WHEN BMI >= 40 THEN 'Obese Class III'
        ELSE 'Unknown'
    END as bmi_category,
    SMOKING_STATUS_CLEAN as smoking_status,
    IS_CURRENT_SMOKER as is_current_smoker,
    IS_OBESE as is_obese,
    IS_SEDENTARY as is_sedentary,
    SYSTOLIC_BP as systolic_bp,
    DIASTOLIC_BP as diastolic_bp,
    SYSTOLIC_CATEGORY as systolic_category,
    DIASTOLIC_CATEGORY as diastolic_category,
    PULSE_PRESSURE as pulse_pressure,
    PULSE_PRESSURE_CATEGORY as pulse_pressure_category,
    HDL_CATEGORY as hdl_category,
    LDL_CATEGORY as ldl_category,
    TRIGLYCERIDES_CATEGORY as triglycerides_category,
    HAS_DIABETES as has_diabetes,
    TEN_YEAR_CV_RISK_CATEGORY as ten_year_cv_risk_category,
    METABOLIC_SYNDROME_CRITERIA_COUNT as metabolic_syndrome_score,
    STROKE_RISK_FACTORS_COUNT as stroke_risk_factors_count,
    POTENTIAL_HYPERTENSIVE_EMERGENCY as potential_hypertensive_emergency,
    MEDICATION_URGENCY as medication_urgency,
    CASE 
        WHEN POTENTIAL_HYPERTENSIVE_EMERGENCY = TRUE THEN 'emergency'
        WHEN MEDICATION_URGENCY IN ('urgent', 'immediate', 'high') THEN 'urgent'
        WHEN MEDICATION_URGENCY IN ('routine', 'monitor', 'low') THEN 'routine'
        ELSE 'needs_assessment'
    END AS clinical_urgency_level,
    PHYSICAL_ACTIVITY_LEVEL as physical_activity_level,
    SLEEP_DURATION as sleep_duration,
    SLEEP_ADEQUACY as sleep_adequacy,
    STRESS_LEVEL as stress_level,
    STRESS_CATEGORY as stress_category,
    SALT_INTAKE_RISK as salt_intake_risk,
    SHOULD_ASK_CRISIS_SYMPTOMS as should_ask_crisis_symptoms,
    SHOULD_ASK_CARDIOVASCULAR_SYMPTOMS as should_ask_cardiovascular_symptoms,
    SHOULD_ASK_STROKE_SYMPTOMS as should_ask_stroke_symptoms,
    SHOULD_ASK_KIDNEY_SYMPTOMS as should_ask_kidney_symptoms,
    SHOULD_ASK_HEADACHE_VISION as should_ask_headache_vision,
    SHOULD_ASK_CHEST_SYMPTOMS as should_ask_chest_symptoms,
    SHOULD_ASK_DIET_SODIUM as should_ask_diet_sodium,
    SHOULD_ASK_EXERCISE_HABITS as should_ask_exercise_habits,
    SHOULD_ASK_STRESS_SLEEP as should_ask_stress_sleep,
    SHOULD_ASK_ALCOHOL_USE as should_ask_alcohol_use,
    SHOULD_ASK_WEIGHT_MANAGEMENT as should_ask_weight_management,
    SHOULD_ASK_CURRENT_MEDICATIONS as should_ask_current_medications,
    SHOULD_ASK_MEDICATION_ADHERENCE as should_ask_medication_adherence,
    NEEDS_LIPID_MANAGEMENT_DISCUSSION as needs_lipid_management,
    NEEDS_SPECIALIST_REFERRAL_FLAG as needs_specialist_referral,
    SMOKING_CESSATION_PRIORITY_HTN as smoking_cessation_priority,
    WEIGHT_LOSS_PRIORITY as weight_management_priority,
    STRESS_MANAGEMENT_PRIORITY as stress_management_priority,
    PRIORITY_EDUCATION_TOPICS as education_topics,
    DBT_LOADED_AT as source_loaded_at,
    CURRENT_TIMESTAMP() as feature_selection_timestamp
FROM ftr_hypertension_conversation_prompts;


SELECT COUNT(*) as hypertension_count FROM finetuning_hypertension_v1;
SELECT * FROM finetuning_hypertension_v1;


-- STEP 1: Create training prompts view (ALL CORRECT COLUMN NAMES)
CREATE OR REPLACE VIEW public.hypertension_training_prompts AS

WITH base_features AS (
    SELECT * FROM public.finetuning_hypertension_v1
),

prompt_components AS (
    SELECT 
        patient_id,
        condition_type,
        clinical_urgency_level,
        gender,
        bmi_category,
        is_current_smoker,
        is_obese,
        is_sedentary,
        systolic_bp,
        diastolic_bp,
        systolic_category,
        diastolic_category,
        pulse_pressure,
        pulse_pressure_category,
        hdl_category,
        ldl_category,
        triglycerides_category,
        has_diabetes,
        ten_year_cv_risk_category,
        metabolic_syndrome_score,
        stroke_risk_factors_count,
        potential_hypertensive_emergency,
        medication_urgency,
        physical_activity_level,
        sleep_duration,
        sleep_adequacy,
        stress_level,
        stress_category,
        salt_intake_risk,
        should_ask_crisis_symptoms,
        should_ask_cardiovascular_symptoms,
        should_ask_stroke_symptoms,
        should_ask_headache_vision,
        should_ask_kidney_symptoms,
        should_ask_chest_symptoms,
        should_ask_diet_sodium,
        should_ask_exercise_habits,
        should_ask_stress_sleep,
        should_ask_alcohol_use,
        should_ask_weight_management,
        should_ask_current_medications,
        needs_lipid_management,
        needs_specialist_referral,
        smoking_cessation_priority,
        weight_management_priority,
        stress_management_priority,
        education_topics,
        
        -- Component 1: Patient Context
        CONCAT(
            'Patient is a ', gender,
            ' with BMI of ', bmi_category,
            CASE WHEN is_current_smoker THEN ', currently smokes' ELSE '' END,
            CASE WHEN is_obese THEN ', classified as obese' ELSE '' END,
            CASE WHEN is_sedentary THEN ', sedentary lifestyle' ELSE '' END,
            '.'
        ) as patient_context,
        
        -- Component 2: Clinical Presentation
        CONCAT(
            'Clinical findings: ',
            'Blood pressure: ', systolic_bp, '/', diastolic_bp, ' mmHg (',
            systolic_category, '/', diastolic_category, '). ',
            'Pulse pressure: ', pulse_pressure, ' mmHg (', pulse_pressure_category, '). ',
            CASE WHEN has_diabetes THEN 'Confirmed diabetes diagnosis. ' ELSE '' END,
            CASE 
                WHEN potential_hypertensive_emergency 
                THEN 'HYPERTENSIVE EMERGENCY CRITERIA MET. '
                ELSE ''
            END
        ) as clinical_presentation,
        
        -- Component 3: Comorbidities & Risk Factors
        CONCAT(
            'Comorbidities and risk factors: ',
            CASE WHEN has_diabetes THEN 'Type 2 diabetes, ' ELSE '' END,
            'Metabolic syndrome score: ', metabolic_syndrome_score, '. ',
            'Stroke risk factors count: ', stroke_risk_factors_count, '. ',
            '10-year cardiovascular risk: ', ten_year_cv_risk_category, '. ',
            'Lipid profile: HDL ', hdl_category, ', LDL ', ldl_category, 
            ', Triglycerides ', triglycerides_category, '.'
        ) as comorbidities,
        
        -- Component 4: Lifestyle Factors
        CONCAT(
            'Lifestyle assessment: ',
            'Physical activity: ', physical_activity_level, '. ',
            'Sleep: ', sleep_duration, ' hours/night (', sleep_adequacy, '). ',
            'Stress level: ', stress_level, '/10 (', stress_category, '). ',
            'Sodium intake risk: ', salt_intake_risk, '.'
        ) as lifestyle_factors,
        
        -- Component 5: Urgency Note
        CASE 
            WHEN potential_hypertensive_emergency 
            THEN 'URGENT: Blood pressure indicates potential hypertensive emergency. Immediate medical evaluation required.'
            WHEN medication_urgency = 'immediate'
            THEN 'URGENT: Blood pressure requires immediate medical attention and likely medication initiation.'
            WHEN medication_urgency = 'urgent'
            THEN 'Elevated blood pressure requiring prompt medical follow-up within 1-2 weeks.'
            ELSE 'Blood pressure at routine monitoring level.'
        END as urgency_note,
        
        -- Component 6: Risk Assessment
        CONCAT(
            'Risk assessment: ',
            '10-year cardiovascular risk: ', ten_year_cv_risk_category, '. ',
            'Metabolic syndrome score: ', metabolic_syndrome_score, '. ',
            'Stroke risk factors: ', stroke_risk_factors_count, '.'
        ) as risk_assessment,
        
        -- Component 7: Symptom Inquiry
        CONCAT(
            'Key symptoms to assess: ',
            CASE WHEN should_ask_crisis_symptoms 
                THEN 'PRIORITY - Severe headache, vision changes, chest pain, confusion (hypertensive emergency signs). ' 
                ELSE '' END,
            CASE WHEN should_ask_cardiovascular_symptoms 
                THEN 'Cardiovascular - chest pain, palpitations, shortness of breath. ' 
                ELSE '' END,
            CASE WHEN should_ask_stroke_symptoms 
                THEN 'Stroke warning signs - sudden weakness, numbness, speech difficulty, facial drooping. ' 
                ELSE '' END,
            CASE WHEN should_ask_headache_vision 
                THEN 'Persistent headaches, blurred vision. ' 
                ELSE '' END,
            CASE WHEN should_ask_kidney_symptoms 
                THEN 'Kidney symptoms - changes in urination, leg swelling. ' 
                ELSE '' END,
            CASE WHEN should_ask_chest_symptoms 
                THEN 'Chest discomfort, palpitations. ' 
                ELSE '' END
        ) as symptom_inquiry,
        
        -- Component 8: Lifestyle Assessment Areas
        CONCAT(
            'Lifestyle areas to assess: ',
            CASE WHEN should_ask_diet_sodium THEN 'sodium intake and dietary patterns, ' ELSE '' END,
            CASE WHEN should_ask_exercise_habits THEN 'physical activity level and exercise routine, ' ELSE '' END,
            CASE WHEN should_ask_stress_sleep THEN 'stress levels and sleep quality, ' ELSE '' END,
            CASE WHEN should_ask_alcohol_use THEN 'alcohol consumption patterns, ' ELSE '' END,
            CASE WHEN should_ask_weight_management THEN 'weight management goals and challenges' ELSE '' END
        ) as lifestyle_assessment,
        
        -- Component 9: Management Recommendations
        CONCAT(
            'Recommended management approach: ',
            CASE 
                WHEN medication_urgency = 'immediate'
                THEN 'IMMEDIATE - Antihypertensive medication needed urgently. Consider emergency department evaluation. '
                WHEN medication_urgency = 'urgent'
                THEN 'URGENT - Schedule appointment within 24-48 hours. Antihypertensive therapy likely needed. '
                WHEN medication_urgency = 'prompt'
                THEN 'Prompt medical follow-up recommended. Medication discussion appropriate. '
                WHEN medication_urgency = 'consider'
                THEN 'Consider medication if lifestyle modifications insufficient. '
                ELSE 'Lifestyle modifications as first-line approach. '
            END,
            
            CASE WHEN should_ask_current_medications THEN 'Review current medications and adherence. ' ELSE '' END,
            CASE WHEN needs_lipid_management THEN 'Lipid management discussion needed. ' ELSE '' END,
            
            'Lifestyle priorities: ',
            CASE 
                WHEN smoking_cessation_priority IN ('critical', 'high')
                THEN 'HIGH PRIORITY - Smoking cessation counseling. '
                WHEN smoking_cessation_priority = 'moderate'
                THEN 'Smoking cessation support. '
                ELSE ''
            END,
            CASE 
                WHEN weight_management_priority IN ('critical', 'high')
                THEN 'HIGH PRIORITY - Weight management intervention (target 5-10% reduction). '
                WHEN weight_management_priority = 'moderate'
                THEN 'Weight management support. '
                ELSE ''
            END,
            CASE 
                WHEN stress_management_priority IN ('high', 'moderate')
                THEN 'Stress reduction techniques. '
                ELSE ''
            END,
            
            CASE WHEN needs_specialist_referral THEN 'Cardiology referral recommended. ' ELSE '' END
        ) as management_recommendations,
        
        -- Component 10: Patient Education
        CONCAT(
            'Priority education topics: ',
            COALESCE(education_topics, 'General hypertension management'), '. ',
            'Key messages: DASH diet (rich in fruits, vegetables, whole grains), ',
            'sodium restriction <2000mg/day, 150 minutes weekly moderate exercise, ',
            'home blood pressure monitoring, medication adherence if prescribed.'
        ) as education_guidance,
        
        -- Component 11: Triage Recommendation
        CASE 
            WHEN clinical_urgency_level = 'emergency' 
            THEN 'TRIAGE: IMMEDIATE EMERGENCY evaluation required. Direct to the emergency department immediately. Do not delay. Patient may have hypertensive emergency requiring IV medications and intensive monitoring.'
            
            WHEN clinical_urgency_level = 'urgent'
            THEN 'TRIAGE: URGENT medical evaluation within 24-48 hours. Schedule appointment promptly. Begin home blood pressure monitoring. Provide interim lifestyle guidance. Patient likely needs antihypertensive medication.'
            
            WHEN clinical_urgency_level = 'routine'
            THEN 'TRIAGE: ROUTINE follow-up appropriate. Schedule regular monitoring appointment. Focus on lifestyle modifications. Home blood pressure tracking recommended. Re-evaluate in 1-3 months.'
            
            ELSE 'TRIAGE: Further assessment needed to determine appropriate care level.'
        END as triage_recommendation
        
    FROM base_features
)

SELECT 
    patient_id,
    condition_type,
    clinical_urgency_level,
    
    -- User Prompt (what patient presents with)
    CONCAT(
        patient_context, ' ',
        clinical_presentation, ' ',
        comorbidities, ' ',
        lifestyle_factors, ' ',
        urgency_note
    ) as user_prompt,
    
    -- Assistant Response (what AI should say)
    CONCAT(
        risk_assessment, '\n\n',
        symptom_inquiry, '\n\n',
        lifestyle_assessment, '\n\n',
        management_recommendations, '\n\n',
        education_guidance, '\n\n',
        triage_recommendation
    ) as assistant_response,
    
    -- Ground truth
    clinical_urgency_level as ground_truth_urgency,
    
    -- Individual components for debugging
    patient_context,
    clinical_presentation,
    comorbidities,
    lifestyle_factors,
    risk_assessment,
    symptom_inquiry,
    lifestyle_assessment,
    management_recommendations,
    education_guidance,
    triage_recommendation,
    
    -- Original fields
    systolic_bp,
    diastolic_bp,
    medication_urgency,
    potential_hypertensive_emergency

FROM prompt_components;


--Creating the training and validation datasets

-- Training set (80%)
CREATE OR REPLACE TABLE public.hypertension_training_set AS
WITH base_data AS (
    SELECT * FROM public.hypertension_training_prompts
    WHERE MOD(ABS(HASH(patient_id)), 10) < 8
)
SELECT 
    patient_id,
    ground_truth_urgency,
    
    -- Prompt: System message + User case
    CONCAT(
        'You are a medical AI assistant specializing in hypertension and cardiovascular disease management. ',
        'Your role is to assess patient presentations, evaluate urgency (Emergency/Urgent/Routine), ',
        'identify key symptoms for inquiry, provide evidence-based management recommendations, ',
        'and suggest appropriate patient education. You must prioritize patient safety, ',
        'detect hypertensive emergencies (BP ≥180/120 with symptoms), provide clear triage recommendations ',
        'based on ACC/AHA guidelines, and recommend specialist referral when appropriate. ',
        'You serve as a clinical decision support tool, not a replacement for medical diagnosis.\n\n',
        'Patient Case:\n',
        user_prompt
    ) as prompt,
    
    -- Completion: Expected AI response
    assistant_response as completion
    
FROM base_data;

-- Validation set (20%)
CREATE OR REPLACE TABLE public.hypertension_validation_set AS
WITH base_data AS (
    SELECT * FROM public.hypertension_training_prompts
    WHERE MOD(ABS(HASH(patient_id)), 10) >= 8
)
SELECT 
    patient_id,
    ground_truth_urgency,
    
    CONCAT(
        'You are a medical AI assistant specializing in hypertension and cardiovascular disease management. ',
        'Your role is to assess patient presentations, evaluate urgency (Emergency/Urgent/Routine), ',
        'identify key symptoms for inquiry, provide evidence-based management recommendations, ',
        'and suggest appropriate patient education. You must prioritize patient safety, ',
        'detect hypertensive emergencies (BP ≥180/120 with symptoms), provide clear triage recommendations ',
        'based on ACC/AHA guidelines, and recommend specialist referral when appropriate.\n\n',
        'Patient Case:\n',
        user_prompt
    ) as prompt,
    
    assistant_response as completion
    
FROM base_data;

-- Verify format
SELECT 
    patient_id,
    ground_truth_urgency,
    LEFT(prompt, 200) as prompt_preview,
    LEFT(completion, 200) as completion_preview
FROM public.hypertension_training_set
LIMIT 5;

-- Check counts
SELECT 
    'Training' as dataset, 
    COUNT(*) as count,
    COUNT(*) * 3 as total_steps_3epochs,
    CASE 
        WHEN COUNT(*) * 3 <= 50000 THEN '✅ Under limit' 
        ELSE '❌ Over limit' 
    END as status
FROM public.hypertension_training_set
UNION ALL
SELECT 
    'Validation', 
    COUNT(*),
    NULL,
    '✅ OK'
FROM public.hypertension_validation_set;


-- ==================================================================
-- STEP 1: Check urgency distribution in full dataset
-- ==================================================================
SELECT 
    ground_truth_urgency,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM public.hypertension_training_set
GROUP BY ground_truth_urgency
ORDER BY count DESC;

-- ==================================================================
-- STEP 2: Create training set with ~16,000 rows (stratified)
-- For 3 epochs: 16,000 × 3 = 48,000 steps ✅
-- ==================================================================

CREATE OR REPLACE TABLE public.hypertension_training_16k AS
WITH ranked_data AS (
    SELECT 
        prompt,
        completion,
        patient_id,
        ground_truth_urgency,
        -- Random ranking within each urgency class
        ROW_NUMBER() OVER (
            PARTITION BY ground_truth_urgency 
            ORDER BY RANDOM()
        ) as rn
    FROM public.hypertension_training_set
),
class_targets AS (
    -- Calculate proportional sample size for each class
    SELECT 
        ground_truth_urgency,
        COUNT(*) as original_count,
        ROUND(16000.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 0) as target_sample_size
    FROM public.hypertension_training_set
    GROUP BY ground_truth_urgency
)
SELECT 
    r.prompt,
    r.completion,
    r.patient_id,
    r.ground_truth_urgency
FROM ranked_data r
JOIN class_targets t ON r.ground_truth_urgency = t.ground_truth_urgency
WHERE r.rn <= t.target_sample_size
ORDER BY RANDOM();

-- ==================================================================
-- STEP 3: Create validation set with ~3,000 rows
-- ==================================================================

CREATE OR REPLACE TABLE public.hypertension_validation_3k AS
SELECT 
    prompt,
    completion,
    patient_id,
    ground_truth_urgency
FROM public.hypertension_validation_set
ORDER BY RANDOM()
LIMIT 3000;

-- ==================================================================
-- STEP 4: Verify the counts and balance
-- ==================================================================

-- Check training set
SELECT 
    'Training Set' as dataset,
    COUNT(*) as row_count,
    COUNT(*) * 3 as total_steps_3epochs,
    CASE 
        WHEN COUNT(*) * 3 <= 50000 THEN '✅ Under limit' 
        ELSE '❌ Over limit' 
    END as status
FROM public.hypertension_training_16k
UNION ALL
SELECT 
    'Validation Set',
    COUNT(*),
    NULL,
    '✅ OK'
FROM public.hypertension_validation_3k;

-- Check class distribution in training set
SELECT 
    ground_truth_urgency,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM public.hypertension_training_16k
GROUP BY ground_truth_urgency
ORDER BY count DESC;

-- Check class distribution in validation set
SELECT 
    ground_truth_urgency,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM public.hypertension_validation_3k
GROUP BY ground_truth_urgency
ORDER BY count DESC;

-- ==================================================================
-- STEP 5: Verify data quality (sample inspection)
-- ==================================================================

SELECT 
    patient_id,
    ground_truth_urgency,
    LEFT(prompt, 150) as prompt_preview,
    LEFT(completion, 150) as completion_preview
FROM public.hypertension_training_16k
LIMIT 5;


-- Start fine-tuning with default 3 epochs
SELECT SNOWFLAKE.CORTEX.FINETUNE(
    'CREATE',
    'hypertension_llm_16k',
    'llama3.1-8b',
    'SELECT prompt, completion FROM public.hypertension_training_16k',
    'SELECT prompt, completion FROM public.hypertension_validation_3k'
) AS job_result;

-- Check status (run periodically)
SELECT SNOWFLAKE.CORTEX.FINETUNE(
    'DESCRIBE',
    'ft_a59708b4-6c81-4a3a-be29-9dd08938920d'
) AS training_status;


SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'hypertension_llm_16k',
    'i Hvae acute heart pains and sleepnessnes. I have high BP. How do I reduce this '
) AS response;
