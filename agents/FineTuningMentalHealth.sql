-- ==================================================================
-- MENTAL HEALTH LLM FINE-TUNING FRAMEWORK
-- Source: FTR_MENTAL_HEALTH_CONVERSATION_PROMPTS
-- Pattern: Following hypertension_llm_16k structure
-- ==================================================================
USE DATABASE wellnest;
USE SCHEMA public;

-- ==================================================================
-- STEP 1: Create feature selection view
-- ==================================================================

CREATE OR REPLACE VIEW finetuning_mental_health_v1 AS
SELECT 
    'MH_' || ROW_NUMBER() OVER (ORDER BY survey_timestamp) as patient_id,
    'mental_health' as condition_type,
    
    -- Demographics
    gender_clean as gender,
    age_category,
    occupation_category,
    
    -- Core Mental Health Features
    depression_severity,
    depression_risk_score,
    anxiety_severity,
    anxiety_risk_score,
    has_comorbid_depression_anxiety,
    
    -- Severity Indicators
    symptom_count,
    severe_symptom_count,
    mental_health_urgency_level,
    
    -- Functional Assessment
    functional_impairment_category,
    functional_impairment_score,
    work_motivation_status,
    social_functioning_status,
    
    -- Behavioral Indicators
    social_isolation_level,
    isolation_severity_score,
    behavioral_change_status,
    mood_swing_severity,
    stress_level,
    
    -- History & Treatment
    has_mental_health_history,
    has_family_mental_health_history,
    in_active_treatment,
    has_treatment_gap,
    care_awareness_status,
    
    -- Safety Flags (CRITICAL)
    needs_suicide_risk_screening,
    
    -- Symptom Inquiry Flags
    should_ask_depressive_symptoms,
    should_ask_anxiety_symptoms,
    should_ask_sleep_patterns,
    should_ask_appetite_changes,
    should_ask_concentration,
    should_ask_energy_levels,
    
    -- Safety Assessment Flags
    should_screen_suicidal_ideation,
    should_assess_selfharm_history,
    should_evaluate_safety_plan,
    
    -- Social & Relationship Flags
    should_ask_social_support,
    should_ask_relationships,
    should_ask_family_dynamics,
    
    -- Functional Inquiry Flags
    should_ask_work_functioning,
    should_ask_daily_activities,
    should_ask_motivation,
    
    -- Lifestyle & Coping Flags
    should_ask_physical_activity,
    should_ask_substance_use,
    should_ask_coping_strategies,
    should_ask_hobbies_interests,
    
    -- Treatment History Flags
    should_ask_past_treatment,
    should_ask_current_medications,
    should_ask_therapy_experience,
    should_ask_treatment_satisfaction,
    
    -- Barriers to Care Flags
    should_ask_care_access,
    should_ask_financial_barriers,
    should_ask_stigma_concerns,
    
    -- Specific Condition Exploration
    should_explore_panic_symptoms,
    should_explore_social_anxiety,
    should_explore_trauma_history,
    should_explore_mood_episodes,
    
    -- Referral Flags
    needs_crisis_referral,
    needs_psychiatrist_referral,
    needs_therapist_referral,
    needs_support_group_referral,
    needs_workplace_accommodation,
    
    -- Treatment Priorities
    therapy_priority,
    medication_evaluation_priority,
    case_complexity_score,
    
    -- Education & Communication
    priority_education_topics,
    selfcare_focus_areas,
    recommended_therapy_modalities,
    communication_tone,
    recommended_followup_timeframe,
    treatment_plan_complexity,
    monitoring_intensity_needed,
    conversation_starting_point,
    primary_clinical_concern,
    
    -- Metadata
    survey_timestamp as source_timestamp,
    CURRENT_TIMESTAMP() as feature_selection_timestamp
    
FROM ftr_mental_health_conversation_prompts;


-- Verify data count
SELECT COUNT(*) as mental_health_count FROM finetuning_mental_health_v1;

-- Check urgency distribution
SELECT 
    mental_health_urgency_level,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM finetuning_mental_health_v1
GROUP BY mental_health_urgency_level
ORDER BY count DESC;


-- ==================================================================
-- STEP 2: Create training prompts view with detailed prompt engineering
-- ==================================================================

-- ==================================================================
-- COMPLETELY REVISED: Mental Health Training Prompts (NULL-PROOF)
-- ==================================================================

CREATE OR REPLACE VIEW public.mental_health_training_prompts AS

WITH base_features AS (
    SELECT * FROM public.finetuning_mental_health_v1
),

prompt_components AS (
    SELECT 
        patient_id,
        condition_type,
        
        -- Key fields with NULL handling
        COALESCE(mental_health_urgency_level, 'needs_assessment') as urgency_level,
        COALESCE(gender, 'unspecified') as gender,
        COALESCE(age_category, 'adult') as age_category,
        COALESCE(occupation_category, 'not_specified') as occupation,
        COALESCE(depression_severity, 'not_assessed') as depression_severity,
        COALESCE(TO_VARCHAR(depression_risk_score), '0') as depression_score,
        COALESCE(anxiety_severity, 'not_assessed') as anxiety_severity,
        COALESCE(TO_VARCHAR(anxiety_risk_score), '0') as anxiety_score,
        COALESCE(functional_impairment_category, 'not_assessed') as impairment_level,
        COALESCE(social_isolation_level, 'unknown') as isolation_level,
        COALESCE(TO_VARCHAR(symptom_count), '0') as symptom_count,
        COALESCE(TO_VARCHAR(severe_symptom_count), '0') as severe_symptoms,
        
        -- Boolean flags
        COALESCE(has_mental_health_history, FALSE) as has_mh_history,
        COALESCE(in_active_treatment, FALSE) as in_treatment,
        COALESCE(has_treatment_gap, FALSE) as treatment_gap,
        COALESCE(needs_suicide_risk_screening, FALSE) as suicide_screening,
        COALESCE(has_comorbid_depression_anxiety, FALSE) as comorbid,
        
        -- User Prompt Components (ALWAYS NON-NULL)
        CONCAT(
            'Patient Demographics: ',
            COALESCE(gender, 'unspecified'), ' in ', 
            COALESCE(age_category, 'adult'), ' age group, ',
            'occupation: ', COALESCE(occupation_category, 'not specified'), '.\n\n',
            
            'Mental Health History: ',
            CASE WHEN COALESCE(has_mental_health_history, FALSE) 
                THEN 'Documented mental health history present. ' 
                ELSE 'No documented history. ' 
            END,
            CASE WHEN COALESCE(in_active_treatment, FALSE)
                THEN 'Currently in treatment. '
                WHEN COALESCE(has_treatment_gap, FALSE)
                THEN 'Treatment gap - previously treated but not currently. '
                ELSE 'Not currently in treatment. '
            END, '\n\n',
            
            'Current Presentation:\n',
            '• Depression: ', COALESCE(depression_severity, 'not assessed'), 
            ' (score: ', COALESCE(TO_VARCHAR(depression_risk_score), '0'), ')\n',
            '• Anxiety: ', COALESCE(anxiety_severity, 'not assessed'),
            ' (score: ', COALESCE(TO_VARCHAR(anxiety_risk_score), '0'), ')\n',
            '• Total symptom count: ', COALESCE(TO_VARCHAR(symptom_count), '0'), '\n',
            '• Severe symptoms: ', COALESCE(TO_VARCHAR(severe_symptom_count), '0'), '\n',
            '• Functional impairment: ', COALESCE(functional_impairment_category, 'not assessed'), '\n',
            '• Social isolation: ', COALESCE(social_isolation_level, 'unknown'), '\n',
            
            CASE WHEN COALESCE(has_comorbid_depression_anxiety, FALSE)
                THEN '• Comorbid depression and anxiety present\n'
                ELSE ''
            END,
            
            '\n',
            CASE WHEN COALESCE(needs_suicide_risk_screening, FALSE)
                THEN '⚠️ SAFETY ALERT: Suicide risk screening criteria met. Direct assessment required.\n'
                ELSE ''
            END
        ) as user_prompt_text,
        
        -- Assistant Response Components (ALWAYS NON-NULL - simplified)
        CONCAT(
            '=== CLINICAL ASSESSMENT ===\n\n',
            
            'URGENCY LEVEL: ', UPPER(COALESCE(mental_health_urgency_level, 'NEEDS_ASSESSMENT')), '\n',
            CASE COALESCE(mental_health_urgency_level, 'routine')
                WHEN 'emergency' THEN 
                    '• IMMEDIATE ACTION: Call 988 Suicide & Crisis Lifeline or 911\n' ||
                    '• Go to nearest emergency department\n' ||
                    '• Do not leave person alone\n' ||
                    '• Emergency psychiatric evaluation required TODAY\n'
                WHEN 'urgent' THEN
                    '• Schedule psychiatric evaluation within 24-48 hours\n' ||
                    '• Crisis resources: 988 available 24/7\n' ||
                    '• Close monitoring recommended\n'
                WHEN 'needs_attention' THEN
                    '• Schedule mental health evaluation within 1-2 weeks\n' ||
                    '• Consider therapy referral\n' ||
                    '• Monitor symptoms for changes\n'
                ELSE
                    '• Routine mental health support appropriate\n' ||
                    '• Preventive counseling if desired\n' ||
                    '• Focus on self-care and coping skills\n'
            END, '\n\n',
            
            '=== SYMPTOM ASSESSMENT ===\n\n',
            'Depression Evaluation:\n',
            '• Severity: ', COALESCE(depression_severity, 'not assessed'), '\n',
            '• Risk score: ', COALESCE(TO_VARCHAR(depression_risk_score), '0'), '/27 (PHQ-9 style)\n',
            '• Key symptoms to assess: low mood, anhedonia, sleep changes, appetite changes, energy, concentration, worthlessness/guilt, psychomotor changes\n\n',
            
            'Anxiety Evaluation:\n',
            '• Severity: ', COALESCE(anxiety_severity, 'not assessed'), '\n',
            '• Risk score: ', COALESCE(TO_VARCHAR(anxiety_risk_score), '0'), '/21 (GAD-7 style)\n',
            '• Key symptoms to assess: excessive worry, restlessness, fatigue, concentration difficulty, irritability, muscle tension, sleep disturbance\n\n',
            
            CASE WHEN COALESCE(needs_suicide_risk_screening, FALSE)
                THEN 
                    '🚨 SAFETY ASSESSMENT REQUIRED:\n' ||
                    '• Directly ask: "Are you having thoughts of harming yourself or ending your life?"\n' ||
                    '• Assess: ideation, plan, intent, means, protective factors\n' ||
                    '• Use Columbia Suicide Severity Rating Scale framework\n' ||
                    '• If positive: immediate safety planning and crisis intervention\n' ||
                    '• Crisis resources: 988 Lifeline (call/text), Crisis Text Line (HOME to 741741)\n\n'
                ELSE ''
            END,
            
            '=== FUNCTIONAL IMPACT ===\n\n',
            '• Functional impairment: ', COALESCE(functional_impairment_category, 'not assessed'), '\n',
            '• Social functioning: ', COALESCE(social_isolation_level, 'unknown'), '\n',
            '• Areas to evaluate:\n',
            '  - Work/school performance and attendance\n',
            '  - Daily activities and self-care\n',
            '  - Social relationships and support network\n',
            '  - Physical activity and hobbies\n\n',
            
            '=== TREATMENT RECOMMENDATIONS ===\n\n',
            CASE 
                WHEN COALESCE(depression_risk_score, 0) >= 15 OR COALESCE(anxiety_risk_score, 0) >= 15
                THEN '1. THERAPY: High priority - immediate referral recommended\n' ||
                     '   • Cognitive Behavioral Therapy (CBT)\n' ||
                     '   • If available: schedule within 1 week\n\n' ||
                     '2. MEDICATION: Psychiatric evaluation recommended\n' ||
                     '   • Consider SSRI/SNRI for moderate-severe symptoms\n' ||
                     '   • Refer to psychiatrist or psychiatric NP\n\n'
                WHEN COALESCE(depression_risk_score, 0) >= 10 OR COALESCE(anxiety_risk_score, 0) >= 10
                THEN '1. THERAPY: Recommended - schedule within 2-3 weeks\n' ||
                     '   • CBT, Acceptance and Commitment Therapy (ACT), or Interpersonal Therapy\n\n' ||
                     '2. MEDICATION: Consider if symptoms persist despite therapy\n\n'
                WHEN COALESCE(symptom_count, 0) >= 3
                THEN '1. THERAPY: Consider counseling for symptom management\n' ||
                     '   • Supportive therapy or brief intervention\n\n' ||
                     '2. MEDICATION: Not indicated at this time, focus on therapy first\n\n'
                ELSE '1. PREVENTIVE SUPPORT: Mental health education and self-care\n' ||
                     '2. MONITOR: Track symptoms over time\n\n'
            END,
            
            '3. PSYCHOSOCIAL INTERVENTIONS:\n',
            '   • Increase social support and connection\n',
            '   • Address barriers to care (cost, stigma, access)\n',
            '   • Workplace accommodations if needed\n',
            '   • Support groups or peer support\n\n',
            
            '=== PATIENT EDUCATION ===\n\n',
            'Self-Care Strategies:\n',
            '• Sleep: Maintain consistent 7-9 hour schedule\n',
            '• Exercise: 30 minutes of activity 5x/week (proven effective for mild-moderate depression/anxiety)\n',
            '• Nutrition: Regular meals, limit caffeine/alcohol\n',
            '• Stress management: Relaxation techniques, mindfulness, deep breathing\n',
            '• Social connection: Reach out to friends/family, avoid isolation\n',
            '• Pleasant activities: Schedule enjoyable activities daily\n',
            '• Limit substances: Alcohol can worsen depression/anxiety\n\n',
            
            'When to Seek Immediate Help:\n',
            '• Thoughts of self-harm or suicide\n',
            '• Severe symptoms interfering with daily functioning\n',
            '• Symptoms rapidly worsening\n',
            '• Unable to care for self or dependents\n\n',
            
            'Crisis Resources:\n',
            '• 988 Suicide & Crisis Lifeline (24/7 call/text)\n',
            '• Crisis Text Line: Text HOME to 741741\n',
            '• Emergency: 911\n',
            '• SAMHSA National Helpline: 1-800-662-4357\n\n',
            
            '=== FOLLOW-UP PLAN ===\n\n',
            CASE COALESCE(mental_health_urgency_level, 'routine')
                WHEN 'emergency' THEN 'IMMEDIATE: Emergency evaluation today, then daily monitoring'
                WHEN 'urgent' THEN 'Within 48 hours: Psychiatric appointment, then weekly follow-up'
                WHEN 'needs_attention' THEN 'Within 1-2 weeks: Mental health evaluation, then bi-weekly monitoring'
                ELSE 'As needed: Routine check-ins, self-monitoring of symptoms'
            END, '\n\n',
            
            'DISPOSITION: ',
            CASE COALESCE(mental_health_urgency_level, 'routine')
                WHEN 'emergency' THEN 'Emergency department or mobile crisis team'
                WHEN 'urgent' THEN 'Urgent outpatient psychiatric appointment'
                WHEN 'needs_attention' THEN 'Outpatient mental health services'
                ELSE 'Self-care with optional counseling support'
            END
        ) as assistant_response_text
        
    FROM base_features
)

SELECT 
    patient_id,
    condition_type,
    urgency_level as mental_health_urgency_level,
    
    -- User prompt (patient presentation)
    user_prompt_text as user_prompt,
    
    -- Assistant response (clinical guidance)
    assistant_response_text as assistant_response,
    
    -- Ground truth
    urgency_level as ground_truth_urgency,
    
    -- Metadata for filtering/analysis
    depression_severity,
    anxiety_severity,
    suicide_screening as needs_suicide_risk_screening,
    depression_score as depression_risk_score,
    anxiety_score as anxiety_risk_score,
    symptom_count,
    severe_symptoms as severe_symptom_count

FROM prompt_components;


-- ==================================================================
-- VERIFY: This should show NO NULLS
-- ==================================================================

SELECT 
    COUNT(*) as total_rows,
    COUNT(user_prompt) as has_user_prompt,
    COUNT(assistant_response) as has_assistant_response,
    COUNT(CASE WHEN assistant_response IS NULL THEN 1 END) as null_responses,
    COUNT(CASE WHEN LENGTH(assistant_response) < 100 THEN 1 END) as suspiciously_short,
    MIN(LENGTH(assistant_response)) as min_length,
    AVG(LENGTH(assistant_response)) as avg_length,
    MAX(LENGTH(assistant_response)) as max_length
FROM mental_health_training_prompts;

-- Should show: total_rows = has_assistant_response, null_responses = 0

-- Sample the data
SELECT 
    patient_id,
    ground_truth_urgency,
    LENGTH(user_prompt) as prompt_len,
    LENGTH(assistant_response) as response_len,
    LEFT(assistant_response, 200) as response_preview
FROM mental_health_training_prompts
LIMIT 10;

-- ==================================================================
-- Recreate training/validation sets with fixed prompts
-- ==================================================================

-- Training set (80%)
CREATE OR REPLACE TABLE public.mental_health_training_set AS
WITH base_data AS (
    SELECT * FROM public.mental_health_training_prompts
    WHERE MOD(ABS(HASH(patient_id)), 10) < 8
)
SELECT 
    patient_id,
    ground_truth_urgency,
    
    CONCAT(
        'You are a compassionate mental health AI assistant specializing in depression, anxiety, and psychiatric triage. ',
        'Your role is to assess mental health presentations, evaluate urgency (Emergency/Urgent/Needs_Attention/Routine), ',
        'conduct safety assessments for suicide risk when indicated, identify key symptoms, ',
        'provide evidence-based treatment recommendations, and suggest appropriate referrals. ',
        'CRITICAL: If suicide risk screening is indicated, directly and compassionately ask about suicidal thoughts. ',
        'Use non-judgmental, validating language that acknowledges suffering while instilling hope. ',
        'Follow APA guidelines for depression and anxiety management.\n\n',
        'Patient Presentation:\n',
        user_prompt
    ) as prompt,
    
    assistant_response as completion
    
FROM base_data;

-- Validation set (20%)
CREATE OR REPLACE TABLE public.mental_health_validation_set AS
WITH base_data AS (
    SELECT * FROM public.mental_health_training_prompts
    WHERE MOD(ABS(HASH(patient_id)), 10) >= 8
)
SELECT 
    patient_id,
    ground_truth_urgency,
    
    CONCAT(
        'You are a compassionate mental health AI assistant specializing in depression, anxiety, and psychiatric triage. ',
        'Assess urgency, conduct safety screening, and provide evidence-based recommendations.\n\n',
        'Patient Presentation:\n',
        user_prompt
    ) as prompt,
    
    assistant_response as completion
    
FROM base_data;

-- Stratified 16k training
CREATE OR REPLACE TABLE public.mental_health_training_16k AS
WITH ranked_data AS (
    SELECT 
        prompt,
        completion,
        patient_id,
        ground_truth_urgency,
        ROW_NUMBER() OVER (
            PARTITION BY ground_truth_urgency 
            ORDER BY RANDOM()
        ) as rn
    FROM public.mental_health_training_set
),
class_targets AS (
    SELECT 
        ground_truth_urgency,
        COUNT(*) as original_count,
        ROUND(16000.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 0) as target_sample_size
    FROM public.mental_health_training_set
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

-- 3k validation
CREATE OR REPLACE TABLE public.mental_health_validation_3k AS
SELECT 
    prompt,
    completion,
    patient_id,
    ground_truth_urgency
FROM public.mental_health_validation_set
ORDER BY RANDOM()
LIMIT 3000;

-- FINAL VERIFICATION - CRITICAL CHECK
SELECT 
    'Training 16k' as dataset,
    COUNT(*) as total_rows,
    COUNT(completion) as has_completion,
    COUNT(CASE WHEN completion IS NULL THEN 1 END) as null_count,
    COUNT(CASE WHEN LENGTH(COALESCE(completion, '')) = 0 THEN 1 END) as empty_count,
    MIN(LENGTH(completion)) as min_length,
    AVG(LENGTH(completion)) as avg_length
FROM mental_health_training_16k
UNION ALL
SELECT 
    'Validation 3k',
    COUNT(*),
    COUNT(completion),
    COUNT(CASE WHEN completion IS NULL THEN 1 END),
    COUNT(CASE WHEN LENGTH(COALESCE(completion, '')) = 0 THEN 1 END),
    MIN(LENGTH(completion)),
    AVG(LENGTH(completion))
FROM mental_health_validation_3k;

-- Should show: null_count = 0, empty_count = 0 for both datasets

-- ==================================================================
-- STEP 4: Check urgency distribution and create stratified 16k sample
-- ==================================================================

-- Check urgency distribution in training set
SELECT 
    ground_truth_urgency,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM public.mental_health_training_set
GROUP BY ground_truth_urgency
ORDER BY count DESC;

-- Create stratified 16k training set (for 3 epochs = 48k steps)
CREATE OR REPLACE TABLE public.mental_health_training_16k AS
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
    FROM public.mental_health_training_set
),
class_targets AS (
    -- Calculate proportional sample size for each class
    SELECT 
        ground_truth_urgency,
        COUNT(*) as original_count,
        ROUND(16000.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 0) as target_sample_size
    FROM public.mental_health_training_set
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

-- Create 3k validation set
CREATE OR REPLACE TABLE public.mental_health_validation_3k AS
SELECT 
    prompt,
    completion,
    patient_id,
    ground_truth_urgency
FROM public.mental_health_validation_set
ORDER BY RANDOM()
LIMIT 3000;


-- ==================================================================
-- STEP 5: Verify counts, balance, and data quality
-- ==================================================================

-- Final count verification
SELECT 
    'Training Set' as dataset,
    COUNT(*) as row_count,
    COUNT(*) * 3 as total_steps_3epochs,
    CASE 
        WHEN COUNT(*) * 3 <= 50000 THEN '✅ Under 50k limit' 
        ELSE '❌ Over limit' 
    END as status
FROM public.mental_health_training_16k
UNION ALL
SELECT 
    'Validation Set',
    COUNT(*),
    NULL,
    '✅ OK'
FROM public.mental_health_validation_3k;

-- Check class distribution in final training set
SELECT 
    ground_truth_urgency,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM public.mental_health_training_16k
GROUP BY ground_truth_urgency
ORDER BY count DESC;


-- Check safety flag distribution (CRITICAL)
SELECT 
    'Suicide Risk Screening Needed' as safety_flag,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM public.mental_health_training_16k), 2) as percentage
FROM public.mental_health_training_16k
WHERE needs_suicide_risk_screening = TRUE;

-- Sample inspection
SELECT 
    patient_id,
    ground_truth_urgency,
    primary_clinical_concern,
    needs_suicide_risk_screening,
    LEFT(prompt, 200) as prompt_preview,
    LEFT(completion, 200) as completion_preview
FROM public.mental_health_training_16k
WHERE needs_suicide_risk_screening = TRUE
LIMIT 3;


-- ==================================================================
-- STEP 6: Start fine-tuning
-- ==================================================================

-- Launch fine-tuning job (default 3 epochs)
SELECT SNOWFLAKE.CORTEX.FINETUNE(
    'CREATE',
    'mental_health_llm_16k',
    'llama3.1-8b',
    'SELECT prompt, completion FROM public.mental_health_training_16k',
    'SELECT prompt, completion FROM public.mental_health_validation_3k'
) AS job_result;

-- Check training status (run periodically)
-- Replace with your actual job ID after launch
SELECT SNOWFLAKE.CORTEX.FINETUNE(
    'DESCRIBE',
    'ft_364ab8f7-2dad-4ff0-a8a0-29a86e55ed43'
) AS training_status;



-- ==================================================================
-- STEP 1: Test the fine-tuned model with individual cases
-- ==================================================================

-- Basic inference test
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mental_health_llm_16k',
    'Patient is a female in young_adult age group, currently working as student. 
    No documented mental health history. Not currently in mental health treatment.

    Mental health assessment: 
    Depression severity: severe_depression (score: 18/27). 
    Anxiety severity: moderate_anxiety (score: 12/21). 
    Total symptom count: 6. Severe symptoms: 4. 
    Functional impairment: severe_impairment. 
    Social isolation: severe_isolation.

    What is your assessment and recommendation?'
) AS model_response;

SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mental_health_llm_16k',
    'im so tired of everything. nothing matters anymore. everyone would be better off without me around. im just a burden'
) AS response;

SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mental_health_llm_16k',
    'i cant do this anymore. school is hell, my parents hate me, i have no friends. whats even the point of waking up every day'
) AS response;

SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mental_health_llm_16k',
    'ive been thinking about how peaceful it would be to just fall asleep and not wake up. is that normal?'
) AS response;

SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mental_health_llm_16k',
    'i cant sleep at night. i lay in bed for hours thinking about everything wrong in my life. when i do sleep i have nightmares. im exhausted all day'
) AS response;




SELECT * FROM ftr_mental_health_conversation_prompts;