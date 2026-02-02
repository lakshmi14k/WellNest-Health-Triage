-- ==================================================================
-- STEP 1: Create training data table in Cortex format
-- ==================================================================
USE DATABASE wellnest;
USE SCHEMA public;
SELECT * FROM diabetes_cortex_training_data


CREATE OR REPLACE TABLE public.diabetes_cortex_training_data AS
SELECT 
    patient_id,
    
    -- Cortex expects a VARIANT column with a messages array
    OBJECT_CONSTRUCT(
        'messages', ARRAY_CONSTRUCT(
            -- System message
            OBJECT_CONSTRUCT(
                'role', 'system',
                'content', 'You are a medical AI assistant specializing in diabetes and metabolic disease management. Your role is to assess patient presentations, evaluate urgency (Emergency/Urgent/Routine), identify key symptoms for inquiry, provide evidence-based management recommendations, and suggest appropriate patient education. You must prioritize patient safety, detect emergencies (severe hyperglycemia/hypoglycemia), provide clear triage recommendations based on ADA clinical guidelines, and recommend specialist referral when appropriate. You serve as a clinical decision support tool, not a replacement for medical diagnosis.'
            ),
            -- User message
            OBJECT_CONSTRUCT(
                'role', 'user',
                'content', user_prompt
            ),
            -- Assistant message
            OBJECT_CONSTRUCT(
                'role', 'assistant',
                'content', assistant_response
            )
        )
    ) as messages,
    
    -- Metadata for tracking (optional)
    ground_truth_urgency,
    condition_type
    
FROM public.diabetes_training_prompts;

-- Verify the format
SELECT 
    patient_id,
    messages,
    ground_truth_urgency
FROM public.diabetes_cortex_training_data
LIMIT 3;


-- ==================================================================
-- STEP 2: Create train/validation split (80/20)
-- ==================================================================

-- ==================================================================
-- STEP 1 (REVISED): Create training data with 'prompt' and 'completion' columns
-- ==================================================================

CREATE OR REPLACE TABLE public.diabetes_training_set AS
WITH base_data AS (
    SELECT * FROM public.diabetes_training_prompts
    WHERE MOD(ABS(HASH(patient_id)), 10) < 8  -- 80% for training
)
SELECT 
    patient_id,
    ground_truth_urgency,
    
    -- 'prompt' column: Combine system message + user prompt
    CONCAT(
        'You are a medical AI assistant specializing in diabetes and metabolic disease management. ',
        'Your role is to assess patient presentations, evaluate urgency (Emergency/Urgent/Routine), ',
        'identify key symptoms for inquiry, provide evidence-based management recommendations, ',
        'and suggest appropriate patient education. You must prioritize patient safety, ',
        'detect emergencies (severe hyperglycemia/hypoglycemia), provide clear triage recommendations ',
        'based on ADA clinical guidelines, and recommend specialist referral when appropriate.\n\n',
        'Patient Case:\n',
        user_prompt
    ) as prompt,
    
    -- 'completion' column: The expected response
    assistant_response as completion
    
FROM base_data;

-- Validation set (20%)
CREATE OR REPLACE TABLE public.diabetes_validation_set AS
WITH base_data AS (
    SELECT * FROM public.diabetes_training_prompts
    WHERE MOD(ABS(HASH(patient_id)), 10) >= 8  -- 20% for validation
)
SELECT 
    patient_id,
    ground_truth_urgency,
    
    CONCAT(
        'You are a medical AI assistant specializing in diabetes and metabolic disease management. ',
        'Your role is to assess patient presentations, evaluate urgency (Emergency/Urgent/Routine), ',
        'identify key symptoms for inquiry, provide evidence-based management recommendations, ',
        'and suggest appropriate patient education. You must prioritize patient safety, ',
        'detect emergencies (severe hyperglycemia/hypoglycemia), provide clear triage recommendations ',
        'based on ADA clinical guidelines, and recommend specialist referral when appropriate.\n\n',
        'Patient Case:\n',
        user_prompt
    ) as prompt,
    
    assistant_response as completion
    
FROM base_data;

-- Verify the format
SELECT 
    patient_id,
    LEFT(prompt, 200) as prompt_preview,
    LEFT(completion, 200) as completion_preview
FROM public.diabetes_training_set
LIMIT 3;

-- Check counts
SELECT 
    'Training' as dataset, COUNT(*) as count 
FROM public.diabetes_training_set
UNION ALL
SELECT 
    'Validation', COUNT(*) 
FROM public.diabetes_validation_set;




-- Let's recalculate for 3 epochs (default)
-- 50,000 / 3 = 16,666 max examples

CREATE OR REPLACE TABLE public.diabetes_training_16k AS
WITH ranked_data AS (
    SELECT 
        prompt,
        completion,
        patient_id,
        ground_truth_urgency,
        ROW_NUMBER() OVER (PARTITION BY ground_truth_urgency ORDER BY RANDOM()) as rn
    FROM public.diabetes_training_set
),
class_targets AS (
    SELECT 
        ground_truth_urgency,
        ROUND(16000.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 0) as max_per_class
    FROM public.diabetes_training_set
    GROUP BY ground_truth_urgency
)
SELECT 
    r.prompt,
    r.completion,
    r.patient_id,
    r.ground_truth_urgency
FROM ranked_data r
JOIN class_targets t ON r.ground_truth_urgency = t.ground_truth_urgency
WHERE r.rn <= t.max_per_class
ORDER BY RANDOM();

-- Validation set
CREATE OR REPLACE TABLE public.diabetes_validation_3k AS
SELECT 
    prompt,
    completion,
    patient_id,
    ground_truth_urgency
FROM public.diabetes_validation_set
ORDER BY RANDOM()
LIMIT 3000;

-- Verify the counts
SELECT 
    'Training' as dataset,
    COUNT(*) as row_count,
    COUNT(*) * 3 as total_steps_3epochs,
    CASE 
        WHEN COUNT(*) * 3 <= 50000 THEN '✅ Under limit' 
        ELSE '❌ Over limit' 
    END as status
FROM public.diabetes_training_16k
UNION ALL
SELECT 
    'Validation',
    COUNT(*),
    NULL,
    '✅ OK'
FROM public.diabetes_validation_3k;

-- Start fine-tuning WITHOUT options parameter (uses default 3 epochs)
SELECT SNOWFLAKE.CORTEX.FINETUNE(
    'CREATE',
    'diabetes_llm_16k',
    'llama3.1-8b',
    'SELECT prompt, completion FROM public.diabetes_training_16k',
    'SELECT prompt, completion FROM public.diabetes_validation_3k',
    NULL  -- No custom options, use defaults
) AS job_result;

-- ==================================================================
-- EVALUATION SCRIPT (RUN AFTER TRAINING COMPLETES)
-- Run this when status = 'SUCCESS'
-- ==================================================================

-- Step 1: Check if model is ready
SELECT SNOWFLAKE.CORTEX.FINETUNE(
    'DESCRIBE',
    'ft_1db06af4-6f4b-40cf-b57e-e313056e8119'
) AS final_status;

-- Step 2: Test the model with a sample
-- Snowflake Cortex COMPLETE expects: model_name, prompt (no options object)
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'diabetes_llm_16k',
    'You are a medical AI assistant specializing in diabetes management.

Patient Case: Patient is a Female with BMI of Obese Class II, currently smokes, classified as obese. Clinical findings: Diabetes stage: Uncontrolled Diabetes. HbA1c level: 9.2%. Glucose control: Poor. Lab values are concordant. Confirmed diabetes diagnosis. Patient has multiple conditions: heart disease, hypertension, diabetes. Glucose levels require urgent medical attention.

Provide a clinical assessment including risk evaluation, urgency classification, recommended symptom inquiries, management approach, and patient education priorities.'
) AS model_response;

-- Step 3: Create evaluation dataset (predictions on validation set)
CREATE OR REPLACE TABLE diabetes_model_predictions AS
SELECT 
    v.patient_id,
    v.ground_truth_urgency,
    v.prompt,
    v.completion as ground_truth_response,
    
    -- Get model prediction
    SNOWFLAKE.CORTEX.COMPLETE(
        'diabetes_llm_16k',
        v.prompt
    ) as model_prediction_raw
    
FROM diabetes_validation_3k v
LIMIT 500;  -- Evaluate on 500 examples first

-- Step 4: Extract predicted urgency
CREATE OR REPLACE TABLE diabetes_predictions_analyzed AS
SELECT 
    patient_id,
    ground_truth_urgency,
    prompt,
    ground_truth_response,
    model_prediction_raw,
    
    -- Extract predicted urgency from model response
    CASE 
        WHEN model_prediction_raw LIKE '%IMMEDIATE EMERGENCY%' 
          OR model_prediction_raw LIKE '%emergency evaluation%'
          OR model_prediction_raw LIKE '%Direct to emergency%'
          OR model_prediction_raw LIKE '%emergency department%'
        THEN 'emergency'
        
        WHEN model_prediction_raw LIKE '%URGENT%evaluation%' 
          OR model_prediction_raw LIKE '%24-48 hours%'
          OR model_prediction_raw LIKE '%urgent medical evaluation%'
          OR model_prediction_raw LIKE '%requires URGENT%'
        THEN 'urgent'
        
        WHEN model_prediction_raw LIKE '%ROUTINE%follow-up%'
          OR model_prediction_raw LIKE '%routine monitoring%'
          OR model_prediction_raw LIKE '%regular monitoring%'
          OR model_prediction_raw LIKE '%ROUTINE%care%'
        THEN 'routine'
        
        ELSE 'unclear'
    END as predicted_urgency
    
FROM diabetes_model_predictions;

-- Step 5: Calculate accuracy
SELECT 
    'OVERALL ACCURACY' as metric,
    COUNT(*) as total,
    SUM(CASE WHEN ground_truth_urgency = predicted_urgency THEN 1 ELSE 0 END) as correct,
    ROUND(
        SUM(CASE WHEN ground_truth_urgency = predicted_urgency THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) as accuracy_percentage
FROM diabetes_predictions_analyzed;

-- Step 6: Confusion matrix
SELECT 
    ground_truth_urgency as actual,
    predicted_urgency as predicted,
    COUNT(*) as count_val,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY ground_truth_urgency), 2) as percentage
FROM diabetes_predictions_analyzed
GROUP BY ground_truth_urgency, predicted_urgency
ORDER BY ground_truth_urgency, count_val DESC;

-- Step 7: Per-class accuracy
SELECT 
    ground_truth_urgency,
    COUNT(*) as total_cases,
    SUM(CASE WHEN ground_truth_urgency = predicted_urgency THEN 1 ELSE 0 END) as correct_cases,
    ROUND(
        SUM(CASE WHEN ground_truth_urgency = predicted_urgency THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) as class_accuracy
FROM diabetes_predictions_analyzed
GROUP BY ground_truth_urgency
ORDER BY total_cases DESC;

-- Step 8: Critical safety check
SELECT 
    'Emergency cases missed' as safety_metric,
    COUNT(*) as count_val
FROM diabetes_predictions_analyzed
WHERE ground_truth_urgency = 'emergency' 
  AND predicted_urgency != 'emergency';

SELECT 
    'Emergency → Routine (CRITICAL ERROR)' as safety_metric,
    COUNT(*) as count_val
FROM diabetes_predictions_analyzed
WHERE ground_truth_urgency = 'emergency' 
  AND predicted_urgency = 'routine';

-- Step 9: Sample correct predictions
SELECT 
    'CORRECT PREDICTIONS' as category,
    patient_id,
    ground_truth_urgency,
    predicted_urgency,
    LEFT(prompt, 200) as prompt_preview,
    LEFT(CAST(model_prediction_raw AS VARCHAR), 200) as prediction_preview
FROM diabetes_predictions_analyzed
WHERE ground_truth_urgency = predicted_urgency
ORDER BY RANDOM()
LIMIT 5;

-- Step 10: Sample incorrect predictions
SELECT 
    'INCORRECT PREDICTIONS' as category,
    patient_id,
    ground_truth_urgency as actual,
    predicted_urgency as predicted,
    LEFT(prompt, 200) as prompt_preview,
    LEFT(CAST(model_prediction_raw AS VARCHAR), 200) as wrong_prediction_preview
FROM diabetes_predictions_analyzed
WHERE ground_truth_urgency != predicted_urgency
ORDER BY RANDOM()
LIMIT 5;