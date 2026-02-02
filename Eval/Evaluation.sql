select * from diabetes_validation_3k;

-- ==========================================
-- DIABETES MODEL EVALUATION
-- Table: diabetes_validation_3k
-- Columns: PROMPT, COMPLETION, PATIENT_ID, GROUND_TRUTH_URGENCY
-- ==========================================

-- STEP 1: Sample 200 diverse cases
CREATE OR REPLACE TABLE diabetes_model_evaluation AS
SELECT 
    PROMPT,
    COMPLETION AS expected_completion,
    PATIENT_ID,
    GROUND_TRUTH_URGENCY AS expected_urgency
FROM diabetes_validation_3k
WHERE GROUND_TRUTH_URGENCY IS NOT NULL
LIMIT 200;

-- Verify distribution
SELECT 
    expected_urgency,
    COUNT(*) AS case_count
FROM diabetes_model_evaluation
GROUP BY expected_urgency
ORDER BY case_count DESC;

-- STEP 2: Generate responses from all models
CREATE OR REPLACE TABLE diabetes_model_responses AS
SELECT 
    PATIENT_ID,
    PROMPT,
    expected_completion,
    expected_urgency,
    
    -- Test Llama 3.1 8B
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-8b',
        PROMPT
    ) AS llama_8b_response,
    
    -- Test Llama 3.1 70B
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        PROMPT
    ) AS llama_70b_response,
    
    -- Test Mistral Large
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large',
        PROMPT
    ) AS mistral_large_response,
    
    -- Test Mixtral 8x7B
    SNOWFLAKE.CORTEX.COMPLETE(
        'mixtral-8x7b',
        PROMPT
    ) AS mixtral_8x7b_response

FROM diabetes_model_evaluation;

-- This runs 200 cases × 4 models = 800 inferences
-- Expected time: 5-10 minutes

-- STEP 3: Evaluate responses using LLM-as-judge
CREATE OR REPLACE TABLE diabetes_model_scores AS
SELECT 
    PATIENT_ID,
    expected_urgency,
    expected_completion,
    
    -- Evaluate Llama 8B
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        CONCAT(
            'You are evaluating a diabetes management AI response. Rate 1-10.\n\n',
            'ORIGINAL PROMPT:\n', PROMPT, '\n\n',
            'EXPECTED RESPONSE:\n', expected_completion, '\n\n',
            'MODEL RESPONSE:\n', llama_8b_response, '\n\n',
            'Expected Urgency: ', expected_urgency, '\n\n',
            'Evaluation Criteria:\n',
            '1. Urgency Assessment (3 pts): Correctly identifies urgency level\n',
            '2. Clinical Accuracy (3 pts): Accurate interpretation of labs/risk scores\n',
            '3. Completeness (2 pts): Addresses all key points from expected response\n',
            '4. Safety & Clarity (2 pts): Appropriate recommendations, clear language\n\n',
            'Return ONLY JSON: {"score": <1-10>, "urgency_correct": <true/false>, "reasoning": "<brief explanation>"}'
        )
    ) AS llama_8b_eval,
    
    -- Evaluate Llama 70B
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        CONCAT(
            'You are evaluating a diabetes management AI response. Rate 1-10.\n\n',
            'ORIGINAL PROMPT:\n', PROMPT, '\n\n',
            'EXPECTED RESPONSE:\n', expected_completion, '\n\n',
            'MODEL RESPONSE:\n', llama_70b_response, '\n\n',
            'Expected Urgency: ', expected_urgency, '\n\n',
            'Evaluation Criteria:\n',
            '1. Urgency Assessment (3 pts): Correctly identifies urgency level\n',
            '2. Clinical Accuracy (3 pts): Accurate interpretation of labs/risk scores\n',
            '3. Completeness (2 pts): Addresses all key points from expected response\n',
            '4. Safety & Clarity (2 pts): Appropriate recommendations, clear language\n\n',
            'Return ONLY JSON: {"score": <1-10>, "urgency_correct": <true/false>, "reasoning": "<brief explanation>"}'
        )
    ) AS llama_70b_eval,
    
    -- Evaluate Mistral Large
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        CONCAT(
            'You are evaluating a diabetes management AI response. Rate 1-10.\n\n',
            'ORIGINAL PROMPT:\n', PROMPT, '\n\n',
            'EXPECTED RESPONSE:\n', expected_completion, '\n\n',
            'MODEL RESPONSE:\n', mistral_large_response, '\n\n',
            'Expected Urgency: ', expected_urgency, '\n\n',
            'Evaluation Criteria:\n',
            '1. Urgency Assessment (3 pts): Correctly identifies urgency level\n',
            '2. Clinical Accuracy (3 pts): Accurate interpretation of labs/risk scores\n',
            '3. Completeness (2 pts): Addresses all key points from expected response\n',
            '4. Safety & Clarity (2 pts): Appropriate recommendations, clear language\n\n',
            'Return ONLY JSON: {"score": <1-10>, "urgency_correct": <true/false>, "reasoning": "<brief explanation>"}'
        )
    ) AS mistral_large_eval,
    
    -- Evaluate Mixtral 8x7B
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        CONCAT(
            'You are evaluating a diabetes management AI response. Rate 1-10.\n\n',
            'ORIGINAL PROMPT:\n', PROMPT, '\n\n',
            'EXPECTED RESPONSE:\n', expected_completion, '\n\n',
            'MODEL RESPONSE:\n', mixtral_8x7b_response, '\n\n',
            'Expected Urgency: ', expected_urgency, '\n\n',
            'Evaluation Criteria:\n',
            '1. Urgency Assessment (3 pts): Correctly identifies urgency level\n',
            '2. Clinical Accuracy (3 pts): Accurate interpretation of labs/risk scores\n',
            '3. Completeness (2 pts): Addresses all key points from expected response\n',
            '4. Safety & Clarity (2 pts): Appropriate recommendations, clear language\n\n',
            'Return ONLY JSON: {"score": <1-10>, "urgency_correct": <true/false>, "reasoning": "<brief explanation>"}'
        )
    ) AS mixtral_8x7b_eval

FROM diabetes_model_responses;

-- STEP 4: Parse scores and create rankings
CREATE OR REPLACE TABLE diabetes_model_rankings AS
SELECT 
    model_name,
    ROUND(AVG(score), 2) AS avg_score,
    ROUND(AVG(CASE WHEN urgency_correct THEN 100.0 ELSE 0.0 END), 1) AS urgency_accuracy_pct,
    COUNT(*) AS total_cases,
    SUM(CASE WHEN score >= 8 THEN 1 ELSE 0 END) AS excellent_responses,
    SUM(CASE WHEN score < 5 THEN 1 ELSE 0 END) AS poor_responses,
    MIN(score) AS worst_score,
    MAX(score) AS best_score
FROM (
    -- Llama 8B scores
    SELECT 
        'llama3.1-8b' AS model_name,
        PARSE_JSON(llama_8b_eval):score::NUMBER AS score,
        PARSE_JSON(llama_8b_eval):urgency_correct::BOOLEAN AS urgency_correct
    FROM diabetes_model_scores
    
    UNION ALL
    
    -- Llama 70B scores
    SELECT 
        'llama3.1-70b' AS model_name,
        PARSE_JSON(llama_70b_eval):score::NUMBER AS score,
        PARSE_JSON(llama_70b_eval):urgency_correct::BOOLEAN AS urgency_correct
    FROM diabetes_model_scores
    
    UNION ALL
    
    -- Mistral Large scores
    SELECT 
        'mistral-large' AS model_name,
        PARSE_JSON(mistral_large_eval):score::NUMBER AS score,
        PARSE_JSON(mistral_large_eval):urgency_correct::BOOLEAN AS urgency_correct
    FROM diabetes_model_scores
    
    UNION ALL
    
    -- Mixtral 8x7B scores
    SELECT 
        'mixtral-8x7b' AS model_name,
        PARSE_JSON(mixtral_8x7b_eval):score::NUMBER AS score,
        PARSE_JSON(mixtral_8x7b_eval):urgency_correct::BOOLEAN AS urgency_correct
    FROM diabetes_model_scores
)
GROUP BY model_name
ORDER BY avg_score DESC;

-- STEP 5: View final rankings
SELECT * FROM diabetes_model_rankings;

-- STEP 6: Analyze failure cases (for the best model)
SELECT 
    PATIENT_ID,
    expected_urgency,
    SUBSTRING(PROMPT, 1, 150) AS prompt_preview,
    SUBSTRING(llama_70b_response, 1, 200) AS response_preview,
    PARSE_JSON(llama_70b_eval):score::NUMBER AS score,
    PARSE_JSON(llama_70b_eval):reasoning::STRING AS why_failed
FROM diabetes_model_scores
WHERE PARSE_JSON(llama_70b_eval):score::NUMBER < 6
ORDER BY score ASC
LIMIT 10;
```

---

Select * from ftr_diabetes_conversation_prompts;
select * from ftr_diabetes_core_clinical;
select * from ftr_diabetes_risk_urgency;