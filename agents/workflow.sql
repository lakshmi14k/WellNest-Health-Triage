-- =============================================================================
-- STORED PROCEDURE: CLASSIFY_USER_QUERY (✅ CORRECTED MODEL NAMES)
-- =============================================================================

-- Drop old version first to ensure clean update
DROP PROCEDURE IF EXISTS WELLNEST.USER_MANAGEMENT.CLASSIFY_USER_QUERY(STRING, STRING);

CREATE PROCEDURE WELLNEST.USER_MANAGEMENT.CLASSIFY_USER_QUERY(
    USER_QUERY STRING,
    USER_ID STRING
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'classify_query'
AS
$$
import json

def classify_query(session, user_query, user_id):
    """Classify user health query into specialist domains"""
    
    # Get user medical profile
    profile_query = f"""
    SELECT 
        u.FULL_NAME,
        u.AGE,
        u.GENDER,
        p.HAS_DIABETES,
        p.HAS_HYPERTENSION,
        p.HAS_HEART_DISEASE,
        p.HAS_MENTAL_HEALTH_HISTORY
    FROM WELLNEST.USER_MANAGEMENT.USERS u
    LEFT JOIN WELLNEST.USER_MANAGEMENT.USER_MEDICAL_PROFILES p
        ON u.USER_ID = p.USER_ID
    WHERE u.USER_ID = '{user_id}'
    """
    
    try:
        profile_result = session.sql(profile_query).collect()
        profile_data = profile_result[0].as_dict() if profile_result else {}
    except:
        profile_data = {}
    
    system_context = """You are WellNest's medical triage system for classifying health queries into specialist domains.

**SCOPE VALIDATION FIRST:**

**IN SCOPE (Classify to one of 3 domains):**
- Diabetes, blood sugar, insulin, HbA1c, glucose management, diabetic complications
- Hypertension, blood pressure, heart disease, cardiovascular health, cholesterol, stroke
- Mental health: depression, anxiety, stress, sleep disorders, mood disorders, panic attacks

**OUT OF SCOPE (Return out_of_scope):**
- Other medical topics (cancer, orthopedics, dermatology, infections, etc.)
- Women's health (pregnancy, PCOS, menstrual health) - NOT SUPPORTED
- Non-medical queries (general knowledge, entertainment, etc.)

---

**DOMAIN CLASSIFICATION (Choose ONE):**

**DIABETES**: Blood sugar, glucose, insulin, HbA1c, diabetic symptoms/complications
**HEART_DISEASE**: Blood pressure, hypertension, heart disease, cardiovascular health, chest pain, stroke
**MENTAL_HEALTH**: Depression, anxiety, stress, sleep disorders, mood disorders, panic attacks

**URGENCY ASSESSMENT:**

**EMERGENCY**: Chest pain+sweating, stroke symptoms, suicidal thoughts with plan, blood sugar <54
**URGENT**: Blood sugar >250, BP >180/110, severe depression
**NEEDS_ATTENTION**: Uncontrolled symptoms, BP 140-179/90-109
**ROUTINE**: General health guidance

---

**RETURN FORMAT (JSON only):**

Out of scope:
{"domain": "OUT_OF_SCOPE", "urgency": "N/A", "confidence": 0.95, "reasoning": "...", "scope_violation": true}

In scope:
{"domain": "DIABETES|HEART_DISEASE|MENTAL_HEALTH", "urgency": "EMERGENCY|URGENT|NEEDS_ATTENTION|ROUTINE", "confidence": 0.95, "symptom_assessment": "...", "reasoning": "...", "safety_flags": [...], "immediate_action_needed": true|false, "scope_violation": false}"""

    # Build user context
    profile_context = ""
    if profile_data:
        conditions = []
        if profile_data.get('HAS_DIABETES'):
            conditions.append("Diabetes")
        if profile_data.get('HAS_HYPERTENSION'):
            conditions.append("Hypertension")
        if profile_data.get('HAS_HEART_DISEASE'):
            conditions.append("Heart Disease")
        if profile_data.get('HAS_MENTAL_HEALTH_HISTORY'):
            conditions.append("Mental Health History")
        
        if conditions:
            profile_context = f"\n\n**Patient Medical History:** {', '.join(conditions)}"
    
    user_message = f"**Patient Query:** {user_query}{profile_context}"
    full_prompt = f"{system_context}\n\n{user_message}\n\n**Classification (JSON only):**"
    full_prompt_escaped = full_prompt.replace("'", "''")
    
    # Call Claude via Cortex
    try:
        cortex_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'claude-4-sonnet',
            '{full_prompt_escaped}'
        ) as response
        """
        
        result = session.sql(cortex_query).collect()
        response = result[0]['RESPONSE']
        
        # Parse JSON
        response_clean = response.strip()
        if response_clean.startswith('```json'):
            response_clean = response_clean.split('```json')[1].split('```')[0].strip()
        elif response_clean.startswith('```'):
            response_clean = response_clean.split('```')[1].split('```')[0].strip()
        
        classification = json.loads(response_clean)
        
        # ✅✅✅ UPDATED MODEL NAMES HERE ✅✅✅
        if not classification.get('scope_violation', False):
            domain = classification['domain']
            
            model_mapping = {
                "DIABETES": "WELLNEST.PUBLIC.DIABETES_LLM_16K1",           # ✅ Changed to 16K1
                "HEART_DISEASE": "WELLNEST.PUBLIC.HYPERTENSION_LLM_16K_1", # ✅ Changed to 16K_1
                "MENTAL_HEALTH": "WELLNEST.PUBLIC.MENTAL_HEALTH_LLM_16K"   # ✅ Kept same
            }
            
            classification['specialist_model'] = model_mapping.get(domain, "llama3.1-8b")
        else:
            classification['specialist_model'] = None
        
        classification['classification_status'] = 'success'
        return classification
        
    except Exception as e:
        # ✅✅✅ UPDATED FALLBACK MODEL NAME TOO ✅✅✅
        return {
            "domain": "DIABETES",
            "urgency": "ROUTINE",
            "confidence": 0.5,
            "symptom_assessment": "Unable to assess",
            "reasoning": f"Classification error: {str(e)}",
            "safety_flags": [],
            "immediate_action_needed": False,
            "specialist_model": "WELLNEST.PUBLIC.DIABETES_LLM_16K1",  # ✅ Changed to 16K1
            "classification_status": "error",
            "scope_violation": False
        }
$$;

GRANT USAGE ON PROCEDURE WELLNEST.USER_MANAGEMENT.CLASSIFY_USER_QUERY(STRING, STRING) 
    TO ROLE SYSADMIN;

-- Verify creation
SELECT '✅ CLASSIFY_USER_QUERY recreated with DIABETES_LLM_16K1' AS STATUS;


-- =============================================================================
-- STORED PROCEDURE: QUERY_SPECIALIST_LLM (Already correct, no changes needed)
-- =============================================================================

-- This procedure doesn't need changes - it receives the model name from CLASSIFY_USER_QUERY
-- Keeping it here for completeness

CREATE OR REPLACE PROCEDURE WELLNEST.USER_MANAGEMENT.QUERY_SPECIALIST_LLM(
    USER_QUERY STRING,
    DOMAIN STRING,
    USER_ID STRING,
    SPECIALIST_MODEL STRING,
    SESSION_ID STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'query_specialist'
AS
$$
import json
import re

def query_specialist(session, user_query, domain, user_id, specialist_model, session_id):
    """Query specialist and lightly format response"""
    
    # Get smart context
    try:
        context_result = session.call(
            'WELLNEST.USER_MANAGEMENT.GET_SMART_CONTEXT',
            user_query,
            user_id,
            domain,
            session_id
        )
        
        context = context_result if isinstance(context_result, dict) else json.loads(context_result)
    except:
        context = {
            "patient_context": "",
            "current_context": "",
            "semantic_context": "",
            "metrics_context": ""
        }
    
    # System prompts
    system_prompts = {
        "DIABETES": """You are a diabetes care specialist providing patient guidance.

**Context you have access to:**
- Patient medical profile and history
- Past blood sugar readings and trends
- Previous conversations about diabetes management

**Your task:**
Provide helpful, personalized diabetes guidance in a conversational tone.

**DO NOT use these structured phrases:**
- "Risk assessment: X/5"
- "Key messages:"
- "Recommended keywords:"
- "TRIAGE:"
- "Priority areas:"

**INSTEAD, speak naturally:**
- "Your blood sugar of 180 is concerning because..."
- "I'm glad to see your HbA1c improved from 8.5 to 7.2!"
- "Let's talk about managing those symptoms you mentioned..."

Respond conversationally based on the patient information provided below.""",

        "HEART_DISEASE": """You are a cardiovascular health specialist providing patient guidance.

**Context you have access to:**
- Patient medical profile and history
- Past blood pressure readings and trends
- Previous conversations about heart health

**Your task:**
Provide helpful, personalized cardiovascular guidance in a conversational tone.

**DO NOT use these structured phrases:**
- "Risk assessment: X/5"
- "Key messages:"
- "Recommended keywords:"
- "TRIAGE:"
- "Lifestyle areas to assess:"

**INSTEAD, speak naturally:**
- "Your blood pressure of 150/95 is elevated..."
- "Great job! Your BP improved from 150 to 130!"
- "Let me explain what these readings mean..."

Respond conversationally based on the patient information provided below.""",

        "MENTAL_HEALTH": """You are a mental health specialist providing supportive guidance.

**Context you have access to:**
- Patient mental health history
- Previous conversations about mood and symptoms
- Ongoing patterns and progress

**Your task:**
Provide empathetic, supportive mental health guidance.

**SAFETY:** If suicidal thoughts mentioned, provide crisis resources (988 Lifeline).

Respond supportively based on the patient information provided below."""
    }
    
    system_prompt = system_prompts.get(domain, system_prompts["DIABETES"])
    
    # Build prompt with context
    full_prompt = f"""{system_prompt}

{'='*70}
PATIENT INFORMATION:
{'='*70}
{context.get('patient_context', 'No patient profile available.')}

{'='*70}
TRACKED HEALTH METRICS:
{'='*70}
{context.get('metrics_context', 'No metrics tracked yet.')}

{'='*70}
CURRENT CONVERSATION (This Session):
{'='*70}
{context.get('current_context', 'This is the first message in this session.')}

{'='*70}
RELEVANT PAST DISCUSSIONS:
{'='*70}
{context.get('semantic_context', 'No similar past conversations found.')}

{'='*70}
CURRENT PATIENT QUESTION:
{'='*70}
{user_query}

{'='*70}
YOUR RESPONSE (Conversational, not structured):
{'='*70}"""

    prompt_escaped = full_prompt.replace("'", "''")
    
    # Call specialist model (model name passed from CLASSIFY_USER_QUERY)
    cortex_query = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        '{specialist_model}',
        '{prompt_escaped}'
    ) AS response
    """
    
    try:
        result = session.sql(cortex_query).collect()
        raw_response = result[0]['RESPONSE'].strip()
        
        # Light formatting cleanup
        formatted_response = light_format_cleanup(raw_response, context)
        
        return formatted_response
    
    except Exception as e:
        return f"I apologize, but I encountered an error: {str(e)}"

def light_format_cleanup(response, context):
    """Remove formatting artifacts, preserve medical content"""
    
    # Remove structured format labels
    cleanup_patterns = [
        (r'Risk assessment:\s*\d+/\d+\s*-\s*', ''),
        (r'Key messages:\s*', ''),
        (r'Recommended keywords:\s*', ''),
        (r'Priority areas to discuss:\s*', ''),
        (r'Lifestyle areas to assess:\s*', ''),
        (r'Priority topics:\s*', ''),
        (r'TRIAGE:\s*', ''),
        (r'Recommended foods:\s*', 'I recommend: '),
    ]
    
    cleaned = response
    for pattern, replacement in cleanup_patterns:
        cleaned = re.sub(pattern, replacement, cleaned, flags=re.IGNORECASE)
    
    # Remove standalone structural lines
    lines = cleaned.split('\n')
    filtered_lines = []
    
    for line in lines:
        line_stripped = line.strip()
        
        if line_stripped in ['Priority areas to assess:', 'Lifestyle priorities:', 
                            'Key phrases:', 'Recommended keywords:']:
            continue
        
        if re.match(r'^(Risk assessment|Key messages|TRIAGE|Priority topics):\s*$', line_stripped, re.IGNORECASE):
            continue
        
        filtered_lines.append(line)
    
    cleaned = '\n'.join(filtered_lines)
    cleaned = re.sub(r'\n\n\n+', '\n\n', cleaned)
    
    # Add encouragement for metric improvements
    metrics_context = context.get('metrics_context', '')
    if metrics_context and 'improved' in metrics_context.lower():
        cleaned = "Great progress with your health metrics! 🎉\n\n" + cleaned
    
    return cleaned.strip()

$$;

GRANT USAGE ON PROCEDURE WELLNEST.USER_MANAGEMENT.CLASSIFY_USER_QUERY(STRING, STRING) 
    TO ROLE SYSADMIN;

SELECT '✅ CLASSIFY_USER_QUERY updated with model names' AS STATUS;


-- =============================================================================
-- VERIFICATION: Test the updated procedure
-- =============================================================================

-- Test classification with diabetes symptoms
CALL WELLNEST.USER_MANAGEMENT.CLASSIFY_USER_QUERY(
    'I have frequent urination and intense thirst',
    '599e418e-1f0f-4354-ab9c-acd3e004b014'
);

-- Check result - should show DIABETES_LLM_16K1
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));


