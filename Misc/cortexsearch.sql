-- =============================================================================
-- CORTEX SEARCH SERVICE FOR CONVERSATIONS
-- =============================================================================

USE DATABASE WELLNEST;
USE SCHEMA USER_MANAGEMENT;

-- Create view WITHOUT non-deterministic functions
CREATE OR REPLACE VIEW WELLNEST.USER_MANAGEMENT.VW_SEARCHABLE_CONVERSATIONS AS
SELECT 
    CONVERSATION_ID,
    USER_ID,
    SESSION_ID,
    MESSAGE_TIMESTAMP,
    
    -- Combine user message and response for richer context
    USER_MESSAGE || ' ' || ASSISTANT_RESPONSE AS combined_text,
    
    USER_MESSAGE,
    ASSISTANT_RESPONSE,
    ROUTED_TO_DOMAIN,
    URGENCY_LEVEL,
    DETECTED_SYMPTOMS,
    
    -- Add useful metadata for filtering
    DATE(MESSAGE_TIMESTAMP) AS conversation_date
    -- REMOVED: days_ago (was causing the error)
    
FROM WELLNEST.USER_MANAGEMENT.CONVERSATION_HISTORY;


-- Create the Cortex Search Service
CREATE OR REPLACE CORTEX SEARCH SERVICE WELLNEST.USER_MANAGEMENT.CONVERSATION_SEARCH_SERVICE
ON combined_text
ATTRIBUTES conversation_id, user_id, routed_to_domain, urgency_level, 
           conversation_date, user_message, assistant_response
WAREHOUSE = WELLNEST
TARGET_LAG = '1 minute'
AS (
    SELECT 
        CONVERSATION_ID,
        USER_ID,
        ROUTED_TO_DOMAIN,
        URGENCY_LEVEL,
        conversation_date,
        USER_MESSAGE,
        ASSISTANT_RESPONSE,
        combined_text
    FROM WELLNEST.USER_MANAGEMENT.VW_SEARCHABLE_CONVERSATIONS
);

-- Grant access
GRANT USAGE ON CORTEX SEARCH SERVICE WELLNEST.USER_MANAGEMENT.CONVERSATION_SEARCH_SERVICE 
    TO ROLE TRAINING_ROLE;


GRANT USAGE ON DATABASE WELLNEST TO ROLE TRAINING_ROLE;
GRANT USAGE ON SCHEMA WELLNEST.USER_MANAGEMENT TO ROLE TRAINING_ROLE;


-- =============================================================================
-- UPDATED: GET_SMART_CONTEXT (Without days_ago field)
-- =============================================================================

CREATE OR REPLACE PROCEDURE WELLNEST.USER_MANAGEMENT.GET_SMART_CONTEXT(
    USER_QUERY STRING,
    USER_ID STRING,
    DOMAIN STRING,
    SESSION_ID STRING
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_smart_context'
AS
$$
import json
from datetime import datetime, timedelta

def get_smart_context(session, user_query, user_id, domain, session_id):
    """
    Get smart context using Cortex Search + metric tracking
    """
    
    # =========================================================================
    # 1. CURRENT SESSION CONTEXT
    # =========================================================================
    current_session_query = f"""
    SELECT USER_MESSAGE, ASSISTANT_RESPONSE
    FROM WELLNEST.USER_MANAGEMENT.CONVERSATION_HISTORY
    WHERE USER_ID = '{user_id}' AND SESSION_ID = '{session_id}'
    ORDER BY MESSAGE_TIMESTAMP DESC
    LIMIT 5
    """
    
    try:
        current = session.sql(current_session_query).collect()
        current_history = [{"user": r['USER_MESSAGE'], "assistant": r['ASSISTANT_RESPONSE']} for r in current]
        current_history.reverse()
    except:
        current_history = []
    
    # =========================================================================
    # 2. SEMANTIC SEARCH using Cortex Search Service
    # =========================================================================
    query_escaped = user_query.replace("'", "''")
    
    # Calculate date 30 days ago for filtering
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    # Use Cortex Search (FIXED - without days_ago filter)
    search_query = f"""
    SELECT 
        conversation_id,
        user_message,
        assistant_response,
        conversation_date,
        routed_to_domain,
        urgency_level
    FROM TABLE(
        WELLNEST.USER_MANAGEMENT.CONVERSATION_SEARCH_SERVICE(
            query => '{query_escaped}',
            filter => {{
                '@eq': {{
                    'user_id': '{user_id}',
                    'routed_to_domain': '{domain}'
                }},
                '@gte': {{
                    'conversation_date': '{thirty_days_ago}'
                }}
            }},
            limit => 3
        )
    )
    """
    
    try:
        search_results = session.sql(search_query).collect()
        similar_conversations = [
            {
                "user_message": r['USER_MESSAGE'],
                "assistant_response": r['ASSISTANT_RESPONSE'],
                "date": str(r['CONVERSATION_DATE']),
                "urgency": r['URGENCY_LEVEL']
            }
            for r in search_results
        ]
    except Exception as e:
        similar_conversations = []
    
    # =========================================================================
    # 3. GET METRIC TRENDS
    # =========================================================================
    metric_trends = {}
    
    if domain == 'LIFESTYLE_DISEASES':
        metric_types = ['blood_pressure_systolic', 'blood_pressure_diastolic', 
                       'blood_sugar', 'hba1c', 'weight', 'cholesterol_total']
    elif domain == 'WOMEN_WELLNESS':
        metric_types = ['blood_pressure_systolic', 'blood_sugar', 'weight']
    else:
        metric_types = []
    
    for metric_type in metric_types:
        trend_query = f"""
        SELECT 
            METRIC_VALUE,
            MEASUREMENT_DATE,
            SEVERITY
        FROM WELLNEST.MEDICAL_DATA.HEALTH_METRICS
        WHERE USER_ID = '{user_id}'
          AND METRIC_TYPE = '{metric_type}'
          AND MEASUREMENT_DATE >= DATEADD(day, -90, CURRENT_DATE())
        ORDER BY MEASUREMENT_DATE ASC
        """
        
        try:
            trend_results = session.sql(trend_query).collect()
            
            if trend_results:
                values = [r['METRIC_VALUE'] for r in trend_results]
                dates = [str(r['MEASUREMENT_DATE']) for r in trend_results]
                
                if len(values) >= 2:
                    first_val = values[0]
                    last_val = values[-1]
                    change = last_val - first_val
                    percent_change = (change / first_val) * 100 if first_val != 0 else 0
                    
                    if abs(percent_change) < 5:
                        trend = "stable"
                    elif percent_change > 0:
                        trend = "increasing"
                    else:
                        trend = "decreasing"
                else:
                    trend = "single_reading"
                
                metric_trends[metric_type] = {
                    "current_value": values[-1],
                    "first_value": values[0],
                    "trend": trend,
                    "percent_change": round(percent_change, 1) if len(values) >= 2 else 0,
                    "data_points": len(values),
                    "latest_date": dates[-1]
                }
        except:
            pass
    
    # =========================================================================
    # 4. GET RECENT SUMMARIES
    # =========================================================================
    summaries_query = f"""
    SELECT 
        SUMMARY_TEXT,
        KEY_TOPICS,
        MENTIONED_METRICS,
        IMPROVEMENT_AREAS,
        CONCERN_AREAS
    FROM WELLNEST.USER_MANAGEMENT.CONVERSATION_SUMMARIES
    WHERE USER_ID = '{user_id}'
      AND DOMAIN = '{domain}'
      AND TIME_PERIOD_START >= DATEADD(day, -30, CURRENT_TIMESTAMP())
    ORDER BY TIME_PERIOD_START DESC
    LIMIT 2
    """
    
    try:
        summary_results = session.sql(summaries_query).collect()
        recent_summaries = [
            {
                "summary": r['SUMMARY_TEXT'],
                "key_topics": r['KEY_TOPICS'],
                "improvements": r['IMPROVEMENT_AREAS'],
                "concerns": r['CONCERN_AREAS']
            }
            for r in summary_results
        ]
    except:
        recent_summaries = []
    
    # =========================================================================
    # 5. BUILD CONTEXT STRINGS
    # =========================================================================
    
    # Patient profile context
    profile_query = f"""
    SELECT 
        u.AGE, u.GENDER,
        p.BMI, p.HAS_DIABETES, p.HAS_HYPERTENSION, p.HAS_HEART_DISEASE,
        p.HAS_MENTAL_HEALTH_HISTORY, p.HAS_PCOS, p.SMOKING_STATUS,
        p.EXERCISE_FREQUENCY, p.IS_PREGNANT, p.PREGNANCY_TRIMESTER
    FROM WELLNEST.USER_MANAGEMENT.USERS u
    LEFT JOIN WELLNEST.USER_MANAGEMENT.USER_MEDICAL_PROFILES p ON u.USER_ID = p.USER_ID
    WHERE u.USER_ID = '{user_id}'
    """
    
    try:
        profile = session.sql(profile_query).collect()[0].as_dict()
    except:
        profile = {}
    
    patient_context = "PATIENT PROFILE:\n"
    if profile:
        if profile.get('AGE'):
            patient_context += f"- Age: {profile['AGE']}\n"
        
        conditions = []
        if profile.get('HAS_DIABETES'): conditions.append("diabetes")
        if profile.get('HAS_HYPERTENSION'): conditions.append("hypertension")
        if profile.get('HAS_HEART_DISEASE'): conditions.append("heart disease")
        if profile.get('HAS_MENTAL_HEALTH_HISTORY'): conditions.append("mental health history")
        if profile.get('HAS_PCOS'): conditions.append("PCOS")
        if profile.get('IS_PREGNANT'): 
            conditions.append(f"pregnant (trimester {profile.get('PREGNANCY_TRIMESTER', '?')})")
        
        if conditions:
            patient_context += f"- Conditions: {', '.join(conditions)}\n"
        
        if profile.get('BMI'):
            patient_context += f"- BMI: {profile['BMI']}\n"
    
    # Current conversation context
    current_context = ""
    if current_history:
        current_context = "\n\nCURRENT CONVERSATION:\n"
        for idx, msg in enumerate(current_history, 1):
            current_context += f"Turn {idx} - User: {msg['user']}\n"
            current_context += f"Turn {idx} - Assistant: {msg['assistant']}\n\n"
    
    # Semantic search context
    semantic_context = ""
    if similar_conversations:
        semantic_context = "\n\nRELEVANT PAST CONVERSATIONS:\n"
        for idx, conv in enumerate(similar_conversations, 1):
            semantic_context += f"\n[Past discussion {idx} - {conv.get('date', 'recent')}]\n"
            semantic_context += f"User: {conv['user_message'][:250]}...\n"
            semantic_context += f"Assistant: {conv['assistant_response'][:250]}...\n"
    
    # Metrics context
    metrics_context = ""
    if metric_trends:
        metrics_context = "\n\nHEALTH METRICS TRACKING:\n"
        for metric_type, trend_data in metric_trends.items():
            metric_name = metric_type.replace('_', ' ').title()
            current = trend_data.get('current_value')
            first = trend_data.get('first_value')
            trend = trend_data.get('trend')
            change = trend_data.get('percent_change', 0)
            points = trend_data.get('data_points', 0)
            
            if current is not None and points >= 2:
                metrics_context += f"- {metric_name}: {first} → {current} ({trend}, {change:+.1f}% change)\n"
            elif current is not None:
                metrics_context += f"- {metric_name}: {current} (single reading)\n"
    
    # Summaries context
    summaries_context = ""
    if recent_summaries:
        summaries_context = "\n\nRECENT SUMMARIES:\n"
        for idx, summ in enumerate(recent_summaries, 1):
            summaries_context += f"- {summ.get('summary', '')}\n"
    
    return {
        "patient_context": patient_context,
        "current_context": current_context,
        "semantic_context": semantic_context,
        "metrics_context": metrics_context,
        "summaries_context": summaries_context,
        "context_stats": {
            "current_turns": len(current_history),
            "similar_found": len(similar_conversations),
            "metrics_tracked": len(metric_trends),
            "summaries_available": len(recent_summaries)
        }
    }
$$;

GRANT USAGE ON PROCEDURE WELLNEST.USER_MANAGEMENT.GET_SMART_CONTEXT(STRING, STRING, STRING, STRING) 
    TO ROLE TRAINING_ROLE;

-- 1. Check if service was created successfully
SHOW CORTEX SEARCH SERVICES IN SCHEMA WELLNEST.USER_MANAGEMENT;

-- 2. Check service status
DESC CORTEX SEARCH SERVICE WELLNEST.USER_MANAGEMENT.CONVERSATION_SEARCH_SERVICE;





-- Wait a moment for the service to be ready, then test:

-- Test 1: Simple search (no filters)
SELECT *
FROM TABLE(
    WELLNEST.USER_MANAGEMENT.CONVERSATION_SEARCH_SERVICE(
        query => 'health',
        limit => 5
    )
);

-- If that works, test with filters:

-- Test 2: Search with user filter
SELECT 
    user_message,
    assistant_response,
    conversation_date,
    routed_to_domain
FROM TABLE(
    WELLNEST.USER_MANAGEMENT.CONVERSATION_SEARCH_SERVICE(
        query => 'blood pressure',
        filter => {
            '@eq': {
                'user_id': 'admin-test-user-001'
            }
        },
        limit => 3
    )
);






-- =============================================================================
-- UPDATED: GET_SMART_CONTEXT with correct Cortex Search syntax
-- =============================================================================

CREATE OR REPLACE PROCEDURE WELLNEST.USER_MANAGEMENT.GET_SMART_CONTEXT(
    USER_QUERY STRING,
    USER_ID STRING,
    DOMAIN STRING,
    SESSION_ID STRING
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_smart_context'
AS
$$
import json

def get_smart_context(session, user_query, user_id, domain, session_id):
    """Get smart context using Cortex Search"""
    
    # Current session
    current_query = f"""
    SELECT USER_MESSAGE, ASSISTANT_RESPONSE
    FROM WELLNEST.USER_MANAGEMENT.CONVERSATION_HISTORY
    WHERE USER_ID = '{user_id}' AND SESSION_ID = '{session_id}'
    ORDER BY MESSAGE_TIMESTAMP DESC
    LIMIT 5
    """
    
    try:
        current = session.sql(current_query).collect()
        current_history = [{"user": r['USER_MESSAGE'], "assistant": r['ASSISTANT_RESPONSE']} for r in current]
        current_history.reverse()
    except:
        current_history = []
    
    # Cortex Search - TRY DIFFERENT SYNTAXES
    query_escaped = user_query.replace("'", "''")
    
    # Try syntax 1: !SEARCH method
    search_query = f"""
    SELECT 
        user_message,
        assistant_response,
        conversation_date,
        routed_to_domain
    FROM TABLE(
        WELLNEST.USER_MANAGEMENT.CONVERSATION_SEARCH_SERVICE!SEARCH(
            query => '{query_escaped}',
            filter => {{
                '@eq': {{
                    'user_id': '{user_id}',
                    'routed_to_domain': '{domain}'
                }}
            }}
        )
    )
    LIMIT 3
    """
    
    similar_conversations = []
    
    try:
        search_results = session.sql(search_query).collect()
        similar_conversations = [
            {
                "user_message": r['USER_MESSAGE'],
                "assistant_response": r['ASSISTANT_RESPONSE'],
                "date": str(r['CONVERSATION_DATE'])
            }
            for r in search_results
        ]
    except Exception as e:
        # If Cortex Search fails, fallback to keyword search
        search_error = str(e)
        
        # Fallback: Simple keyword search
        keywords = []
        query_lower = user_query.lower()
        
        if any(word in query_lower for word in ['blood pressure', 'bp', 'hypertension']):
            keywords.append("blood pressure")
        if any(word in query_lower for word in ['blood sugar', 'glucose', 'diabetes']):
            keywords.append("blood sugar")
        if any(word in query_lower for word in ['cholesterol', 'ldl', 'hdl']):
            keywords.append("cholesterol")
        
        if keywords:
            like_conditions = [f"LOWER(USER_MESSAGE) LIKE '%{kw}%'" for kw in keywords]
            keyword_filter = " OR ".join(like_conditions)
            
            fallback_query = f"""
            SELECT 
                USER_MESSAGE,
                ASSISTANT_RESPONSE,
                DATE(MESSAGE_TIMESTAMP) as conversation_date
            FROM WELLNEST.USER_MANAGEMENT.CONVERSATION_HISTORY
            WHERE USER_ID = '{user_id}'
              AND ROUTED_TO_DOMAIN = '{domain}'
              AND SESSION_ID != '{session_id}'
              AND ({keyword_filter})
            ORDER BY MESSAGE_TIMESTAMP DESC
            LIMIT 3
            """
            
            try:
                fallback_results = session.sql(fallback_query).collect()
                similar_conversations = [
                    {
                        "user_message": r['USER_MESSAGE'],
                        "assistant_response": r['ASSISTANT_RESPONSE'],
                        "date": str(r['CONVERSATION_DATE'])
                    }
                    for r in fallback_results
                ]
            except:
                pass
    
    # Get metrics (same as before)
    metric_trends = {}
    
    if domain == 'LIFESTYLE_DISEASES':
        metric_types = ['blood_pressure_systolic', 'blood_sugar', 'hba1c', 'weight']
    elif domain == 'WOMEN_WELLNESS':
        metric_types = ['blood_pressure_systolic', 'blood_sugar', 'weight']
    else:
        metric_types = []
    
    for metric_type in metric_types:
        trend_query = f"""
        SELECT METRIC_VALUE, MEASUREMENT_DATE
        FROM WELLNEST.MEDICAL_DATA.HEALTH_METRICS
        WHERE USER_ID = '{user_id}'
          AND METRIC_TYPE = '{metric_type}'
          AND MEASUREMENT_DATE >= DATEADD(day, -90, CURRENT_DATE())
        ORDER BY MEASUREMENT_DATE ASC
        """
        
        try:
            results = session.sql(trend_query).collect()
            
            if results:
                values = [r['METRIC_VALUE'] for r in results]
                
                if len(values) >= 2:
                    change = ((values[-1] - values[0]) / values[0]) * 100
                    trend = "stable" if abs(change) < 5 else ("increasing" if change > 0 else "decreasing")
                else:
                    trend = "single_reading"
                    change = 0
                
                metric_trends[metric_type] = {
                    "current_value": values[-1],
                    "first_value": values[0],
                    "trend": trend,
                    "percent_change": round(change, 1),
                    "data_points": len(values)
                }
        except:
            pass
    
    # Get profile
    profile_query = f"""
    SELECT u.AGE, u.GENDER, p.BMI, p.HAS_DIABETES, p.HAS_HYPERTENSION, 
           p.HAS_HEART_DISEASE, p.HAS_MENTAL_HEALTH_HISTORY, p.HAS_PCOS,
           p.IS_PREGNANT, p.PREGNANCY_TRIMESTER, p.SMOKING_STATUS, p.EXERCISE_FREQUENCY
    FROM WELLNEST.USER_MANAGEMENT.USERS u
    LEFT JOIN WELLNEST.USER_MANAGEMENT.USER_MEDICAL_PROFILES p ON u.USER_ID = p.USER_ID
    WHERE u.USER_ID = '{user_id}'
    """
    
    try:
        profile = session.sql(profile_query).collect()[0].as_dict()
    except:
        profile = {}
    
    # Build contexts
    patient_context = "PATIENT PROFILE:\n"
    if profile:
        if profile.get('AGE'):
            patient_context += f"- Age: {profile['AGE']}\n"
        
        conditions = []
        if profile.get('HAS_DIABETES'): conditions.append("diabetes")
        if profile.get('HAS_HYPERTENSION'): conditions.append("hypertension")
        if profile.get('HAS_HEART_DISEASE'): conditions.append("heart disease")
        if profile.get('HAS_MENTAL_HEALTH_HISTORY'): conditions.append("mental health history")
        if profile.get('HAS_PCOS'): conditions.append("PCOS")
        if profile.get('IS_PREGNANT'): 
            conditions.append(f"pregnant (trimester {profile.get('PREGNANCY_TRIMESTER', '?')})")
        
        if conditions:
            patient_context += f"- Medical conditions: {', '.join(conditions)}\n"
        
        if profile.get('BMI'):
            patient_context += f"- BMI: {profile['BMI']}\n"
    
    current_context = ""
    if current_history:
        current_context = "\n\nCURRENT CONVERSATION:\n"
        for idx, msg in enumerate(current_history, 1):
            current_context += f"\nTurn {idx}:\nUser: {msg['user']}\nAssistant: {msg['assistant']}\n"
    
    semantic_context = ""
    if similar_conversations:
        semantic_context = "\n\nRELEVANT PAST CONVERSATIONS:\n"
        for idx, conv in enumerate(similar_conversations, 1):
            semantic_context += f"\n[Past discussion {idx} - {conv['date']}]\n"
            semantic_context += f"User: {conv['user_message'][:250]}...\n"
            semantic_context += f"Assistant: {conv['assistant_response'][:250]}...\n"
    
    metrics_context = ""
    if metric_trends:
        metrics_context = "\n\nHEALTH METRICS TRACKING:\n"
        for metric_type, data in metric_trends.items():
            metric_name = metric_type.replace('_', ' ').title()
            if data['data_points'] >= 2:
                metrics_context += f"- {metric_name}: {data['first_value']} → {data['current_value']} ({data['trend']}, {data['percent_change']:+.1f}%)\n"
            else:
                metrics_context += f"- {metric_name}: {data['current_value']}\n"
    
    return {
        "patient_context": patient_context,
        "current_context": current_context,
        "semantic_context": semantic_context,
        "metrics_context": metrics_context,
        "context_stats": {
            "current_turns": len(current_history),
            "similar_found": len(similar_conversations),
            "metrics_tracked": len(metric_trends),
            "search_method": "cortex_search" if similar_conversations else "fallback_keywords"
        }
    }
$$;

GRANT USAGE ON PROCEDURE WELLNEST.USER_MANAGEMENT.GET_SMART_CONTEXT(STRING, STRING, STRING, STRING) 
    TO ROLE training_role;



-- Test the smart context procedure
CALL WELLNEST.USER_MANAGEMENT.GET_SMART_CONTEXT(
    'What helps lower blood pressure?',
    'admin-test-user-001',
    'LIFESTYLE_DISEASES',
    'test-session-123'
);







-- =============================================================================
-- STEP 1A: EXTRACT_AND_SAVE_METRICS
-- =============================================================================

CREATE OR REPLACE PROCEDURE WELLNEST.MEDICAL_DATA.EXTRACT_AND_SAVE_METRICS(
    USER_MESSAGE STRING,
    ASSISTANT_RESPONSE STRING,
    USER_ID STRING,
    CONVERSATION_ID STRING,
    DOMAIN STRING
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'extract_metrics'
AS
$$
import json
import re
import uuid

def extract_metrics(session, user_message, assistant_response, user_id, conversation_id, domain):
    """Extract health metrics using pattern matching"""
    
    if domain not in ['LIFESTYLE_DISEASES', 'WOMEN_WELLNESS']:
        return {"extracted": 0, "message": "No metrics for this domain", "debug": f"Domain: {domain}"}
    
    metrics = []
    combined_text = f"{user_message} {assistant_response}"
    
    # Blood Pressure: Match "150/95" pattern
    bp_pattern = r'(\d{2,3})\s*/\s*(\d{2,3})'
    bp_matches = re.findall(bp_pattern, combined_text)
    
    for systolic, diastolic in bp_matches:
        systolic_val = int(systolic)
        diastolic_val = int(diastolic)
        
        # Validate reasonable BP range
        if 70 <= systolic_val <= 250 and 40 <= diastolic_val <= 150:
            metrics.append({
                "type": "blood_pressure_systolic",
                "value": systolic_val,
                "unit": "mmHg"
            })
            metrics.append({
                "type": "blood_pressure_diastolic",
                "value": diastolic_val,
                "unit": "mmHg"
            })
    
    # Blood Sugar: Match numbers after "blood sugar", "glucose", "BS"
    bs_patterns = [
        r'blood\s+sugar[:\s]+(\d{2,3})',
        r'glucose[:\s]+(\d{2,3})',
        r'\bbs[:\s]+(\d{2,3})',
        r'sugar\s+level[:\s]+(\d{2,3})'
    ]
    
    for pattern in bs_patterns:
        for match in re.finditer(pattern, combined_text, re.IGNORECASE):
            bs_val = int(match.group(1))
            if 40 <= bs_val <= 600:
                metrics.append({
                    "type": "blood_sugar",
                    "value": bs_val,
                    "unit": "mg/dL"
                })
    
    # HbA1c: Match decimal numbers after "hba1c" or "a1c"
    hba1c_pattern = r'(?:hba1c|a1c)[:\s]+(\d+\.?\d*)'
    for match in re.finditer(hba1c_pattern, combined_text, re.IGNORECASE):
        hba1c_val = float(match.group(1))
        if 3.0 <= hba1c_val <= 20.0:
            metrics.append({
                "type": "hba1c",
                "value": hba1c_val,
                "unit": "%"
            })
    
    # Remove duplicates (keep first occurrence)
    seen = set()
    unique_metrics = []
    for m in metrics:
        key = (m['type'], m['value'])
        if key not in seen:
            seen.add(key)
            unique_metrics.append(m)
    
    # Save metrics to database
    saved_count = 0
    errors = []
    
    for metric in unique_metrics:
        metric_id = str(uuid.uuid4())
        metric_type = metric['type']
        value = metric['value']
        unit = metric['unit']
        
        # Determine severity
        is_abnormal = False
        severity = 'normal'
        
        if metric_type == 'blood_pressure_systolic':
            if value >= 180: 
                is_abnormal, severity = True, 'severe'
            elif value >= 140: 
                is_abnormal, severity = True, 'moderate'
            elif value >= 130: 
                is_abnormal, severity = True, 'mild'
        elif metric_type == 'blood_pressure_diastolic':
            if value >= 110: 
                is_abnormal, severity = True, 'severe'
            elif value >= 90: 
                is_abnormal, severity = True, 'moderate'
            elif value >= 80: 
                is_abnormal, severity = True, 'mild'
        elif metric_type == 'blood_sugar':
            if value >= 250: 
                is_abnormal, severity = True, 'severe'
            elif value >= 180: 
                is_abnormal, severity = True, 'moderate'
            elif value >= 140: 
                is_abnormal, severity = True, 'mild'
        elif metric_type == 'hba1c':
            if value >= 9.0: 
                is_abnormal, severity = True, 'severe'
            elif value >= 7.0: 
                is_abnormal, severity = True, 'moderate'
            elif value >= 6.5: 
                is_abnormal, severity = True, 'mild'
        
        insert_query = f"""
        INSERT INTO WELLNEST.MEDICAL_DATA.HEALTH_METRICS (
            METRIC_ID, USER_ID, CONVERSATION_ID, METRIC_TYPE,
            METRIC_VALUE, METRIC_UNIT, MEASUREMENT_DATE, REPORTED_DATE,
            SOURCE, CONFIDENCE_SCORE, IS_ABNORMAL, SEVERITY
        ) VALUES (
            '{metric_id}', 
            '{user_id}', 
            '{conversation_id}', 
            '{metric_type}',
            {value}, 
            '{unit}', 
            CURRENT_DATE(), 
            CURRENT_TIMESTAMP(),
            'conversation_extracted', 
            0.95,
            {is_abnormal}, 
            '{severity}'
        )
        """
        
        try:
            session.sql(insert_query).collect()
            saved_count += 1
        except Exception as e:
            errors.append(f"{metric_type}: {str(e)}")
    
    return {
        "extracted": saved_count,
        "total_found": len(unique_metrics),
        "method": "pattern_matching",
        "errors": errors if errors else None,
        "debug_text": combined_text[:200]
    }
$$;

GRANT USAGE ON PROCEDURE WELLNEST.MEDICAL_DATA.EXTRACT_AND_SAVE_METRICS(STRING, STRING, STRING, STRING, STRING) 
    TO ROLE SYSADMIN;

SELECT '✅ EXTRACT_AND_SAVE_METRICS updated successfully' AS STATUS;


-- =============================================================================
-- STEP 1B: GET_METRIC_TRENDS
-- =============================================================================

CREATE OR REPLACE PROCEDURE WELLNEST.MEDICAL_DATA.GET_METRIC_TRENDS(
    USER_ID STRING,
    METRIC_TYPE STRING,
    DAYS_BACK INTEGER
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_trends'
AS
$$
import json

def get_trends(session, user_id, metric_type, days_back):
    """Get metric trends"""
    
    query = f"""
    SELECT METRIC_VALUE, METRIC_UNIT, MEASUREMENT_DATE
    FROM WELLNEST.MEDICAL_DATA.HEALTH_METRICS
    WHERE USER_ID = '{user_id}'
      AND METRIC_TYPE = '{metric_type}'
      AND MEASUREMENT_DATE >= DATEADD(day, -{days_back}, CURRENT_DATE())
    ORDER BY MEASUREMENT_DATE ASC
    """
    
    try:
        results = session.sql(query).collect()
        
        if not results:
            return {"metric_type": metric_type, "data_points": 0, "trend": "no_data"}
        
        values = [r['METRIC_VALUE'] for r in results]
        dates = [str(r['MEASUREMENT_DATE']) for r in results]
        
        if len(values) >= 2:
            change = ((values[-1] - values[0]) / values[0]) * 100
            trend = "stable" if abs(change) < 5 else ("increasing" if change > 0 else "decreasing")
            percent_change = round(change, 1)
        else:
            trend = "single_reading"
            percent_change = 0
        
        return {
            "metric_type": metric_type,
            "data_points": len(values),
            "current_value": values[-1],
            "first_value": values[0],
            "trend": trend,
            "percent_change": percent_change,
            "measurements": [{"date": dates[i], "value": values[i]} for i in range(len(values))],
            "unit": results[0]['METRIC_UNIT']
        }
    
    except Exception as e:
        return {"error": str(e), "data_points": 0}
$$;

GRANT USAGE ON PROCEDURE WELLNEST.MEDICAL_DATA.GET_METRIC_TRENDS(STRING, STRING, INTEGER) 
    TO ROLE SYSADMIN;


-- =============================================================================
-- STEP 1C: SUMMARIZE_SESSION
-- =============================================================================

CREATE OR REPLACE PROCEDURE WELLNEST.USER_MANAGEMENT.SUMMARIZE_SESSION(
    USER_ID STRING,
    SESSION_ID STRING
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'summarize_session'
AS
$$
import json

def summarize_session(session, user_id, session_id):
    """Summarize conversation session"""
    
    messages_query = f"""
    SELECT USER_MESSAGE, ASSISTANT_RESPONSE, ROUTED_TO_DOMAIN
    FROM WELLNEST.USER_MANAGEMENT.CONVERSATION_HISTORY
    WHERE USER_ID = '{user_id}' AND SESSION_ID = '{session_id}'
    ORDER BY MESSAGE_TIMESTAMP ASC
    """
    
    try:
        results = session.sql(messages_query).collect()
        
        if not results or len(results) < 2:
            return {"summary_created": False, "reason": "Not enough messages"}
        
        conversation_text = ""
        for idx, row in enumerate(results, 1):
            conversation_text += f"\nTurn {idx}:\nUser: {row['USER_MESSAGE']}\nAssistant: {row['ASSISTANT_RESPONSE']}\n"
        
        domain = results[0]['ROUTED_TO_DOMAIN']
        
        summary_prompt = f"""Summarize this conversation.

{conversation_text}

JSON format:
{{
    "main_topic": "topic",
    "key_concerns": ["c1", "c2"],
    "recommendations": ["r1", "r2"],
    "metrics_mentioned": {{"bp": 150}},
    "summary_text": "2-3 sentences"
}}"""

        prompt_escaped = summary_prompt.replace("'", "''")
        
        claude_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-sonnet-4', '{prompt_escaped}') AS summary
        """
        
        result = session.sql(claude_query).collect()
        response = result[0]['SUMMARY'].strip()
        
        if '```json' in response:
            response = response.split('```json')[1].split('```')[0].strip()
        elif '```' in response:
            response = response.split('```')[1].split('```')[0].strip()
        
        summary_data = json.loads(response)
        summary_id = str(__import__('uuid').uuid4())
        
        concerns = summary_data.get('key_concerns', [])
        topics_sql = "NULL"
        if concerns:
            topics_sql = f"ARRAY_CONSTRUCT('" + "', '".join(concerns) + "')"
        
        metrics_json = json.dumps(summary_data.get('metrics_mentioned', {})).replace("'", "''")
        summary_escaped = summary_data.get('summary_text', '').replace("'", "''")
        
        insert_query = f"""
        INSERT INTO WELLNEST.USER_MANAGEMENT.CONVERSATION_SUMMARIES (
            SUMMARY_ID, USER_ID, SUMMARY_TYPE, SUMMARY_TEXT,
            KEY_TOPICS, DOMAIN, MENTIONED_METRICS,
            TIME_PERIOD_START, TIME_PERIOD_END, CREATED_AT
        )
        SELECT 
            '{summary_id}', '{user_id}', 'session_summary', '{summary_escaped}',
            {topics_sql}, '{domain}', PARSE_JSON('{metrics_json}'),
            MIN(MESSAGE_TIMESTAMP), MAX(MESSAGE_TIMESTAMP), CURRENT_TIMESTAMP()
        FROM WELLNEST.USER_MANAGEMENT.CONVERSATION_HISTORY
        WHERE SESSION_ID = '{session_id}'
        """
        
        session.sql(insert_query).collect()
        
        return {"summary_created": True, "summary_id": summary_id, "summary": summary_data}
    
    except Exception as e:
        return {"summary_created": False, "error": str(e)}
$$;

GRANT USAGE ON PROCEDURE WELLNEST.USER_MANAGEMENT.SUMMARIZE_SESSION(STRING, STRING) 
    TO ROLE SYSADMIN;


-- =============================================================================
-- STEP 1D: UPDATED QUERY_SPECIALIST_LLM (Using Smart Context)
-- =============================================================================

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

def query_specialist(session, user_query, domain, user_id, specialist_model, session_id):
    """Query specialist with smart context"""
    
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
    
    # System prompts by domain
    system_prompts = {
        "LIFESTYLE_DISEASES": """You are a healthcare assistant for lifestyle diseases (diabetes, hypertension, cardiovascular health).

You have access to patient's medical profile, conversation history, and tracked health metrics.

Provide personalized responses that:
- Reference past discussions when relevant ("Last time you mentioned...")
- Acknowledge metric improvements ("Your BP has improved from 150 to 135!")
- Show continuity in care
- Build on previous recommendations

Important: Cannot diagnose or prescribe. Recommend professional consultation.""",

        "MENTAL_HEALTH": """You are a mental health assistant for anxiety, depression, stress, and sleep disorders.

You remember previous mental health discussions and can provide therapeutic continuity.

Provide empathetic support that:
- Acknowledges progress or challenges
- References coping strategies from past chats
- Shows understanding of ongoing patterns

SAFETY: If suicidal thoughts, provide crisis resources (988) immediately.""",

        "WOMEN_WELLNESS": """You are a women's health assistant for pregnancy, PCOS, and reproductive health.

You track pregnancy progression and remember previous symptoms/concerns.

Provide supportive guidance that:
- Tracks pregnancy milestones
- References previous discussions
- Monitors relevant health metrics

Pregnancy complications need immediate medical evaluation."""
    }
    
    system_prompt = system_prompts.get(domain, system_prompts["LIFESTYLE_DISEASES"])
    user_query_escaped = user_query.replace("'", "''")
    
    full_prompt = f"""{system_prompt}

{context.get('patient_context', '')}

{context.get('metrics_context', '')}

{context.get('current_context', '')}

{context.get('semantic_context', '')}

CURRENT QUESTION: {user_query_escaped}

Provide a helpful, personalized response:"""

    prompt_escaped = full_prompt.replace("'", "''")
    
    cortex_query = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        '{specialist_model}',
        '{prompt_escaped}'
    ) AS response
    """
    
    try:
        result = session.sql(cortex_query).collect()
        return result[0]['RESPONSE']
    except Exception as e:
        return f"Error: {str(e)}"
$$;

GRANT USAGE ON PROCEDURE WELLNEST.USER_MANAGEMENT.QUERY_SPECIALIST_LLM(STRING, STRING, STRING, STRING, STRING) 
    TO ROLE SYSADMIN;





-- Check all procedures were created
SELECT 
    PROCEDURE_SCHEMA,
    PROCEDURE_NAME,
    ARGUMENT_SIGNATURE
FROM WELLNEST.INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_SCHEMA IN ('USER_MANAGEMENT', 'MEDICAL_DATA')
  AND PROCEDURE_NAME IN (
      'CLASSIFY_USER_QUERY',
      'GET_SMART_CONTEXT',
      'QUERY_SPECIALIST_LLM',
      'EXTRACT_AND_SAVE_METRICS',
      'GET_METRIC_TRENDS',
      'SUMMARIZE_SESSION'
  )
ORDER BY PROCEDURE_SCHEMA, PROCEDURE_NAME;








-- Fix the CONVERSATION_ID column size
ALTER TABLE WELLNEST.MEDICAL_DATA.HEALTH_METRICS 
    ALTER COLUMN CONVERSATION_ID SET DATA TYPE VARCHAR(100);

-- Verify the change
DESC TABLE WELLNEST.MEDICAL_DATA.HEALTH_METRICS;

-- Test metric extraction again
CALL WELLNEST.MEDICAL_DATA.EXTRACT_AND_SAVE_METRICS(
    'My BP is 150/95',
    'Your blood pressure is elevated',
    '599e410e-1f0f-4354-ab9c-acd3e004b014',
    '422bee32' || UUID_STRING(),
    'LIFESTYLE_DISEASES'
);

-- Check result
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));


SELECT 
    METRIC_TYPE,
    METRIC_VALUE,
    METRIC_UNIT,
    MEASUREMENT_DATE,
    SEVERITY,
    SOURCE
FROM WELLNEST.MEDICAL_DATA.HEALTH_METRICS
WHERE USER_ID = '599e410e-1f0f-4354-ab9c-acd3e004b014'
ORDER BY CREATED_AT DESC;

-- =============================================================================
-- STEP 1: CREATE HEALTH_METRICS TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS WELLNEST.MEDICAL_DATA.HEALTH_METRICS (
    METRIC_ID VARCHAR(36) PRIMARY KEY,
    USER_ID VARCHAR(36) NOT NULL,
    CONVERSATION_ID VARCHAR(36),
    
    -- Metric details
    METRIC_TYPE VARCHAR(50) NOT NULL,  -- blood_pressure_systolic, blood_sugar, hba1c, weight, etc.
    METRIC_VALUE FLOAT NOT NULL,
    METRIC_UNIT VARCHAR(20),
    
    -- Temporal info
    MEASUREMENT_DATE DATE,
    REPORTED_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Source and confidence
    SOURCE VARCHAR(50) DEFAULT 'conversation_extracted',  -- 'conversation_extracted', 'document_extracted', 'manual_entry'
    CONFIDENCE_SCORE FLOAT DEFAULT 1.0,
    
    -- Clinical significance
    IS_ABNORMAL BOOLEAN DEFAULT FALSE,
    SEVERITY VARCHAR(20),  -- 'normal', 'mild', 'moderate', 'severe'
    
    -- Metadata
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    NOTES VARCHAR(500),
    
    CONSTRAINT fk_health_metrics_user 
        FOREIGN KEY (USER_ID) 
        REFERENCES WELLNEST.USER_MANAGEMENT.USERS(USER_ID)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_health_metrics_user 
    ON WELLNEST.MEDICAL_DATA.HEALTH_METRICS(USER_ID);

CREATE INDEX IF NOT EXISTS idx_health_metrics_type 
    ON WELLNEST.MEDICAL_DATA.HEALTH_METRICS(METRIC_TYPE);

CREATE INDEX IF NOT EXISTS idx_health_metrics_date 
    ON WELLNEST.MEDICAL_DATA.HEALTH_METRICS(MEASUREMENT_DATE);

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE WELLNEST.MEDICAL_DATA.HEALTH_METRICS 
    TO ROLE SYSADMIN;

-- Verify table creation
SELECT 'HEALTH_METRICS table created successfully' AS STATUS;


-- =============================================================================
-- STEP 2: CREATE CONVERSATION_SUMMARIES TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS WELLNEST.USER_MANAGEMENT.CONVERSATION_SUMMARIES (
    SUMMARY_ID VARCHAR(36) PRIMARY KEY,
    USER_ID VARCHAR(36) NOT NULL,
    
    -- Summary type
    SUMMARY_TYPE VARCHAR(50) DEFAULT 'session_summary',  -- 'session_summary', 'weekly_summary', 'monthly_summary'
    
    -- Summary content
    SUMMARY_TEXT VARCHAR(5000),
    KEY_TOPICS ARRAY,
    DOMAIN VARCHAR(50),
    
    -- Extracted metrics mentioned
    MENTIONED_METRICS VARIANT,
    
    -- Time coverage
    TIME_PERIOD_START TIMESTAMP_NTZ,
    TIME_PERIOD_END TIMESTAMP_NTZ,
    
    -- Metadata
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    GENERATED_BY VARCHAR(50) DEFAULT 'claude-sonnet-4',
    
    CONSTRAINT fk_conversation_summaries_user 
        FOREIGN KEY (USER_ID) 
        REFERENCES WELLNEST.USER_MANAGEMENT.USERS(USER_ID)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_conversation_summaries_user 
    ON WELLNEST.USER_MANAGEMENT.CONVERSATION_SUMMARIES(USER_ID);

CREATE INDEX IF NOT EXISTS idx_conversation_summaries_time 
    ON WELLNEST.USER_MANAGEMENT.CONVERSATION_SUMMARIES(TIME_PERIOD_START);

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE WELLNEST.USER_MANAGEMENT.CONVERSATION_SUMMARIES 
    TO ROLE SYSADMIN;

-- Verify table creation
SELECT 'CONVERSATION_SUMMARIES table created successfully' AS STATUS;


-- =============================================================================
-- STEP 3: VERIFY ALL TABLES EXIST
-- =============================================================================

SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_TYPE,
    ROW_COUNT,
    CREATED
FROM WELLNEST.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('USER_MANAGEMENT', 'MEDICAL_DATA')
  AND TABLE_NAME IN ('HEALTH_METRICS', 'CONVERSATION_SUMMARIES', 'CONVERSATION_HISTORY')
ORDER BY TABLE_SCHEMA, TABLE_NAME;
```

---


