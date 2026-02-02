# =============================================================================
# WELLNEST STREAMLIT APPLICATION - WITH SMART CONTEXT INTEGRATION
# =============================================================================

import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import bcrypt
import uuid
from datetime import datetime, timedelta, date
import json

# Get Snowflake session
session = get_active_session()

# =============================================================================
# PAGE CONFIGURATION
# =============================================================================

st.set_page_config(
    page_title="WellNest - Health Assistant",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# INITIALIZE SESSION STATE
# =============================================================================

if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

if 'current_page' not in st.session_state:
    st.session_state.current_page = 'dashboard'

# =============================================================================
# CUSTOM CSS
# =============================================================================

st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
        font-weight: bold;
    }
    .sub-header {
        font-size: 1.2rem;
        text-align: center;
        color: #666;
        margin-bottom: 2rem;
    }
    .stButton>button {
        width: 100%;
        background-color: #1f77b4;
        color: white;
        border-radius: 5px;
        padding: 0.5rem;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# =============================================================================
# HELPER FUNCTIONS (EXISTING - PRESERVED)
# =============================================================================

def hash_password(password: str) -> str:
    """Hash a password using bcrypt"""
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')

def verify_password(password: str, hashed_password: str) -> bool:
    """Verify a password against its hash"""
    return bcrypt.checkpw(
        password.encode('utf-8'), 
        hashed_password.encode('utf-8')
    )

def validate_email(email: str) -> bool:
    """Basic email validation"""
    import re
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def validate_password_strength(password: str) -> tuple:
    """Validate password strength"""
    if len(password) < 8:
        return False, "Password must be at least 8 characters long"
    if not any(c.isupper() for c in password):
        return False, "Password must contain at least one uppercase letter"
    if not any(c.islower() for c in password):
        return False, "Password must contain at least one lowercase letter"
    if not any(c.isdigit() for c in password):
        return False, "Password must contain at least one number"
    return True, "Password is strong"

def calculate_bmi(weight_kg, height_cm):
    """Calculate BMI from weight and height"""
    if weight_kg and height_cm and weight_kg > 0 and height_cm > 0:
        height_m = height_cm / 100
        return round(weight_kg / (height_m ** 2), 1)
    return None

def get_bmi_category(bmi):
    """Get BMI category and color"""
    if not bmi:
        return "Unknown", "gray"
    elif bmi < 18.5:
        return "Underweight", "orange"
    elif bmi < 25:
        return "Normal Weight", "green"
    elif bmi < 30:
        return "Overweight", "orange"
    else:
        return "Obese", "red"

def calculate_age(dob):
    """Calculate age from date of birth"""
    if dob:
        today = date.today()
        return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    return None

def get_profile_completeness(profile):
    """Calculate profile completeness percentage"""
    fields_to_check = [
        'HEIGHT_CM', 'WEIGHT_KG', 'BLOOD_TYPE', 'SMOKING_STATUS',
        'ALCOHOL_CONSUMPTION', 'EXERCISE_FREQUENCY', 'EMERGENCY_CONTACT_NAME',
        'EMERGENCY_CONTACT_PHONE'
    ]
    completed = sum(1 for field in fields_to_check if profile.get(field) is not None)
    return int((completed / len(fields_to_check)) * 100)

def format_file_size(size_bytes: int) -> str:
    """Convert bytes to human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"

# =============================================================================
# DATABASE FUNCTIONS (EXISTING - PRESERVED)
# =============================================================================

def authenticate_user(email: str, password: str):
    """Authenticate user by email and password"""
    query = f"""
    SELECT 
        USER_ID, EMAIL, HASHED_PASSWORD, FULL_NAME, 
        ACCOUNT_STATUS, FAILED_LOGIN_ATTEMPTS, ACCOUNT_LOCKED_UNTIL
    FROM WELLNEST.USER_MANAGEMENT.USERS
    WHERE EMAIL = '{email}'
    """
    
    try:
        result = session.sql(query).collect()
        
        if not result:
            return None
        
        user = result[0]
        
        if user['ACCOUNT_LOCKED_UNTIL']:
            if datetime.now() < user['ACCOUNT_LOCKED_UNTIL']:
                st.error("⚠️ Account is temporarily locked. Please try again later.")
                return None
        
        if user['ACCOUNT_STATUS'] != 'active':
            st.error("⚠️ Account is not active. Please contact support.")
            return None
        
        if verify_password(password, user['HASHED_PASSWORD']):
            update_query = f"""
            UPDATE WELLNEST.USER_MANAGEMENT.USERS
            SET LAST_LOGIN = CURRENT_TIMESTAMP(),
                FAILED_LOGIN_ATTEMPTS = 0,
                ACCOUNT_LOCKED_UNTIL = NULL
            WHERE USER_ID = '{user['USER_ID']}'
            """
            session.sql(update_query).collect()
            
            return {
                'user_id': user['USER_ID'],
                'email': user['EMAIL'],
                'full_name': user['FULL_NAME']
            }
        else:
            failed_attempts = user['FAILED_LOGIN_ATTEMPTS'] + 1
            
            if failed_attempts >= 5:
                lock_until = datetime.now() + timedelta(minutes=15)
                update_query = f"""
                UPDATE WELLNEST.USER_MANAGEMENT.USERS
                SET FAILED_LOGIN_ATTEMPTS = {failed_attempts},
                    ACCOUNT_LOCKED_UNTIL = '{lock_until}'
                WHERE USER_ID = '{user['USER_ID']}'
                """
                session.sql(update_query).collect()
                st.error("🔒 Too many failed attempts. Account locked for 15 minutes.")
            else:
                update_query = f"""
                UPDATE WELLNEST.USER_MANAGEMENT.USERS
                SET FAILED_LOGIN_ATTEMPTS = {failed_attempts}
                WHERE USER_ID = '{user['USER_ID']}'
                """
                session.sql(update_query).collect()
                remaining = 5 - failed_attempts
                st.error(f"❌ Invalid password. {remaining} attempt(s) remaining.")
            
            return None
    
    except Exception as e:
        st.error(f"Authentication error: {str(e)}")
        return None

def register_user(email: str, password: str, full_name: str, 
                  date_of_birth: str = None, gender: str = None) -> bool:
    """Register a new user"""
    check_query = f"""
    SELECT EMAIL FROM WELLNEST.USER_MANAGEMENT.USERS WHERE EMAIL = '{email}'
    """
    
    try:
        existing = session.sql(check_query).collect()
        
        if existing:
            st.error("❌ An account with this email already exists.")
            return False
        
        user_id = str(uuid.uuid4())
        profile_id = str(uuid.uuid4())
        hashed_pw = hash_password(password)
        
        dob_sql = f"'{date_of_birth}'" if date_of_birth else "NULL"
        gender_sql = f"'{gender}'" if gender else "NULL"
        
        insert_query = f"""
        INSERT INTO WELLNEST.USER_MANAGEMENT.USERS (
            USER_ID, EMAIL, HASHED_PASSWORD, FULL_NAME,
            DATE_OF_BIRTH, GENDER, ACCOUNT_STATUS,
            EMAIL_VERIFIED, TERMS_ACCEPTED, PRIVACY_CONSENT, CREATED_AT
        ) VALUES (
            '{user_id}', '{email}', '{hashed_pw}', '{full_name}',
            {dob_sql}, {gender_sql}, 'active',
            FALSE, TRUE, TRUE, CURRENT_TIMESTAMP()
        )
        """
        
        session.sql(insert_query).collect()
        
        profile_query = f"""
        INSERT INTO WELLNEST.USER_MANAGEMENT.USER_MEDICAL_PROFILES (
            PROFILE_ID, USER_ID, CREATED_AT, LAST_UPDATED
        ) VALUES (
            '{profile_id}', '{user_id}',
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        )
        """
        
        session.sql(profile_query).collect()
        
        return True
    
    except Exception as e:
        st.error(f"Registration failed: {str(e)}")
        return False

def get_user_stats(user_id: str) -> dict:
    """Get user statistics for dashboard"""
    try:
        conv_query = f"""
        SELECT COUNT(*) AS CONV_COUNT
        FROM WELLNEST.USER_MANAGEMENT.CONVERSATION_HISTORY
        WHERE USER_ID = '{user_id}'
        """
        conv_result = session.sql(conv_query).collect()
        conv_count = conv_result[0]['CONV_COUNT'] if conv_result else 0
        
        doc_query = f"""
        SELECT COUNT(*) AS DOC_COUNT
        FROM WELLNEST.MEDICAL_DATA.UPLOADED_DOCUMENTS
        WHERE USER_ID = '{user_id}'
        """
        doc_result = session.sql(doc_query).collect()
        doc_count = doc_result[0]['DOC_COUNT'] if doc_result else 0
        
        profile_query = f"""
        SELECT 
            CASE WHEN HEIGHT_CM IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN WEIGHT_KG IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN BLOOD_TYPE IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN SMOKING_STATUS IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN EXERCISE_FREQUENCY IS NOT NULL THEN 1 ELSE 0 END
            AS COMPLETED_FIELDS
        FROM WELLNEST.USER_MANAGEMENT.USER_MEDICAL_PROFILES
        WHERE USER_ID = '{user_id}'
        """
        profile_result = session.sql(profile_query).collect()
        completed = profile_result[0]['COMPLETED_FIELDS'] if profile_result else 0
        profile_completeness = int((completed / 5) * 100)
        
        return {
            'conversations': conv_count,
            'documents': doc_count,
            'profile_completeness': profile_completeness
        }
    
    except Exception as e:
        st.error(f"Error fetching stats: {str(e)}")
        return {'conversations': 0, 'documents': 0, 'profile_completeness': 0}

def get_user_profile(user_id):
    """Fetch user profile from database"""
    query = f"""
    SELECT 
        u.USER_ID, u.EMAIL, u.FULL_NAME, u.DATE_OF_BIRTH, u.GENDER,
        u.PHONE_NUMBER, u.CREATED_AT,
        p.PROFILE_ID, p.HEIGHT_CM, p.WEIGHT_KG, p.BMI, p.BLOOD_TYPE,
        p.HAS_DIABETES, p.HAS_HYPERTENSION, p.HAS_HEART_DISEASE,
        p.HAS_MENTAL_HEALTH_HISTORY, p.HAS_PCOS,
        p.SMOKING_STATUS, p.ALCOHOL_CONSUMPTION, p.EXERCISE_FREQUENCY,
        p.IS_PREGNANT, p.PREGNANCY_TRIMESTER, p.MENSTRUAL_CYCLE_REGULAR,
        p.LAST_MENSTRUAL_PERIOD, p.EMERGENCY_CONTACT_NAME,
        p.EMERGENCY_CONTACT_PHONE, p.EMERGENCY_CONTACT_RELATIONSHIP,
        p.LAST_UPDATED
    FROM WELLNEST.USER_MANAGEMENT.USERS u
    LEFT JOIN WELLNEST.USER_MANAGEMENT.USER_MEDICAL_PROFILES p
        ON u.USER_ID = p.USER_ID
    WHERE u.USER_ID = '{user_id}'
    """
    
    result = session.sql(query).collect()
    
    if result:
        row = result[0]
        return {k: row[k] for k in row.asDict().keys()}
    return None

def update_user_info(user_id, full_name, phone_number):
    """Update basic user information"""
    phone_sql = f"'{phone_number}'" if phone_number else "NULL"
    
    query = f"""
    UPDATE WELLNEST.USER_MANAGEMENT.USERS
    SET FULL_NAME = '{full_name}', PHONE_NUMBER = {phone_sql}
    WHERE USER_ID = '{user_id}'
    """
    session.sql(query).collect()

def update_medical_profile(user_id, profile_data):
    """Update medical profile information"""
    set_clauses = []
    
    if profile_data.get('height_cm'):
        set_clauses.append(f"HEIGHT_CM = {profile_data['height_cm']}")
    if profile_data.get('weight_kg'):
        set_clauses.append(f"WEIGHT_KG = {profile_data['weight_kg']}")
    if profile_data.get('bmi'):
        set_clauses.append(f"BMI = {profile_data['bmi']}")
    if profile_data.get('blood_type'):
        set_clauses.append(f"BLOOD_TYPE = '{profile_data['blood_type']}'")
    
    set_clauses.append(f"HAS_DIABETES = {profile_data.get('has_diabetes', False)}")
    set_clauses.append(f"HAS_HYPERTENSION = {profile_data.get('has_hypertension', False)}")
    set_clauses.append(f"HAS_HEART_DISEASE = {profile_data.get('has_heart_disease', False)}")
    set_clauses.append(f"HAS_MENTAL_HEALTH_HISTORY = {profile_data.get('has_mental_health_history', False)}")
    set_clauses.append(f"HAS_PCOS = {profile_data.get('has_pcos', False)}")
    
    if profile_data.get('smoking_status'):
        set_clauses.append(f"SMOKING_STATUS = '{profile_data['smoking_status']}'")
    if profile_data.get('alcohol_consumption'):
        set_clauses.append(f"ALCOHOL_CONSUMPTION = '{profile_data['alcohol_consumption']}'")
    if profile_data.get('exercise_frequency'):
        set_clauses.append(f"EXERCISE_FREQUENCY = '{profile_data['exercise_frequency']}'")
    
    if profile_data.get('gender') == 'Female':
        set_clauses.append(f"IS_PREGNANT = {profile_data.get('is_pregnant', False)}")
        if profile_data.get('pregnancy_trimester'):
            set_clauses.append(f"PREGNANCY_TRIMESTER = {profile_data['pregnancy_trimester']}")
        set_clauses.append(f"MENSTRUAL_CYCLE_REGULAR = {profile_data.get('menstrual_cycle_regular', False)}")
        if profile_data.get('last_menstrual_period'):
            set_clauses.append(f"LAST_MENSTRUAL_PERIOD = '{profile_data['last_menstrual_period']}'")
    
    if profile_data.get('emergency_contact_name'):
        set_clauses.append(f"EMERGENCY_CONTACT_NAME = '{profile_data['emergency_contact_name']}'")
    if profile_data.get('emergency_contact_phone'):
        set_clauses.append(f"EMERGENCY_CONTACT_PHONE = '{profile_data['emergency_contact_phone']}'")
    if profile_data.get('emergency_contact_relationship'):
        set_clauses.append(f"EMERGENCY_CONTACT_RELATIONSHIP = '{profile_data['emergency_contact_relationship']}'")
    
    set_clauses.append("LAST_UPDATED = CURRENT_TIMESTAMP()")
    
    query = f"""
    UPDATE WELLNEST.USER_MANAGEMENT.USER_MEDICAL_PROFILES
    SET {', '.join(set_clauses)}
    WHERE USER_ID = '{user_id}'
    """
    
    session.sql(query).collect()

# =============================================================================
# CONVERSATION FUNCTIONS (EXISTING - PRESERVED)
# =============================================================================

def save_conversation(user_id: str, user_message: str, assistant_response: str,
                      routed_domain: str = None, urgency_level: str = None,
                      detected_symptoms: list = None):
    """Save a conversation turn"""
    conversation_id = str(uuid.uuid4())
    
    if 'session_id' not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
    
    session_id = st.session_state.session_id
    
    symptoms_sql = "NULL"
    if detected_symptoms and len(detected_symptoms) > 0:
        symptoms_clean = [s.replace("'", "''") for s in detected_symptoms]
        symptoms_str = "', '".join(symptoms_clean)
        symptoms_sql = f"ARRAY_CONSTRUCT('{symptoms_str}')"
    
    query = f"""
    INSERT INTO WELLNEST.USER_MANAGEMENT.CONVERSATION_HISTORY (
        CONVERSATION_ID,
        USER_ID,
        SESSION_ID,
        MESSAGE_TIMESTAMP,
        USER_MESSAGE,
        ASSISTANT_RESPONSE,
        ROUTED_TO_DOMAIN,
        URGENCY_LEVEL,
        DETECTED_SYMPTOMS
    ) 
    SELECT 
        '{conversation_id}',
        '{user_id}',
        '{session_id}',
        CURRENT_TIMESTAMP(),
        TO_VARCHAR(?),
        TO_VARCHAR(?),
        {f"'{routed_domain}'" if routed_domain else "NULL"},
        {f"'{urgency_level}'" if urgency_level else "NULL"},
        {symptoms_sql}
    """
    
    try:
        session.sql(query, params=[user_message, assistant_response]).collect()
        return True
    except Exception as e:
        st.error(f"❌ Error saving conversation: {str(e)}")
        return False

def get_conversation_history(user_id: str, limit: int = 50):
    """Retrieve conversation history for a user"""
    query = f"""
    SELECT 
        CONVERSATION_ID,
        SESSION_ID,
        MESSAGE_TIMESTAMP,
        USER_MESSAGE,
        ASSISTANT_RESPONSE,
        ROUTED_TO_DOMAIN,
        URGENCY_LEVEL,
        DETECTED_SYMPTOMS
    FROM WELLNEST.USER_MANAGEMENT.CONVERSATION_HISTORY
    WHERE USER_ID = '{user_id}'
    ORDER BY MESSAGE_TIMESTAMP DESC
    LIMIT {limit}
    """
    
    try:
        result = session.sql(query).collect()
        return [{k: row[k] for k in row.asDict().keys()} for row in result]
    except Exception as e:
        st.error(f"Error loading conversation history: {str(e)}")
        return []

def get_current_session_messages(user_id: str, session_id: str):
    """Get messages from current session only"""
    query = f"""
    SELECT 
        MESSAGE_TIMESTAMP,
        USER_MESSAGE,
        ASSISTANT_RESPONSE,
        ROUTED_TO_DOMAIN,
        URGENCY_LEVEL
    FROM WELLNEST.USER_MANAGEMENT.CONVERSATION_HISTORY
    WHERE USER_ID = '{user_id}' AND SESSION_ID = '{session_id}'
    ORDER BY MESSAGE_TIMESTAMP ASC
    """
    
    try:
        result = session.sql(query).collect()
        return [{k: row[k] for k in row.asDict().keys()} for row in result]
    except Exception as e:
        return []

# =============================================================================
# DOCUMENT FUNCTIONS (EXISTING - PRESERVED)
# =============================================================================

def save_uploaded_document(user_id: str, file_name: str, file_size: int, 
                           file_content: bytes, document_type: str = None):
    """Save uploaded document with content to database"""
    document_id = str(uuid.uuid4())
    
    import base64
    file_content_b64 = base64.b64encode(file_content).decode('utf-8')
    
    try:
        safe_filename = file_name.replace("'", "''")
        
        insert_query = f"""
        INSERT INTO WELLNEST.MEDICAL_DATA.UPLOADED_DOCUMENTS (
            DOCUMENT_ID,
            USER_ID,
            ORIGINAL_FILENAME,
            DOCUMENT_TYPE,
            UPLOAD_TIMESTAMP,
            FILE_SIZE_BYTES,
            FILE_CONTENT_BASE64,
            PROCESSING_STATUS,
            PROCESSING_STARTED_AT
        ) VALUES (
            '{document_id}',
            '{user_id}',
            '{safe_filename}',
            '{document_type if document_type else "unknown"}',
            CURRENT_TIMESTAMP(),
            {file_size},
            '{file_content_b64}',
            'pending',
            CURRENT_TIMESTAMP()
        )
        """
        
        session.sql(insert_query).collect()
        
        return document_id
    
    except Exception as e:
        st.error(f"Error saving document: {str(e)}")
        return None

def get_user_documents(user_id: str, limit: int = 50):
    """Retrieve all documents uploaded by a user"""
    query = f"""
    SELECT 
        DOCUMENT_ID,
        ORIGINAL_FILENAME,
        DOCUMENT_TYPE,
        UPLOAD_TIMESTAMP,
        FILE_SIZE_BYTES,
        PROCESSING_STATUS,
        PROCESSING_COMPLETED_AT,
        EXTRACTED_DATA,
        EXTRACTION_CONFIDENCE_SCORE,
        DETECTED_TEST_TYPES
    FROM WELLNEST.MEDICAL_DATA.UPLOADED_DOCUMENTS
    WHERE USER_ID = '{user_id}'
    ORDER BY UPLOAD_TIMESTAMP DESC
    LIMIT {limit}
    """
    
    try:
        result = session.sql(query).collect()
        return [{k: row[k] for k in row.asDict().keys()} for row in result]
    except Exception as e:
        st.error(f"Error loading documents: {str(e)}")
        return []

def update_document_processing_status(document_id: str, status: str, 
                                      extracted_data: dict = None,
                                      confidence_score: float = None,
                                      error_message: str = None):
    """Update document processing status after extraction"""
    set_clauses = [
        f"PROCESSING_STATUS = '{status}'",
        "PROCESSING_COMPLETED_AT = CURRENT_TIMESTAMP()"
    ]
    
    if extracted_data:
        json_str = json.dumps(extracted_data).replace("'", "''")
        set_clauses.append(f"EXTRACTED_DATA = PARSE_JSON('{json_str}')")
    
    if confidence_score is not None:
        set_clauses.append(f"EXTRACTION_CONFIDENCE_SCORE = {confidence_score}")
    
    if error_message:
        error_escaped = error_message.replace("'", "''")
        set_clauses.append(f"PROCESSING_ERROR_MESSAGE = '{error_escaped}'")
    
    query = f"""
    UPDATE WELLNEST.MEDICAL_DATA.UPLOADED_DOCUMENTS
    SET {', '.join(set_clauses)}
    WHERE DOCUMENT_ID = '{document_id}'
    """
    
    try:
        session.sql(query).collect()
        return True
    except Exception as e:
        st.error(f"Error updating document status: {str(e)}")
        return False

def delete_document(document_id: str, user_id: str):
    """Delete a document"""
    query = f"""
    DELETE FROM WELLNEST.MEDICAL_DATA.UPLOADED_DOCUMENTS
    WHERE DOCUMENT_ID = '{document_id}' AND USER_ID = '{user_id}'
    """
    
    try:
        session.sql(query).collect()
        return True
    except Exception as e:
        st.error(f"Error deleting document: {str(e)}")
        return False

def extract_document_type(filename: str) -> str:
    """Determine document type from filename"""
    filename_lower = filename.lower()
    
    if any(word in filename_lower for word in ['lab', 'blood', 'test', 'result']):
        return 'lab_report'
    elif any(word in filename_lower for word in ['prescription', 'rx', 'medication']):
        return 'prescription'
    elif any(word in filename_lower for word in ['xray', 'x-ray', 'mri', 'ct', 'scan', 'imaging']):
        return 'imaging'
    elif any(word in filename_lower for word in ['discharge', 'summary', 'hospital']):
        return 'discharge_summary'
    else:
        return 'other'

def process_pdf_document(file_content: bytes, filename: str) -> dict:
    """Process PDF document and extract medical information"""
    placeholder_data = {
        'extraction_method': 'placeholder',
        'status': 'awaiting_llm_integration',
        'message': 'Document uploaded successfully. LLM extraction will be implemented next.',
        'file_info': {
            'filename': filename,
            'size_bytes': len(file_content),
            'detected_type': extract_document_type(filename)
        }
    }
    
    return {
        'extracted_data': placeholder_data,
        'confidence_score': 0.0,
        'processing_status': 'pending'
    }

def get_document_stats(user_id: str) -> dict:
    """Get document statistics for dashboard"""
    try:
        query = f"""
        SELECT 
            COUNT(*) as TOTAL_DOCS,
            SUM(CASE WHEN PROCESSING_STATUS = 'completed' THEN 1 ELSE 0 END) as PROCESSED,
            SUM(CASE WHEN PROCESSING_STATUS = 'pending' THEN 1 ELSE 0 END) as PENDING,
            SUM(FILE_SIZE_BYTES) as TOTAL_SIZE
        FROM WELLNEST.MEDICAL_DATA.UPLOADED_DOCUMENTS
        WHERE USER_ID = '{user_id}'
        """
        
        result = session.sql(query).collect()
        
        if result:
            row = result[0]
            return {
                'total': row['TOTAL_DOCS'] or 0,
                'processed': row['PROCESSED'] or 0,
                'pending': row['PENDING'] or 0,
                'total_size': row['TOTAL_SIZE'] or 0
            }
        return {'total': 0, 'processed': 0, 'pending': 0, 'total_size': 0}
    
    except Exception as e:
        return {'total': 0, 'processed': 0, 'pending': 0, 'total_size': 0}

# =============================================================================
# EMERGENCY DETECTION (EXISTING - PRESERVED)
# =============================================================================

def detect_emergency_keywords(message: str) -> tuple:
    """Detect emergency keywords in user message"""
    message_lower = message.lower()
    
    emergency_keywords = [
        'chest pain', 'heart attack', 'stroke', 'seizure',
        'unconscious', 'severe bleeding', 'not breathing',
        'suicide', 'kill myself', 'end my life',
        'severe headache', 'can\'t breathe', 'choking'
    ]
    
    urgent_keywords = [
        'high fever', 'vomiting blood', 'severe pain',
        'can\'t move', 'vision loss', 'confusion',
        'severe allergic', 'broken bone', 'severe burn'
    ]
    
    symptom_keywords = [
        'headache', 'fever', 'cough', 'fatigue', 'nausea',
        'dizziness', 'pain', 'swelling', 'rash', 'shortness of breath',
        'anxiety', 'depression', 'insomnia', 'stress',
        'blood pressure', 'blood sugar', 'diabetes', 'hypertension'
    ]
    
    detected_symptoms = []
    
    for keyword in emergency_keywords:
        if keyword in message_lower:
            detected_symptoms.append(keyword)
            return True, 'emergency', detected_symptoms
    
    for keyword in urgent_keywords:
        if keyword in message_lower:
            detected_symptoms.append(keyword)
    
    if detected_symptoms:
        return False, 'urgent', detected_symptoms
    
    for keyword in symptom_keywords:
        if keyword in message_lower:
            detected_symptoms.append(keyword)
    
    if len(detected_symptoms) >= 3:
        return False, 'needs_attention', detected_symptoms
    elif detected_symptoms:
        return False, 'routine', detected_symptoms
    else:
        return False, 'routine', []

# =============================================================================
# 🆕 SMART CONTEXT FUNCTIONS - NEW ADDITIONS
# =============================================================================

def extract_and_track_metrics(user_message: str, assistant_response: str,
                              user_id: str, conversation_id: str, domain: str):
    """Extract and save health metrics from conversation"""
    try:
        result = session.call(
            'WELLNEST.MEDICAL_DATA.EXTRACT_AND_SAVE_METRICS',
            user_message,
            assistant_response,
            user_id,
            conversation_id,
            domain
        )
        
        data = result if isinstance(result, dict) else json.loads(result)
        
        if data.get('extracted', 0) > 0:
            with st.sidebar:
                st.success(f"📊 Tracked {data['extracted']} metric(s)")
        
        return data
    except Exception as e:
        return {"extracted": 0, "error": str(e)}

def display_metric_trends(user_id: str, domain: str):
    """Display tracked health metrics in sidebar"""
    if domain not in ['LIFESTYLE_DISEASES', 'WOMEN_WELLNESS']:
        return
    
    metrics_config = {
        'LIFESTYLE_DISEASES': [
            ('blood_pressure_systolic', 'BP (Systolic)', 'mmHg', True),
            ('blood_sugar', 'Blood Sugar', 'mg/dL', True),
            ('hba1c', 'HbA1c', '%', True),
            ('weight', 'Weight', 'kg', False)
        ],
        'WOMEN_WELLNESS': [
            ('blood_pressure_systolic', 'BP (Systolic)', 'mmHg', True),
            ('blood_sugar', 'Blood Sugar', 'mg/dL', True),
            ('weight', 'Weight', 'kg', False)
        ]
    }
    
    metrics = metrics_config.get(domain, [])
    displayed = False
    
    for metric_type, name, unit, lower_better in metrics:
        try:
            trend = session.call(
                'WELLNEST.MEDICAL_DATA.GET_METRIC_TRENDS',
                user_id,
                metric_type,
                30
            )
            
            data = trend if isinstance(trend, dict) else json.loads(trend)
            
            if data.get('data_points', 0) > 0:
                if not displayed:
                    with st.sidebar:
                        st.markdown("---")
                        st.markdown("### 📊 Tracked Metrics")
                        st.caption("Auto-extracted from chats")
                    displayed = True
                
                current = data['current_value']
                change = data.get('percent_change', 0)
                points = data['data_points']
                
                with st.sidebar:
                    st.metric(
                        label=name,
                        value=f"{current:.1f} {unit}",
                        delta=f"{change:+.1f}%" if abs(change) > 0.1 else "stable",
                        delta_color="inverse" if lower_better else "normal"
                    )
                    st.caption(f"Based on {points} reading{'s' if points > 1 else ''}")
        except:
            continue

def summarize_current_session():
    """Summarize session when starting new conversation"""
    if not st.session_state.get('session_id'):
        return False
    
    try:
        check = session.sql(f"""
            SELECT COUNT(*) as cnt
            FROM WELLNEST.USER_MANAGEMENT.CONVERSATION_HISTORY
            WHERE SESSION_ID = '{st.session_state.session_id}'
        """).collect()
        
        if check[0]['CNT'] < 2:
            return False
        
        result = session.call(
            'WELLNEST.USER_MANAGEMENT.SUMMARIZE_SESSION',
            st.session_state.user_id,
            st.session_state.session_id
        )
        
        data = result if isinstance(result, dict) else json.loads(result)
        return data.get('summary_created', False)
    except:
        return False

# =============================================================================
# LLM ROUTER & SPECIALIST (EXISTING - WITH SMART CONTEXT UPDATES)
# =============================================================================

def call_router_llm(user_message: str, user_id: str) -> dict:
    """Call the router stored procedure to classify user query"""
    try:
        result = session.call(
            'WELLNEST.USER_MANAGEMENT.CLASSIFY_USER_QUERY',
            user_message,
            user_id
        )
        
        if isinstance(result, str):
            classification = json.loads(result)
        else:
            classification = result
        
        return classification
    
    except Exception as e:
        st.error(f"🔴 Router classification error: {str(e)}")
        
        return {
            "domain": "LIFESTYLE_DISEASES",
            "urgency": "ROUTINE",
            "confidence": 0.5,
            "reasoning": f"Router error - using default routing: {str(e)}",
            "safety_flags": [],
            "specialist_model": "llama3.1-8b",
            "classification_status": "error"
        }

def call_specialist_llm(user_message: str, classification: dict, user_id: str) -> str:
    """
    🆕 UPDATED: Call specialist with SESSION_ID for smart context
    """
    try:
        # Ensure session_id exists
        if 'session_id' not in st.session_state:
            st.session_state.session_id = str(uuid.uuid4())
        
        # Call specialist with session_id for context-aware responses
        response = session.call(
            'WELLNEST.USER_MANAGEMENT.QUERY_SPECIALIST_LLM',
            user_message,                           # USER_QUERY
            classification['domain'],               # DOMAIN
            user_id,                                # USER_ID
            classification['specialist_model'],     # SPECIALIST_MODEL
            st.session_state.session_id             # 🆕 SESSION_ID for context
        )
        
        return response
    
    except Exception as e:
        st.error(f"🔴 Specialist LLM error: {str(e)}")
        
        return f"""I apologize, but I encountered an error processing your request.

**Error Details:** {str(e)}

**What you can do:**
- Try rephrasing your question
- Check back in a few moments
- Contact support if the issue persists

I'm here to help once the technical issue is resolved!"""

def remove_hallucinated_phrases(response: str, user_id: str) -> str:
    """Strip out hallucinated conversation references"""
    import re
    
    history = get_conversation_history(user_id, limit=1)
    has_history = len(history) > 0
    
    if not has_history:
        removal_patterns = [
            r"We've talked about [^.]+\.",
            r"we've talked about [^.]+\.",
            r"I remember [^.]+\.",
            r"we previously discussed [^.]+\.",
            r"We previously discussed [^.]+\.",
            r"Last time [^.]+\.",
            r"last time [^.]+\.",
            r"we explored [^.]+together[^.]+\.",
            r"We explored [^.]+together[^.]+\.",
            r"you mentioned [^.]+before[^.]+\.",
            r"You mentioned [^.]+before[^.]+\.",
            r"I'm glad to see [^.]+progress[^.]+\.",
            r"your [^.]+ has improved [^.]+\.",
            r"I've reviewed your [^.]+\.",
        ]
        
        cleaned_response = response
        
        for pattern in removal_patterns:
            cleaned_response = re.sub(pattern, '', cleaned_response, flags=re.IGNORECASE)
        
        cleaned_response = re.sub(r'\n\n+', '\n\n', cleaned_response)
        cleaned_response = re.sub(r'  +', ' ', cleaned_response)
        
        return cleaned_response.strip()
    
    return response

def format_llm_response(specialist_response: str, classification: dict) -> str:
    """Format the specialist's response with appropriate context and warnings"""
    domain_emoji = {
        'DIABETES': '💉',
        'HEART_DISEASE': '❤️',
        'MENTAL_HEALTH': '🧠'
    }
    
    urgency_badge = ""
    if classification['urgency'] == 'URGENT':
        urgency_badge = "\n\n⚠️ **URGENT**: Please seek medical attention within 24 hours.\n"
    elif classification['urgency'] == 'NEEDS_ATTENTION':
        urgency_badge = "\n\n📋 **Note**: Consider scheduling a doctor's appointment to discuss this.\n"
    
    domain_name = classification['domain'].replace('_', ' ').title()
    emoji = domain_emoji.get(classification['domain'], '🏥')
    
    formatted_response = f"""{emoji} **{domain_name} Assistant**

{specialist_response}

{urgency_badge}

---
*Classification confidence: {int(classification.get('confidence', 0.5) * 100)}%*  
*💡 Remember: This is educational guidance. Always consult healthcare professionals for medical decisions.*
"""
    
    return formatted_response

def process_user_message(user_message: str) -> str:
    """
    🆕 UPDATED: Complete pipeline with metric tracking
    """
    
    # Step 1: Emergency detection
    is_emergency_local, urgency_local, symptoms_local = detect_emergency_keywords(user_message)
    
    # Step 2: Router classification
    with st.spinner("🔍 Analyzing your question..."):
        classification = call_router_llm(user_message, st.session_state.user_id)
    
    # Step 3: Check scope
    if classification.get('scope_violation', False) or classification.get('domain') == 'OUT_OF_SCOPE':
        out_of_scope_response = """🏥 **WellNest Health Assistant**

I appreciate your question, but I'm specifically designed to help with:

✅ **Diabetes & Blood Sugar Management**
✅ **Hypertension & Cardiovascular Health**  
✅ **Mental Health** (depression, anxiety, stress, sleep)

Your question appears to be outside these areas.

**How I can help:**
- Blood sugar concerns and diabetes management
- Blood pressure and heart health
- Mental wellbeing and stress

Please feel free to ask about any of these health topics! 🩺"""
        
        save_conversation(
            st.session_state.user_id,
            user_message,
            out_of_scope_response,
            routed_domain='out_of_scope',
            urgency_level='routine',
            detected_symptoms=[]
        )
        
        return out_of_scope_response
    
    if is_emergency_local or urgency_local == 'urgent':
        classification['urgency'] = urgency_local.upper()
        if symptoms_local:
            classification['safety_flags'] = symptoms_local
    
    # Step 4: Handle emergencies
    if classification['urgency'] == 'EMERGENCY':
        emergency_response = f"""🚨 **EMERGENCY DETECTED** 🚨

{classification.get('symptom_assessment', 'Based on your symptoms, this may require immediate medical attention.')}

**⚠️ TAKE ACTION NOW:**
- 🚑 Call 911 (US) or your local emergency number
- 🏥 Go to the nearest Emergency Department
- 📞 Call your doctor immediately

**Detected concerns:** {', '.join(classification.get('safety_flags', symptoms_local))}

---

**🆘 Crisis Resources:**
- **National Suicide Prevention Lifeline:** 988
- **Crisis Text Line:** Text HOME to 741741
- **Emergency Services:** 911

---

⚠️ **This is an AI system and CANNOT provide emergency medical care.**  
⚠️ **Do not delay seeking professional medical help.**"""
        
        save_conversation(
            st.session_state.user_id,
            user_message,
            emergency_response,
            routed_domain='emergency_triage',
            urgency_level='emergency',
            detected_symptoms=classification.get('safety_flags', symptoms_local)
        )
        
        return emergency_response
    
    # 🆕 Step 5: Display current metrics in sidebar
    st.session_state.last_domain = classification['domain']
    display_metric_trends(st.session_state.user_id, classification['domain'])
    
    # Step 6: Call specialist (now with smart context)
    with st.spinner(f"💭 Consulting {classification['domain'].replace('_', ' ').title()} specialist..."):
        specialist_response = call_specialist_llm(
            user_message,
            classification,
            st.session_state.user_id
        )
    
    # Step 7: Remove hallucinations
    cleaned_response = remove_hallucinated_phrases(specialist_response, st.session_state.user_id)
    
    # Step 8: Format response
    final_response = format_llm_response(cleaned_response, classification)
    
    # Step 9: Save conversation
    save_result = save_conversation(
        st.session_state.user_id,
        user_message,
        final_response,
        routed_domain=classification['domain'],
        urgency_level=classification['urgency'].lower(),
        detected_symptoms=classification.get('safety_flags', [])
    )
    
    # 🆕 Step 10: Extract metrics (async - don't block UI)
    try:
        conv_id_query = f"""
        SELECT CONVERSATION_ID
        FROM WELLNEST.USER_MANAGEMENT.CONVERSATION_HISTORY
        WHERE USER_ID = '{st.session_state.user_id}'
          AND SESSION_ID = '{st.session_state.session_id}'
        ORDER BY MESSAGE_TIMESTAMP DESC
        LIMIT 1
        """
        
        conv_result = session.sql(conv_id_query).collect()
        if conv_result:
            conversation_id = conv_result[0]['CONVERSATION_ID']
            extract_and_track_metrics(
                user_message, final_response,
                st.session_state.user_id, conversation_id,
                classification['domain']
            )
    except:
        pass  # Don't fail if metric extraction fails
    
    if save_result:
        st.toast(f"💾 Saved ({classification['domain']})", icon="✅")
    
    return final_response

# =============================================================================
# CHAT INTERFACE (WITH SMART CONTEXT UPDATES)
# =============================================================================

def initialize_chat_session():
    """Initialize chat session state"""
    if 'session_id' not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
    
    if 'messages' not in st.session_state:
        st.session_state.messages = []
        
        history = get_current_session_messages(
            st.session_state.user_id, 
            st.session_state.session_id
        )
        
        if not history:
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"Hello {st.session_state.full_name}! 👋 I'm your WellNest health assistant with smart memory. How can I help you today?"
            })
        else:
            for msg in history:
                st.session_state.messages.append({
                    "role": "user",
                    "content": msg['USER_MESSAGE']
                })
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": msg['ASSISTANT_RESPONSE']
                })

def render_chat_page():
    """🆕 UPDATED: Display chat interface with smart context features"""
    st.title("💬 Health Assistant Chat")
    st.markdown("---")
    
    initialize_chat_session()
    
    # Sidebar chat info
    with st.sidebar:
        st.markdown("### 💬 Chat Session Info")
        st.caption(f"Session ID: {st.session_state.session_id[:8]}...")
        
        # 🆕 UPDATED: New conversation button with summarization
        if st.button("🔄 New Conversation", use_container_width=True):
            with st.spinner("📝 Summarizing conversation..."):
                if summarize_current_session():
                    st.sidebar.success("✅ Session saved!")
            
            st.session_state.session_id = str(uuid.uuid4())
            st.session_state.messages = []
            st.rerun()
        
        st.markdown("---")
        
        total_conversations = len(get_conversation_history(st.session_state.user_id, limit=1000))
        st.metric("Total Conversations", total_conversations)
        
        st.markdown("---")
        
        if st.button("📜 View History", use_container_width=True):
            st.session_state.show_history = True
    
    # Show conversation history modal
    if st.session_state.get('show_history', False):
        with st.expander("📜 Conversation History", expanded=True):
            history = get_conversation_history(st.session_state.user_id, limit=20)
            
            if history:
                for i, conv in enumerate(history):
                    timestamp = conv['MESSAGE_TIMESTAMP'].strftime('%Y-%m-%d %I:%M %p')
                    urgency_badge = ""
                    if conv['URGENCY_LEVEL'] == 'emergency':
                        urgency_badge = "🚨"
                    elif conv['URGENCY_LEVEL'] == 'urgent':
                        urgency_badge = "⚠️"
                    
                    with st.container():
                        st.markdown(f"**{urgency_badge} {timestamp}** - Session: {conv['SESSION_ID'][:8]}")
                        st.text(f"You: {conv['USER_MESSAGE'][:80]}...")
                        if i < 19:
                            st.divider()
            else:
                st.info("No conversation history yet. Start chatting!")
            
            if st.button("❌ Close History"):
                st.session_state.show_history = False
                st.rerun()
    
    # Emergency warning banner
    if st.session_state.messages:
        last_message = st.session_state.messages[-1]
        if "EMERGENCY DETECTED" in last_message.get('content', ''):
            st.error("🚨 **EMERGENCY DETECTED IN LAST MESSAGE** - Please seek immediate medical attention!")
    
    # Display chat messages
    chat_container = st.container()
    
    with chat_container:
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
    
    # Chat input
    if user_input := st.chat_input("Type your health question here..."):
        st.session_state.messages.append({
            "role": "user",
            "content": user_input
        })
        
        with st.chat_message("user"):
            st.markdown(user_input)
        
        with st.chat_message("assistant"):
            with st.spinner("Analyzing your message..."):
                response = process_user_message(user_input)
                st.markdown(response)
        
        st.session_state.messages.append({
            "role": "assistant",
            "content": response
        })
        
        st.rerun()
    
    # Chat instructions
    st.markdown("---")
    with st.expander("ℹ️ How to Use the Chat"):
        st.markdown("""
        ### Tips for Best Results:
        
        - **Be specific** about your symptoms (duration, severity, location)
        - **Mention relevant medical history** if applicable
        - **Include context** like recent activities or changes
        
        ### What I Can Help With:
        
        - 🩺 Symptom assessment and guidance
        - 💊 General health education
        - 📊 Understanding medical test results
        - 🏃 Lifestyle and wellness advice
        
        ### What I Cannot Do:
        
        - ❌ Diagnose medical conditions
        - ❌ Prescribe medications
        - ❌ Replace professional medical care
        - ❌ Provide emergency medical treatment
        
        **Always consult healthcare professionals for medical decisions.**
        """)

# =============================================================================
# PAGE RENDERING (EXISTING - PRESERVED)
# =============================================================================

def render_sidebar():
    """Render sidebar navigation"""
    with st.sidebar:
        st.markdown("### 🏥 WellNest")
        st.markdown("---")
        
        if st.session_state.authenticated:
            st.markdown(f"**👤 {st.session_state.full_name}**")
            st.markdown(f"📧 {st.session_state.email}")
            st.markdown("---")
            
            st.markdown("### 📋 Navigation")
            
            if st.button("🏠 Dashboard", use_container_width=True):
                st.session_state.current_page = 'dashboard'
                st.rerun()
            
            if st.button("👤 My Profile", use_container_width=True):
                st.session_state.current_page = 'profile'
                st.rerun()
            
            if st.button("💬 Health Chat", use_container_width=True):
                st.session_state.current_page = 'chat'
                st.rerun()
            
            if st.button("📄 My Documents", use_container_width=True):
                st.session_state.current_page = 'documents'
                st.rerun()
            
            st.markdown("---")
            
            if st.button("🚪 Logout", use_container_width=True):
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.rerun()

def render_login_page():
    """Display login and signup interface"""
    st.markdown('<h1 class="main-header">🏥 WellNest</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Your Personal AI Health Assistant with Smart Memory</p>', unsafe_allow_html=True)
    
    tab1, tab2 = st.tabs(["🔐 Login", "📝 Sign Up"])
    
    with tab1:
        st.subheader("Login to Your Account")
        
        with st.form("login_form", clear_on_submit=False):
            email = st.text_input("Email Address", placeholder="your.email@example.com")
            password = st.text_input("Password", type="password")
            
            submit = st.form_submit_button("🔓 Login", use_container_width=True)
            
            if submit:
                if not email or not password:
                    st.error("⚠️ Please fill in all fields")
                else:
                    with st.spinner("Authenticating..."):
                        user = authenticate_user(email, password)
                        if user:
                            st.session_state.user_id = user['user_id']
                            st.session_state.email = user['email']
                            st.session_state.full_name = user['full_name']
                            st.session_state.authenticated = True
                            st.session_state.current_page = 'dashboard'
                            st.success(f"✅ Welcome back, {user['full_name']}!")
                            st.rerun()
        
        with st.expander("🧪 Test Credentials"):
            st.info("**Email:** admin@wellnest.com\n**Password:** WellNest2024!")
    
    with tab2:
        st.subheader("Create Your WellNest Account")
        
        with st.form("signup_form"):
            full_name = st.text_input("Full Name *", placeholder="John Doe")
            email_signup = st.text_input("Email Address *", placeholder="john.doe@example.com", key="signup_email")
            
            col1, col2 = st.columns(2)
            with col1:
                password_signup = st.text_input("Password *", type="password", key="signup_password")
            with col2:
                password_confirm = st.text_input("Confirm Password *", type="password")
            
            st.markdown("**Optional Information:**")
            col3, col4 = st.columns(2)
            with col3:
                dob = st.date_input(
                    "Date of Birth", 
                    value=None, 
                    min_value=datetime(1900, 1, 1), 
                    max_value=datetime.today()
                )
            with col4:
                gender = st.selectbox("Gender", 
                                     ["Prefer not to say", "Male", "Female", "Other"])
            
            agree_terms = st.checkbox("I agree to the Terms of Service and Privacy Policy *")
            
            submit_signup = st.form_submit_button("🔐 Create Account", use_container_width=True)
            
            if submit_signup:
                if not all([full_name, email_signup, password_signup, password_confirm]):
                    st.error("⚠️ Please fill in all required fields marked with *")
                elif not validate_email(email_signup):
                    st.error("⚠️ Please enter a valid email address")
                elif password_signup != password_confirm:
                    st.error("⚠️ Passwords do not match")
                else:
                    is_valid, msg = validate_password_strength(password_signup)
                    if not is_valid:
                        st.error(f"⚠️ {msg}")
                    elif not agree_terms:
                        st.error("⚠️ Please agree to the Terms of Service")
                    else:
                        with st.spinner("Creating your account..."):
                            dob_str = str(dob) if dob else None
                            gender_str = gender if gender != "Prefer not to say" else None
                            
                            if register_user(email_signup, password_signup, full_name, dob_str, gender_str):
                                st.success("✅ Account created successfully! Please log in.")
                                st.balloons()

def render_dashboard():
    """Display main dashboard"""
    st.title("Welcome to WellNest! 🎉")
    st.markdown(f"### Hello, {st.session_state.full_name}! 👋")
    
    stats = get_user_stats(st.session_state.user_id)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            label="💬 Conversations",
            value=stats['conversations'],
            delta="Start chatting!" if stats['conversations'] == 0 else None
        )
    
    with col2:
        doc_stats = get_document_stats(st.session_state.user_id)
        st.metric(
            label="📄 Documents",
            value=doc_stats['total'],
            delta=f"{doc_stats['pending']} pending" if doc_stats['pending'] > 0 else "All processed"
        )
        if doc_stats['total'] > 0:
            st.caption(f"💾 {format_file_size(doc_stats['total_size'])} total")
    
    with col3:
        st.metric(
            label="✅ Profile Completeness",
            value=f"{stats['profile_completeness']}%",
            delta=f"{100 - stats['profile_completeness']}% remaining"
        )
    
    st.markdown("---")
    
    col_a, col_b = st.columns(2)
    
    with col_a:
        st.markdown("### 🚀 Getting Started")
        st.markdown("""
        1. **Complete your profile** with health information
        2. **Upload medical documents** (lab reports, prescriptions)
        3. **Start chatting** with your AI health assistant
        4. Get personalized health insights and recommendations
        """)
    
    with col_b:
        st.markdown("### 🎯 What WellNest Can Do")
        st.markdown("""
        - 🩺 **Symptom Analysis**: Understand your symptoms
        - 🔬 **Document Insights**: Extract data from medical reports
        - 📊 **Health Tracking**: Monitor your health metrics
        - 💊 **Medication Reminders**: Stay on track with treatment
        """)
    
    st.markdown("---")
    st.markdown("### 💬 Recent Conversations")
    
    recent_convs = get_conversation_history(st.session_state.user_id, limit=3)
    
    if recent_convs:
        for conv in recent_convs:
            timestamp = conv['MESSAGE_TIMESTAMP'].strftime('%b %d, %I:%M %p')
            
            urgency_badge = ""
            if conv['URGENCY_LEVEL'] == 'emergency':
                urgency_badge = "🚨 Emergency"
            elif conv['URGENCY_LEVEL'] == 'urgent':
                urgency_badge = "⚠️ Urgent"
            elif conv['URGENCY_LEVEL'] == 'needs_attention':
                urgency_badge = "📋 Needs Attention"
            
            with st.container():
                col_time, col_urgency = st.columns([3, 1])
                with col_time:
                    st.caption(f"🕐 {timestamp}")
                with col_urgency:
                    if urgency_badge:
                        st.caption(urgency_badge)
                
                st.markdown(f"**You:** {conv['USER_MESSAGE'][:100]}{'...' if len(conv['USER_MESSAGE']) > 100 else ''}")
                st.divider()
        
        if st.button("💬 Go to Chat", use_container_width=True):
            st.session_state.current_page = 'chat'
            st.rerun()
    else:
        st.info("No conversations yet. Start chatting with your health assistant!")
        if st.button("💬 Start Your First Chat", use_container_width=True):
            st.session_state.current_page = 'chat'
            st.rerun()

def render_profile_page():
    """Display profile page - PRESERVED AS-IS"""
    st.title("👤 My Health Profile")
    st.markdown("---")
    
    profile = get_user_profile(st.session_state.user_id)
    
    if not profile:
        st.error("❌ Could not load profile. Please try again.")
        return
    
    completeness = get_profile_completeness(profile)
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(f"### Profile Completeness: **{completeness}%**")
        st.progress(completeness / 100)
    with col2:
        if completeness < 50:
            st.warning("⚠️ Low")
        elif completeness < 80:
            st.info("📊 Good")
        else:
            st.success("✅ Complete")
    
    st.markdown("---")
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "📋 Basic Information", 
        "🏥 Medical History", 
        "🚶 Lifestyle Factors",
        "🆘 Emergency Contact"
    ])
    
    # TAB 1: Basic Information
    with tab1:
        st.subheader("Basic Information")
        
        with st.form("basic_info_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                full_name = st.text_input("Full Name *", value=profile['FULL_NAME'])
                email = st.text_input("Email", value=profile['EMAIL'], disabled=True)
                phone = st.text_input("Phone Number", value=profile['PHONE_NUMBER'] or "")
            
            with col2:
                dob = st.date_input(
                    "Date of Birth",
                    value=profile['DATE_OF_BIRTH'],
                    min_value=date(1900, 1, 1),
                    max_value=date.today(),
                    disabled=True
                )
                
                if profile['DATE_OF_BIRTH']:
                    age = calculate_age(profile['DATE_OF_BIRTH'])
                    st.metric("Age", f"{age} years")
                
                gender = st.selectbox(
                    "Gender",
                    ["Male", "Female", "Other", "Prefer not to say"],
                    index=["Male", "Female", "Other", "Prefer not to say"].index(profile['GENDER']) if profile['GENDER'] else 3,
                    disabled=True
                )
            
            st.markdown("---")
            st.subheader("Physical Measurements")
            
            col3, col4, col5 = st.columns(3)
            
            with col3:
                height_cm = st.number_input(
                    "Height (cm)",
                    min_value=50.0,
                    max_value=250.0,
                    value=float(profile['HEIGHT_CM']) if profile['HEIGHT_CM'] else 170.0,
                    step=0.1
                )
            
            with col4:
                weight_kg = st.number_input(
                    "Weight (kg)",
                    min_value=20.0,
                    max_value=300.0,
                    value=float(profile['WEIGHT_KG']) if profile['WEIGHT_KG'] else 70.0,
                    step=0.1
                )
            
            with col5:
                bmi = calculate_bmi(weight_kg, height_cm)
                if bmi:
                    category, color = get_bmi_category(bmi)
                    st.metric("BMI", f"{bmi}", delta=category)
                    st.markdown(f":{color}[{category}]")
            
            col6, col7 = st.columns(2)
            with col6:
                blood_type = st.selectbox(
                    "Blood Type",
                    ["Unknown", "A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"],
                    index=["Unknown", "A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"].index(profile['BLOOD_TYPE']) if profile['BLOOD_TYPE'] else 0
                )
            
            submit_basic = st.form_submit_button("💾 Save Basic Information", use_container_width=True)
            
            if submit_basic:
                if not full_name:
                    st.error("⚠️ Full name is required")
                else:
                    with st.spinner("Updating profile..."):
                        update_user_info(st.session_state.user_id, full_name, phone)
                        
                        profile_data = {
                            'height_cm': height_cm,
                            'weight_kg': weight_kg,
                            'bmi': bmi,
                            'blood_type': blood_type if blood_type != "Unknown" else None,
                            'gender': gender
                        }
                        update_medical_profile(st.session_state.user_id, profile_data)
                        
                        st.success("✅ Profile updated successfully!")
                        st.rerun()
    
    # TAB 2: Medical History
    with tab2:
        st.subheader("Medical History")
        
        with st.form("medical_history_form"):
            st.markdown("**Select all conditions that apply:**")
            
            col1, col2 = st.columns(2)
            
            with col1:
                has_diabetes = st.checkbox("Diabetes", value=profile['HAS_DIABETES'] or False)
                has_hypertension = st.checkbox("Hypertension (High Blood Pressure)", value=profile['HAS_HYPERTENSION'] or False)
                has_heart_disease = st.checkbox("Heart Disease", value=profile['HAS_HEART_DISEASE'] or False)
            
            with col2:
                has_mental_health = st.checkbox("Mental Health History", value=profile['HAS_MENTAL_HEALTH_HISTORY'] or False)
                has_pcos = st.checkbox("PCOS (Women only)", value=profile['HAS_PCOS'] or False)
            
            if profile['GENDER'] == 'Female':
                st.markdown("---")
                st.markdown("**Women's Health Information:**")
                
                col3, col4 = st.columns(2)
                
                with col3:
                    is_pregnant = st.checkbox("Currently Pregnant", value=profile['IS_PREGNANT'] or False)
                    
                    if is_pregnant:
                        trimester = st.selectbox(
                            "Trimester",
                            [1, 2, 3],
                            index=profile['PREGNANCY_TRIMESTER'] - 1 if profile['PREGNANCY_TRIMESTER'] else 0
                        )
                    else:
                        trimester = None
                
                with col4:
                    menstrual_regular = st.checkbox("Regular Menstrual Cycle", value=profile['MENSTRUAL_CYCLE_REGULAR'] or False)
                    
                    lmp = st.date_input(
                        "Last Menstrual Period (Optional)",
                        value=profile['LAST_MENSTRUAL_PERIOD'],
                        max_value=date.today()
                    )
            else:
                is_pregnant = False
                trimester = None
                menstrual_regular = False
                lmp = None
            
            submit_medical = st.form_submit_button("💾 Save Medical History", use_container_width=True)
            
            if submit_medical:
                with st.spinner("Updating medical history..."):
                    profile_data = {
                        'has_diabetes': has_diabetes,
                        'has_hypertension': has_hypertension,
                        'has_heart_disease': has_heart_disease,
                        'has_mental_health_history': has_mental_health,
                        'has_pcos': has_pcos,
                        'is_pregnant': is_pregnant,
                        'pregnancy_trimester': trimester,
                        'menstrual_cycle_regular': menstrual_regular,
                        'last_menstrual_period': str(lmp) if lmp else None,
                        'gender': profile['GENDER']
                    }
                    update_medical_profile(st.session_state.user_id, profile_data)
                    
                    st.success("✅ Medical history updated successfully!")
                    st.rerun()
    
    # TAB 3: Lifestyle
    with tab3:
        st.subheader("Lifestyle Factors")
        
        with st.form("lifestyle_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                smoking_status = st.selectbox(
                    "Smoking Status",
                    ["Never", "Former", "Current"],
                    index=["Never", "Former", "Current"].index(profile['SMOKING_STATUS']) if profile['SMOKING_STATUS'] else 0
                )
                
                alcohol = st.selectbox(
                    "Alcohol Consumption",
                    ["None", "Occasional", "Moderate", "Heavy"],
                    index=["None", "Occasional", "Moderate", "Heavy"].index(profile['ALCOHOL_CONSUMPTION']) if profile['ALCOHOL_CONSUMPTION'] else 0
                )
            
            with col2:
                exercise_options = ["Sedentary", "Light (1-2 days/week)", "Moderate (3-4 days/week)", "Active (5+ days/week)"]
                try:
                    exercise_idx = exercise_options.index(profile['EXERCISE_FREQUENCY']) if profile['EXERCISE_FREQUENCY'] else 0
                except ValueError:
                    exercise_idx = 0
                
                exercise = st.selectbox(
                    "Exercise Frequency",
                    exercise_options,
                    index=exercise_idx
                )
            
            submit_lifestyle = st.form_submit_button("💾 Save Lifestyle Information", use_container_width=True)
            
            if submit_lifestyle:
                with st.spinner("Updating lifestyle information..."):
                    profile_data = {
                        'smoking_status': smoking_status,
                        'alcohol_consumption': alcohol,
                        'exercise_frequency': exercise
                    }
                    update_medical_profile(st.session_state.user_id, profile_data)
                    
                    st.success("✅ Lifestyle information updated successfully!")
                    st.rerun()
    
    # TAB 4: Emergency Contact
    with tab4:
        st.subheader("Emergency Contact Information")
        
        st.info("💡 This information will be used in case of medical emergency")
        
        with st.form("emergency_contact_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                emergency_name = st.text_input(
                    "Contact Name",
                    value=profile['EMERGENCY_CONTACT_NAME'] or ""
                )
                emergency_phone = st.text_input(
                    "Contact Phone",
                    value=profile['EMERGENCY_CONTACT_PHONE'] or ""
                )
            
            with col2:
                relationship_options = ["Spouse", "Parent", "Child", "Sibling", "Friend", "Other"]
                try:
                    relationship_idx = relationship_options.index(profile['EMERGENCY_CONTACT_RELATIONSHIP']) if profile['EMERGENCY_CONTACT_RELATIONSHIP'] else 0
                except ValueError:
                    relationship_idx = 0
                
                emergency_relationship = st.selectbox(
                    "Relationship",
                    relationship_options,
                    index=relationship_idx
                )
            
            submit_emergency = st.form_submit_button("💾 Save Emergency Contact", use_container_width=True)
            
            if submit_emergency:
                with st.spinner("Updating emergency contact..."):
                    profile_data = {
                        'emergency_contact_name': emergency_name,
                        'emergency_contact_phone': emergency_phone,
                        'emergency_contact_relationship': emergency_relationship
                    }
                    update_medical_profile(st.session_state.user_id, profile_data)
                    
                    st.success("✅ Emergency contact updated successfully!")
                    st.rerun()

def render_documents_page():
    """Display document upload and management interface - PRESERVED AS-IS"""
    st.title("📄 Medical Documents")
    st.markdown("---")
    
    tab1, tab2 = st.tabs(["📤 Upload New Document", "📁 My Documents"])
    
    with tab1:
        st.subheader("Upload Medical Document")
        
        st.info("""
        ### Supported Documents:
        - 🩺 Lab test results (blood work, glucose tests, etc.)
        - 💊 Prescriptions and medication lists
        - 🏥 Imaging reports (X-rays, MRI, CT scans)
        - 📋 Discharge summaries
        - 📊 Health screening reports
        
        **Supported formats:** PDF (up to 10 MB)
        """)
        
        uploaded_file = st.file_uploader(
            "Choose a PDF file",
            type=['pdf'],
            help="Maximum file size: 10 MB"
        )
        
        if uploaded_file is not None:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("📄 Filename", uploaded_file.name)
            with col2:
                file_size = len(uploaded_file.getvalue())
                st.metric("💾 Size", format_file_size(file_size))
            with col3:
                detected_type = extract_document_type(uploaded_file.name)
                st.metric("🏷️ Type", detected_type.replace('_', ' ').title())
            
            st.markdown("---")
            
            doc_type = st.selectbox(
                "Document Type",
                ["Lab Report", "Prescription", "Imaging Report", "Discharge Summary", "Other"],
                index=["lab_report", "prescription", "imaging", "discharge_summary", "other"].index(detected_type)
            )
            
            doc_description = st.text_area(
                "Description (Optional)",
                placeholder="Add any notes about this document...",
                height=100
            )
            
            col_upload, col_cancel = st.columns([3, 1])
            
            with col_upload:
                if st.button("📤 Upload Document", use_container_width=True, type="primary"):
                    if file_size > 10 * 1024 * 1024:
                        st.error("⚠️ File size exceeds 10 MB limit. Please upload a smaller file.")
                    else:
                        with st.spinner("Uploading and processing document..."):
                            file_content = uploaded_file.getvalue()
                            
                            document_id = save_uploaded_document(
                                st.session_state.user_id,
                                uploaded_file.name,
                                file_size,
                                file_content,
                                doc_type.lower().replace(' ', '_')
                            )
                            
                            if document_id:
                                processing_result = process_pdf_document(
                                    file_content, 
                                    uploaded_file.name
                                )
                                
                                update_document_processing_status(
                                    document_id,
                                    processing_result['processing_status'],
                                    processing_result['extracted_data'],
                                    processing_result['confidence_score']
                                )
                                
                                st.success("✅ Document uploaded successfully!")
                                st.balloons()
                                
                                with st.expander("📊 Processing Summary"):
                                    st.json(processing_result['extracted_data'])
                                
                                st.info("📄 Switch to 'My Documents' tab to view all your uploaded files.")
            
            with col_cancel:
                if st.button("❌ Cancel", use_container_width=True):
                    st.rerun()
    
    with tab2:
        st.subheader("Your Uploaded Documents")
        
        documents = get_user_documents(st.session_state.user_id)
        
        if not documents:
            st.info("🔭 No documents uploaded yet. Upload your first medical document in the 'Upload New Document' tab!")
        else:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("📄 Total Documents", len(documents))
            with col2:
                processed = sum(1 for doc in documents if doc['PROCESSING_STATUS'] == 'completed')
                st.metric("✅ Processed", processed)
            with col3:
                pending = sum(1 for doc in documents if doc['PROCESSING_STATUS'] == 'pending')
                st.metric("⏳ Pending", pending)
            
            st.markdown("---")
            
            all_types = list(set([doc['DOCUMENT_TYPE'] for doc in documents if doc['DOCUMENT_TYPE']]))
            selected_type = st.selectbox(
                "Filter by Type",
                ["All"] + [t.replace('_', ' ').title() for t in all_types]
            )
            
            filtered_docs = documents
            if selected_type != "All":
                selected_type_clean = selected_type.lower().replace(' ', '_')
                filtered_docs = [doc for doc in documents if doc['DOCUMENT_TYPE'] == selected_type_clean]
            
            st.markdown(f"### 📋 Documents ({len(filtered_docs)})")
            
            for idx, doc in enumerate(filtered_docs):
                with st.expander(
                    f"📄 {doc['ORIGINAL_FILENAME']} - {doc['UPLOAD_TIMESTAMP'].strftime('%b %d, %Y')}"
                ):
                    info_col1, info_col2, info_col3 = st.columns(3)
                    
                    with info_col1:
                        st.markdown(f"**Type:** {doc['DOCUMENT_TYPE'].replace('_', ' ').title()}")
                        st.markdown(f"**Size:** {format_file_size(doc['FILE_SIZE_BYTES'])}")
                    
                    with info_col2:
                        status = doc['PROCESSING_STATUS']
                        if status == 'completed':
                            st.success(f"✅ {status.title()}")
                        elif status == 'pending':
                            st.warning(f"⏳ {status.title()}")
                        elif status == 'processing':
                            st.info(f"🔄 {status.title()}")
                        else:
                            st.error(f"❌ {status.title()}")
                        
                        if doc['PROCESSING_COMPLETED_AT']:
                            st.caption(f"Processed: {doc['PROCESSING_COMPLETED_AT'].strftime('%I:%M %p')}")
                    
                    with info_col3:
                        if doc['EXTRACTION_CONFIDENCE_SCORE']:
                            confidence_pct = int(doc['EXTRACTION_CONFIDENCE_SCORE'] * 100)
                            st.metric("Confidence", f"{confidence_pct}%")
                    
                    if doc['EXTRACTED_DATA']:
                        st.markdown("---")
                        st.markdown("**📊 Extracted Information:**")
                        
                        try:
                            extracted = json.loads(doc['EXTRACTED_DATA']) if isinstance(doc['EXTRACTED_DATA'], str) else doc['EXTRACTED_DATA']
                            st.json(extracted)
                        except:
                            st.text(str(doc['EXTRACTED_DATA']))
                    
                    if doc['DETECTED_TEST_TYPES']:
                        st.markdown("**🔬 Detected Tests:**")
                        tests = doc['DETECTED_TEST_TYPES']
                        if isinstance(tests, str):
                            st.markdown(tests)
                        else:
                            st.markdown(", ".join(tests))
                    
                    st.markdown("---")
                    button_col1, button_col2, button_col3 = st.columns(3)
                    
                    with button_col1:
                        if doc['PROCESSING_STATUS'] == 'pending':
                            if st.button(f"🔄 Reprocess", key=f"reprocess_{doc['DOCUMENT_ID']}"):
                                st.info("🔄 Reprocessing will be available after LLM integration")
                    
                    with button_col2:
                        if st.button(f"📥 Download", key=f"download_{doc['DOCUMENT_ID']}", disabled=True):
                            st.info("📥 Download feature coming soon")
                    
                    with button_col3:
                        if st.button(f"🗑️ Delete", key=f"delete_{doc['DOCUMENT_ID']}", type="secondary"):
                            if delete_document(doc['DOCUMENT_ID'], st.session_state.user_id):
                                st.success("✅ Document deleted successfully!")
                                st.rerun()

# =============================================================================
# MAIN APPLICATION LOGIC
# =============================================================================

def main():
    """Main application router"""
    
    if st.session_state.authenticated:
        render_sidebar()
    
    if not st.session_state.authenticated:
        render_login_page()
    else:
        if st.session_state.current_page == 'dashboard':
            render_dashboard()
        elif st.session_state.current_page == 'profile':
            render_profile_page()
        elif st.session_state.current_page == 'chat':
            render_chat_page()
        elif st.session_state.current_page == 'documents':
            render_documents_page()

if __name__ == "__main__":
    main()