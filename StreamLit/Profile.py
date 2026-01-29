# =============================================================================
# WELLNEST - USER PROFILE PAGE
# =============================================================================
# Allows users to view and update their medical profile information
# =============================================================================

import streamlit as st
from snowflake.snowpark.context import get_active_session
from datetime import datetime, date

# Get Snowflake session
session = get_active_session()

# =============================================================================
# PAGE CONFIGURATION
# =============================================================================

st.set_page_config(
    page_title="My Profile - WellNest",
    page_icon="👤",
    layout="wide"
)

# =============================================================================
# AUTHENTICATION CHECK
# =============================================================================

def check_authentication():
    """Redirect to login if not authenticated"""
    if 'authenticated' not in st.session_state or not st.session_state.authenticated:
        st.warning("⚠️ Please log in to access your profile.")
        st.stop()

check_authentication()

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

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

# =============================================================================
# DATABASE FUNCTIONS
# =============================================================================

def get_user_profile(user_id):
    """Fetch user profile from database"""
    query = f"""
    SELECT 
        u.USER_ID,
        u.EMAIL,
        u.FULL_NAME,
        u.DATE_OF_BIRTH,
        u.GENDER,
        u.PHONE_NUMBER,
        u.CREATED_AT,
        p.PROFILE_ID,
        p.HEIGHT_CM,
        p.WEIGHT_KG,
        p.BMI,
        p.BLOOD_TYPE,
        p.HAS_DIABETES,
        p.HAS_HYPERTENSION,
        p.HAS_HEART_DISEASE,
        p.HAS_MENTAL_HEALTH_HISTORY,
        p.HAS_PCOS,
        p.SMOKING_STATUS,
        p.ALCOHOL_CONSUMPTION,
        p.EXERCISE_FREQUENCY,
        p.IS_PREGNANT,
        p.PREGNANCY_TRIMESTER,
        p.MENSTRUAL_CYCLE_REGULAR,
        p.LAST_MENSTRUAL_PERIOD,
        p.EMERGENCY_CONTACT_NAME,
        p.EMERGENCY_CONTACT_PHONE,
        p.EMERGENCY_CONTACT_RELATIONSHIP,
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
    SET 
        FULL_NAME = '{full_name}',
        PHONE_NUMBER = {phone_sql}
    WHERE USER_ID = '{user_id}'
    """
    
    session.sql(query).collect()

def update_medical_profile(user_id, profile_data):
    """Update medical profile information"""
    
    # Build SET clause dynamically
    set_clauses = []
    
    # Physical measurements
    if profile_data.get('height_cm'):
        set_clauses.append(f"HEIGHT_CM = {profile_data['height_cm']}")
    if profile_data.get('weight_kg'):
        set_clauses.append(f"WEIGHT_KG = {profile_data['weight_kg']}")
    if profile_data.get('bmi'):
        set_clauses.append(f"BMI = {profile_data['bmi']}")
    
    # Blood type
    if profile_data.get('blood_type'):
        set_clauses.append(f"BLOOD_TYPE = '{profile_data['blood_type']}'")
    
    # Medical history flags
    set_clauses.append(f"HAS_DIABETES = {profile_data.get('has_diabetes', False)}")
    set_clauses.append(f"HAS_HYPERTENSION = {profile_data.get('has_hypertension', False)}")
    set_clauses.append(f"HAS_HEART_DISEASE = {profile_data.get('has_heart_disease', False)}")
    set_clauses.append(f"HAS_MENTAL_HEALTH_HISTORY = {profile_data.get('has_mental_health_history', False)}")
    set_clauses.append(f"HAS_PCOS = {profile_data.get('has_pcos', False)}")
    
    # Lifestyle factors
    if profile_data.get('smoking_status'):
        set_clauses.append(f"SMOKING_STATUS = '{profile_data['smoking_status']}'")
    if profile_data.get('alcohol_consumption'):
        set_clauses.append(f"ALCOHOL_CONSUMPTION = '{profile_data['alcohol_consumption']}'")
    if profile_data.get('exercise_frequency'):
        set_clauses.append(f"EXERCISE_FREQUENCY = '{profile_data['exercise_frequency']}'")
    
    # Women's health
    if profile_data.get('gender') == 'Female':
        set_clauses.append(f"IS_PREGNANT = {profile_data.get('is_pregnant', False)}")
        if profile_data.get('pregnancy_trimester'):
            set_clauses.append(f"PREGNANCY_TRIMESTER = {profile_data['pregnancy_trimester']}")
        set_clauses.append(f"MENSTRUAL_CYCLE_REGULAR = {profile_data.get('menstrual_cycle_regular', False)}")
        if profile_data.get('last_menstrual_period'):
            set_clauses.append(f"LAST_MENSTRUAL_PERIOD = '{profile_data['last_menstrual_period']}'")
    
    # Emergency contact
    if profile_data.get('emergency_contact_name'):
        set_clauses.append(f"EMERGENCY_CONTACT_NAME = '{profile_data['emergency_contact_name']}'")
    if profile_data.get('emergency_contact_phone'):
        set_clauses.append(f"EMERGENCY_CONTACT_PHONE = '{profile_data['emergency_contact_phone']}'")
    if profile_data.get('emergency_contact_relationship'):
        set_clauses.append(f"EMERGENCY_CONTACT_RELATIONSHIP = '{profile_data['emergency_contact_relationship']}'")
    
    # Always update timestamp
    set_clauses.append("LAST_UPDATED = CURRENT_TIMESTAMP()")
    
    # Execute update
    query = f"""
    UPDATE WELLNEST.USER_MANAGEMENT.USER_MEDICAL_PROFILES
    SET {', '.join(set_clauses)}
    WHERE USER_ID = '{user_id}'
    """
    
    session.sql(query).collect()

# =============================================================================
# PAGE LAYOUT
# =============================================================================

# Sidebar
with st.sidebar:
    st.markdown("### 🏥 WellNest")
    st.markdown("---")
    st.markdown(f"**👤 {st.session_state.full_name}**")
    st.markdown(f"📧 {st.session_state.email}")
    st.markdown("---")
    
    if st.button("🏠 Back to Dashboard", use_container_width=True):
        st.switch_page("app.py")
    
    if st.button("🚪 Logout", use_container_width=True):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

# Main content
st.title("👤 My Health Profile")
st.markdown("---")

# Fetch current profile
profile = get_user_profile(st.session_state.user_id)

if not profile:
    st.error("❌ Could not load profile. Please try again.")
    st.stop()

# Calculate profile completeness
completeness = get_profile_completeness(profile)

# Display profile completeness
col1, col2, col3 = st.columns([2, 1, 1])
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

# =============================================================================
# PROFILE SECTIONS
# =============================================================================

# Create tabs for different sections
tab1, tab2, tab3, tab4 = st.tabs([
    "📋 Basic Information", 
    "🏥 Medical History", 
    "🚶 Lifestyle Factors",
    "🆘 Emergency Contact"
])

# =============================================================================
# TAB 1: BASIC INFORMATION
# =============================================================================
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
            # Calculate BMI
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
                    # Update user info
                    update_user_info(st.session_state.user_id, full_name, phone)
                    
                    # Update medical profile
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

# =============================================================================
# TAB 2: MEDICAL HISTORY
# =============================================================================
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
        
        # Women's Health Section (only show if female)
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

# =============================================================================
# TAB 3: LIFESTYLE FACTORS
# =============================================================================
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
            exercise = st.selectbox(
                "Exercise Frequency",
                ["Sedentary", "Light (1-2 days/week)", "Moderate (3-4 days/week)", "Active (5+ days/week)"],
                index=["Sedentary", "Light (1-2 days/week)", "Moderate (3-4 days/week)", "Active (5+ days/week)"].index(profile['EXERCISE_FREQUENCY']) if profile['EXERCISE_FREQUENCY'] else 0
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

# =============================================================================
# TAB 4: EMERGENCY CONTACT
# =============================================================================
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
            emergency_relationship = st.selectbox(
                "Relationship",
                ["Spouse", "Parent", "Child", "Sibling", "Friend", "Other"],
                index=["Spouse", "Parent", "Child", "Sibling", "Friend", "Other"].index(profile['EMERGENCY_CONTACT_RELATIONSHIP']) if profile['EMERGENCY_CONTACT_RELATIONSHIP'] else 0
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

# =============================================================================
# PROFILE SUMMARY SECTION
# =============================================================================
st.markdown("---")
st.subheader("📊 Profile Summary")

summary_col1, summary_col2, summary_col3 = st.columns(3)

with summary_col1:
    st.markdown("**Basic Info**")
    st.write(f"✅ Name: {profile['FULL_NAME']}")
    st.write(f"✅ Email: {profile['EMAIL']}")
    if profile['DATE_OF_BIRTH']:
        age = calculate_age(profile['DATE_OF_BIRTH'])
        st.write(f"✅ Age: {age} years")

with summary_col2:
    st.markdown("**Health Metrics**")
    if profile['HEIGHT_CM'] and profile['WEIGHT_KG']:
        st.write(f"✅ Height: {profile['HEIGHT_CM']} cm")
        st.write(f"✅ Weight: {profile['WEIGHT_KG']} kg")
        if profile['BMI']:
            category, _ = get_bmi_category(profile['BMI'])
            st.write(f"✅ BMI: {profile['BMI']} ({category})")
    else:
        st.write("⚠️ Physical measurements not set")

with summary_col3:
    st.markdown("**Medical Conditions**")
    conditions = []
    if profile['HAS_DIABETES']: conditions.append("Diabetes")
    if profile['HAS_HYPERTENSION']: conditions.append("Hypertension")
    if profile['HAS_HEART_DISEASE']: conditions.append("Heart Disease")
    if profile['HAS_MENTAL_HEALTH_HISTORY']: conditions.append("Mental Health")
    if profile['HAS_PCOS']: conditions.append("PCOS")
    
    if conditions:
        for condition in conditions:
            st.write(f"⚠️ {condition}")
    else:
        st.write("✅ No conditions reported")

# Last updated timestamp
if profile['LAST_UPDATED']:
    st.caption(f"Last updated: {profile['LAST_UPDATED'].strftime('%Y-%m-%d %I:%M %p')}")