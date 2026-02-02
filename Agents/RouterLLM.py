# router_schema.py

HEALTH_DOMAINS = {
    "lifestyle_diseases_diabetes": {
        "description": "Diabetes, blood sugar, HbA1c, glucose control, insulin",
        "keywords": ["diabetes", "blood sugar", "glucose", "insulin", "HbA1c", "diabetic", "hyperglycemia", "hypoglycemia"],
        "model": "diabetes_llm_16k",
        "urgency_features": ["glucose_urgency_level", "hyperglycemia_urgency", "hypoglycemia_urgency"]
    },
    
    "lifestyle_diseases_hypertension": {
        "description": "Blood pressure, hypertension, cardiovascular health, cholesterol",
        "keywords": ["blood pressure", "hypertension", "BP", "cardiovascular", "cholesterol", "heart rate", "systolic", "diastolic"],
        "model": "hypertension_llm",
        "urgency_features": ["bp_urgency_level", "potential_hypertensive_emergency"]
    },
    
    "mental_health": {
        "description": "Depression, anxiety, stress, mood, mental wellness, sleep",
        "keywords": ["depression", "anxiety", "stress", "mental health", "mood", "sleep", "therapy", "counseling"],
        "model": "mental_health_llm",
        "urgency_features": ["mental_health_urgency_level", "needs_suicide_risk_screening"]
    },
    
    "womens_wellness_maternal": {
        "description": "Pregnancy, prenatal care, gestational diabetes, preeclampsia",
        "keywords": ["pregnancy", "pregnant", "prenatal", "gestational", "preeclampsia", "maternal", "baby"],
        "model": "maternal_health_llm",
        "urgency_features": ["maternal_urgency_level", "preeclampsia_bp_warning"]
    },
    
    "womens_wellness_pcos": {
        "description": "PCOS, menstrual irregularities, hormonal imbalances, fertility",
        "keywords": ["PCOS", "menstrual", "period", "ovulation", "fertility", "hormonal", "testosterone"],
        "model": "pcos_llm",
        "urgency_features": ["pcos_urgency_level", "needs_specialist_evaluation"]
    },
    
    "emergency": {
        "description": "Life-threatening symptoms requiring immediate medical attention",
        "keywords": ["chest pain", "stroke", "seizure", "unconscious", "severe bleeding", "suicide"],
        "model": "emergency_protocol",
        "urgency_features": ["emergency"]
    },
    
    "general_health": {
        "description": "General wellness questions, health education, lifestyle advice",
        "keywords": ["healthy", "wellness", "prevention", "nutrition", "exercise", "general"],
        "model": "general_health_llm",
        "urgency_features": ["routine"]
    }
}

URGENCY_LEVELS = {
    "emergency": "Immediate medical attention required - direct to emergency services",
    "urgent": "Medical consultation needed within 24-48 hours",
    "needs_attention": "Medical follow-up recommended within 1-2 weeks",
    "routine": "Standard health guidance, no immediate concern"