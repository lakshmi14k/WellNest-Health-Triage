import streamlit as st
import google.generativeai as genai
from datetime import datetime
import json

# Page configuration
st.set_page_config(
    page_title="WellNest - Your Health Assistant",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better UI
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    .chat-message {
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
        color: #ffffff;
    }
    .user-message {
        background-color: #1565c0;
        margin-left: 2rem;
    }
    .assistant-message {
        background-color: #424242;
        margin-right: 2rem;
    }
    .disclaimer {
        background-color: #856404;
        color: #ffffff;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #ffc107;
        margin: 1rem 0;
    }
    .urgency-emergency {
        background-color: #721c24;
        color: #ffffff;
        border-left: 4px solid #dc3545;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .urgency-urgent {
        background-color: #856404;
        color: #ffffff;
        border-left: 4px solid #ffc107;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .health-category {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 1rem;
        font-size: 0.85rem;
        margin: 0.25rem;
    }
    </style>
""", unsafe_allow_html=True)

# Health categories and their colors
HEALTH_CATEGORIES = {
    "Lifestyle Diseases": "#FF6B6B",
    "Diabetes": "#4ECDC4",
    "Heart Health": "#FF6B9D",
    "Mental Health": "#95E1D3",
    "Women's Health": "#F38181"
}

# System prompt for the health assistant
SYSTEM_PROMPT = """You are WellNest, an AI health assistant specializing in:
- Lifestyle Diseases (obesity, hypertension, cholesterol)
- Diabetes (Type 1, Type 2, gestational)
- Heart Health (cardiovascular conditions, prevention)
- Mental Health (anxiety, depression, stress, sleep disorders)
- Women's Health (menstrual health, pregnancy, menopause, PCOS)

Your role:
1. Provide evidence-based health information and guidance
2. Ask clarifying questions to understand symptoms better
3. Assess urgency level: EMERGENCY, URGENT, or ROUTINE
4. ALWAYS include medical disclaimers
5. Recommend professional medical consultation when appropriate

Guidelines:
- Be empathetic and supportive
- Use clear, non-technical language
- Ask follow-up questions about: symptom duration, severity, related symptoms
- For emergency symptoms (chest pain, severe bleeding, suicidal thoughts), immediately advise seeking emergency care
- Never diagnose or prescribe medication
- Focus on education, prevention, and lifestyle modifications

CRITICAL: If you detect emergency symptoms, start your response with "⚠️ EMERGENCY:"
For urgent symptoms requiring prompt attention, start with "⚡ URGENT:"
For routine queries, start with "📋 ROUTINE:"
"""

def initialize_gemini(api_key):
    """Initialize Gemini API"""
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.0-flash-exp')
        return model
    except Exception as e:
        st.error(f"Error initializing Gemini: {str(e)}")
        return None

def get_urgency_level(response_text):
    """Extract urgency level from response"""
    if "⚠️ EMERGENCY:" in response_text:
        return "emergency"
    elif "⚡ URGENT:" in response_text:
        return "urgent"
    else:
        return "routine"

def display_urgency_banner(urgency_level):
    """Display urgency banner based on assessment"""
    if urgency_level == "emergency":
        st.markdown("""
            <div class="urgency-emergency">
                <h3>⚠️ EMERGENCY - Seek Immediate Medical Attention</h3>
                <p>Call emergency services (911) or go to the nearest emergency room immediately.</p>
            </div>
        """, unsafe_allow_html=True)
    elif urgency_level == "urgent":
        st.markdown("""
            <div class="urgency-urgent">
                <h3>⚡ URGENT - Consult Healthcare Provider Soon</h3>
                <p>Please schedule an appointment with your healthcare provider within 24-48 hours.</p>
            </div>
        """, unsafe_allow_html=True)

def chat_with_model(model, user_message, chat_history):
    """Send message to Gemini and get response"""
    try:
        # Build conversation context
        conversation = SYSTEM_PROMPT + "\n\n"
        for msg in chat_history:
            role = "User" if msg["role"] == "user" else "Assistant"
            conversation += f"{role}: {msg['content']}\n\n"
        conversation += f"User: {user_message}\n\nAssistant:"
        
        # Generate response
        response = model.generate_content(conversation)
        return response.text
    except Exception as e:
        return f"Error generating response: {str(e)}"

# Initialize session state
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'api_key' not in st.session_state:
    st.session_state.api_key = None
if 'model' not in st.session_state:
    st.session_state.model = None

# Sidebar
with st.sidebar:
    st.markdown("## 🏥 WellNest Health Assistant")
    st.markdown("---")
    
    # API Key input
    api_key = st.text_input(
        "Gemini API Key",
        type="password",
        value=st.session_state.api_key or "",
        help="Enter your Google Gemini API key"
    )
    
    if api_key and api_key != st.session_state.api_key:
        st.session_state.api_key = api_key
        st.session_state.model = initialize_gemini(api_key)
        if st.session_state.model:
            st.success("✅ API Key configured!")
    
    st.markdown("---")
    
    # Health categories
    st.markdown("### Health Topics Covered:")
    for category, color in HEALTH_CATEGORIES.items():
        st.markdown(
            f'<span class="health-category" style="background-color: {color}; color: white;">{category}</span>',
            unsafe_allow_html=True
        )
    
    st.markdown("---")
    
    # Example questions
    st.markdown("### 💡 Example Questions:")
    example_questions = [
        "I've been feeling tired and thirsty lately. Could this be diabetes?",
        "What are early signs of heart disease?",
        "I'm having trouble sleeping. What can help?",
        "What lifestyle changes can help manage high blood pressure?",
        "What are common symptoms of PCOS?"
    ]
    
    for i, question in enumerate(example_questions):
        if st.button(question, key=f"example_{i}"):
            if st.session_state.model:
                st.session_state.chat_history.append({
                    "role": "user",
                    "content": question,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                st.rerun()
    
    st.markdown("---")
    
    # Clear chat button
    if st.button("🗑️ Clear Conversation"):
        st.session_state.chat_history = []
        st.rerun()
    
    # Download chat history
    if st.session_state.chat_history:
        chat_json = json.dumps(st.session_state.chat_history, indent=2)
        st.download_button(
            label="📥 Download Chat History",
            data=chat_json,
            file_name=f"wellnest_chat_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json"
        )

# Main content
st.markdown('<h1 class="main-header">🏥 WellNest Health Assistant</h1>', unsafe_allow_html=True)

# Disclaimer
st.markdown("""
    <div class="disclaimer">
        <strong>⚠️ Medical Disclaimer:</strong> WellNest is an educational health information tool and does not provide medical diagnosis or treatment. 
        Always consult qualified healthcare professionals for medical advice. In case of emergency, call 911 or go to the nearest emergency room.
    </div>
""", unsafe_allow_html=True)

# Check if API key is configured
if not st.session_state.model:
    st.warning("👈 Please enter your Gemini API key in the sidebar to start chatting.")
    st.markdown("""
        ### How to get your Gemini API key:
        1. Visit [Google AI Studio](https://makersuite.google.com/app/apikey)
        2. Sign in with your Google account
        3. Click "Create API Key"
        4. Copy the key and paste it in the sidebar
    """)
else:
    # Display chat history
    for message in st.session_state.chat_history:
        if message["role"] == "user":
            st.markdown(
                f'<div class="chat-message user-message"><strong>You:</strong><br>{message["content"]}</div>',
                unsafe_allow_html=True
            )
        else:
            urgency = get_urgency_level(message["content"])
            display_urgency_banner(urgency)
            st.markdown(
                f'<div class="chat-message assistant-message"><strong>WellNest:</strong><br>{message["content"]}</div>',
                unsafe_allow_html=True
            )
    
    # Process pending user message (from example button clicks)
    if st.session_state.chat_history and st.session_state.chat_history[-1]["role"] == "user":
        last_message = st.session_state.chat_history[-1]["content"]
        
        with st.spinner("🤔 Analyzing your query..."):
            response = chat_with_model(
                st.session_state.model,
                last_message,
                st.session_state.chat_history[:-1]
            )
            
            st.session_state.chat_history.append({
                "role": "assistant",
                "content": response,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            st.rerun()
    
    # Chat input
    user_input = st.chat_input("Ask about your health concerns...")
    
    if user_input:
        # Add user message to history
        st.session_state.chat_history.append({
            "role": "user",
            "content": user_input,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
        # Get AI response
        with st.spinner("🤔 Analyzing your query..."):
            response = chat_with_model(
                st.session_state.model,
                user_input,
                st.session_state.chat_history[:-1]
            )
            
            st.session_state.chat_history.append({
                "role": "assistant",
                "content": response,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            st.rerun()

# Footer
st.markdown("---")
st.markdown("""
    <div style="text-align: center; color: #666; font-size: 0.9rem;">
        <p>WellNest MVP - Healthcare AI Assistant | Built with Streamlit & Google Gemini</p>
        <p>Remember: This is an educational tool. Always consult healthcare professionals for medical advice.</p>
    </div>
""", unsafe_allow_html=True)