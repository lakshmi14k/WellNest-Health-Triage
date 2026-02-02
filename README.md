**WellNest Health Triage System : An AI-Powered Non-Emergency Health Management Platform**

**Problem Statement**: Millions face lifestyle-related health conditions (diabetes, hypertension, sleep disorders, mental health issues) with care that is reactive, fragmented, and costly. Patients lack continuous monitoring, personalized guidance, and accessible 24/7 support for routine health questions.

**The Impact:**
- 1 in 3 adults live with a lifestyle-related condition
- 70% of healthcare costs linked to preventable conditions
- Healthcare access gaps leave patients without timely guidance

**Solution Overview**: WellNest is a non-emergency AI health companion that bridges the gap between complex medical information and everyday understanding. The system:

- **Automates routine health tracking** across diabetes, hypertension, mental wellness, and maternal health
- **Provides 24/7 AI wellness support** through conversational interface
- **Delivers personalized treatment plans** using multi-LLM architecture with domain-specific specialists
- **Enhances diagnostic accuracy** through intelligent query routing and context-aware responses

**What Makes WellNest Unique:**
- Unlike general LLMs, WellNest uses fine-tuned models trained on clinical notes, research papers, and medical datasets
- Multi-domain routing architecture prevents information overload - specialized models achieve 99.5% accuracy vs. 85% for generalist approaches
- Complete data pipeline with medallion architecture ensures data quality and governance

**Technical Contributions:**

**Data Engineering Pipeline:**

**Built end-to-end dbt transformation pipeline** processing 7 healthcare datasets through 18+ models:

 1. Exploratory Data Analysis & Data Quality Assessment:
- Conducted Python-based EDA identifying critical patterns and data quality issues across 7 medical datasets
- Analyzed missing data patterns, feature correlations, and domain-specific data quality requirements
- Established data validation rules and cleansing requirements for healthcare context

 2. Medallion Architecture Implementation (Bronze → Silver → Gold)

**Bronze Layer (Raw Data Ingestion):**
- Ingested 7 raw healthcare datasets into Snowflake:
  - Blood Pressure Data
  - Diabetes Data
  - Mental Health Data
  - PCOS Data
  - PCOS Infertility Data
  - Maternal Health Data
  - Menstrual Health Data
- Preserved historical data with append-only architecture

**Silver Layer (Data Cleaning & Standardization):**
- Created 7 staging models (stg_*_cleaned) applying data quality rules:
  - Deduplication and null value handling strategies tailored per domain
  - Data type validation and format standardization
  - Outlier detection and treatment using clinical thresholds
  - Feature normalization and unit conversions
- Resolved dbt-Snowflake authentication challenges using Personal Access Tokens for trial accounts

**Gold Layer (Feature Engineering & ML-Ready Datasets):**
- Engineered 96 domain-specific features across 9 feature tables:
  - **Core Clinical Features:** Normalized symptom terms, standardized units, severity classifications
  - **Risk-Urgency Features:** Emergency tagging, safety classification, urgency levels
  - **Prompting Features:** Natural language framing, instruction-response pairs for LLM fine-tuning
- Created features aligned with clinical guidelines (WHO, NIH standards)
- Examples: Pulse pressure calculations (cardiovascular risk), BMI categorization, glucose-HbA1c concordance, metabolic syndrome scoring

 3. Feature Engineering for LLM Fine-Tuning:

**Expanded feature space from 10-18 raw columns to 96 engineered features:**
- **Purpose:** Provide richer context for LLM training beyond limited raw information
- **Approach:** Encoded domain expertise, captured non-obvious relationships, created interpretable signals
- **Example (Diabetes):** 
  - Before: 9 raw columns (gender, age, BMI, glucose, HbA1c, etc.)
  - After: 32 features including diabetes_stage, glucose_control_status, cardiometabolic_disease_count, cardiovascular_risk_score, smoking_cessation_priority

**LLM-Ready Dataset Preparation:**
- Conversational framing for supervised fine-tuning instruction pairs
- Safety classification preventing emergency guidance
- Standardized formats for consistent model training

 4. Data Quality & Governance:

- Implemented data lineage tracking through dbt DAG
- Created data validation tests ensuring pipeline reliability
- Established feature quality standards for ML model consumption
- Prevented feature leakage by identifying and removing 7 leaked features that provided direct target information

**Team Collaboration Context:**

**13-Week Capstone Project | Team of 3**

This was a collaborative academic project where responsibilities were divided by technical domain:

**Primary Role: Data Engineering & Pipeline:**
- Complete dbt transformation pipeline
- Medallion architecture implementation
- Feature engineering for ML/LLM readiness
- Data quality and validation

**Teams' Primary Roles:**
- **ML Modeling & LLM Fine-Tuning :** Traditional ML baseline (XGBoost, Random Forest, SVM), Snowflake Cortex LLM fine-tuning, router logic implementation
- **Backend & Frontend Development :** Snowflake integration, Streamlit UI, user authentication, guardrails implementation

**Collaborative Work:**
- System architecture design
- Integration testing
- Safety validation
- Documentation

**Tech Stack:**

| Component | Technology |
|-----------|-----------|
| **Cloud Platform** | Snowflake (data warehouse, Cortex LLM hosting) |
| **Data Transformation** | dbt Core (18+ models, medallion architecture) |
| **Exploratory Analysis** | Python (pandas, numpy, matplotlib, seaborn) |
| **ML Baseline Models** | XGBoost, Random Forest, SVM (93.5% accuracy) |
| **Router LLM** | Claude Sonnet 4 (query classification, routing orchestration) |
| **Domain LLMs** | Llama 3.1-8B (fine-tuned, 99.5% accuracy per domain) |
| **Frontend** | Streamlit (conversational interface, dashboards) |
| **Backend** | Python, Snowflake Stored Procedures |
| **Authentication** | Snowflake user management, session state |

**Project Structure**
```
WellNest-Health-Triage/
├── models/                            # dbt transformation models
│   ├── staging/                       # Silver layer: Data cleaning
│   │   ├── stg_bloodpressure_cleaned.sql
│   │   ├── stg_diabetes_cleaned.sql
│   │   ├── stg_mentalhealth_cleaned.sql
│   │   ├── stg_pcos_cleaned.sql
│   │   ├── stg_pcos_infertility_cleaned.sql
│   │   ├── stg_maternalhealth_cleaned.sql
│   │   └── stg_menstrual_cleaned.sql
│   ├── intermediate/                  # Business logic transformations
│   │   ├── int_diabetes_risk_urgency.sql
│   │   ├── int_hypertension_risk_urgency.sql
│   │   └── int_mental_health_risk_urgency.sql
│   └── marts/                         # Gold layer: Feature engineering
│       ├── ftr_diabetes_core_clinical.sql
│       ├── ftr_diabetes_risk_urgency.sql
│       ├── ftr_diabetes_conversation_prompts.sql
│       ├── ftr_hypertension_core_clinical.sql
│       ├── ftr_hypertension_risk_urgency.sql
│       ├── ftr_hypertension_conversation_prompts.sql
│       ├── ftr_mental_health_core_clinical.sql
│       ├── ftr_mental_health_risk_urgency.sql
│       └── ftr_pcos_core_clinical.sql
│
├── seeds/                             # Reference data
├── macros/                            # Reusable dbt macros
├── analyses/                          # Ad-hoc analytical queries
├── snapshots/                         # Historical data snapshots
│
├── EDA/                               # Exploratory data analysis notebooks
│
├── ML_Models/                         # Baseline ML experiments
├── agents/                            # LLM routing logic
├── StreamLit/                         # Frontend application
├── Demo_App/                          # Demo interface
├── eval/                              # Model evaluation scripts
├── tests/                             # Data validation tests
├── misc/                              # Miscellaneous utilities
│
├── docs/
│   ├── COLLABORATION_CONTEXT.md       # Team roles and attribution
│   ├── DBT_PIPELINE.md                # Technical deep-dive on transformations
│   └── WellNest Final Presentation.pptx # Project presentation
│
├── dbt_project.yml                    # dbt configuration
├── .gitignore
└── README.md
```

**System Architecture:**

 Data Flow Pipeline:

```
Raw Data Sources (7 datasets)
         ↓
   Snowflake Bronze Layer (Raw tables)
         ↓
   dbt Transformation Pipeline
         ├─ Silver Layer: Staging models (data cleaning)
         ├─ Intermediate Layer: Business logic
         └─ Gold Layer: Feature engineering (ML-ready)
         ↓
   LLM Fine-Tuning Data
         ├─ Baseline ML Models (XGBoost, RF, SVM)
         └─ Snowflake Cortex Fine-Tuning (Llama 3.1-8B)
         ↓
   Production System
```

 User Query Flow:

```
User Query (Streamlit Interface)
         ↓
   Guardrails Layer (Keyword filtering, safety checks)
         ↓
   Router LLM (Claude Sonnet 4)
    ├─ Query classification
    ├─ Domain detection
    ├─ Urgency assessment
    └─ Routing decision
         ↓
   Domain-Specific LLM (Fine-tuned Llama 3.1-8B)
    ├─ Diabetes Model
    ├─ Mental Health Model
    └─ Hypertension Model
         ↓
   Response Post-Processing
    ├─ Safety validation
    ├─ Medical disclaimer addition
    └─ Emergency escalation (if needed)
         ↓
   Response to User + Conversation Logging
```

**How It Works:**

 1. Data Processing Foundation:
The system begins with clean, feature-rich data:
- 7 raw medical datasets ingested into Snowflake Bronze layer
- dbt pipeline transforms data through Silver (cleaning) and Gold (feature engineering) layers
- 96 engineered features provide clinical context for AI models

 2. User Interaction:
- User signs up and creates health profile in Snowflake
- User submits health query through Streamlit interface
- Query Intent Classifier detects category (diabetes, mental health, hypertension)

 3. Intelligent Routing: (Claude Sonnet 4)
- Receives query + classification + user profile
- Routes to appropriate domain-specific expert model
- Structures and standardizes final response

 4. Domain Expertise: (Fine-tuned Llama 3.1-8B)
Three specialized models trained on the feature-engineered datasets:
- **Diabetes Model:** Trained on diabetes-specific features
- **Mental Health Model:** Sleep, stress, anxiety specialization
- **Hypertension Model:** Blood pressure and cardiovascular focus

 5. Response Delivery:
- Router LLM structures the specialist's output
- Safety guardrails validate response appropriateness
- Medical disclaimers added automatically
- Emergency escalation triggers if life-threatening symptoms detected

**Results & Performance:**

 Data Pipeline Metrics:
- **Datasets Processed:** 7 healthcare domains
- **dbt Models Created:** 18+ transformations (7 staging, 3 intermediate, 9 marts)
- **Features Engineered:** 96 domain-specific features
- **Data Quality:** <1% missing values in gold layer, zero duplicates

 Model Performance:
- **Baseline ML (Evaluation work):** 93.5% accuracy (Random Forest)
  - Identified feature leakage issues requiring pipeline refinement
  - Established performance benchmark for LLM comparison
- **Domain LLMs (Fine-tuning):** 99.5% accuracy per specialist
  - 6.5% improvement over traditional ML
  - Perfect stability (no catastrophic failures)
- **Multi-Domain Routing:** 95% confidence in domain classification

 System Capabilities:
- **Conversational AI:** Natural language health query understanding
- **24/7 Availability:** Automated triage and guidance
- **Safety Compliance:** 3-layer guardrails (keyword filter, router validation, response safety)
- **Personalized Context:** User profile integration for tailored recommendations

**Installation & Setup:**

 Prerequisites:
- Snowflake account with Cortex LLM access
- dbt Core installed (`pip install dbt-snowflake`)
- Python 3.8+ with required packages

 dbt Pipeline Setup:

1. **Configure Snowflake Connection:**
```yaml
 ~/.dbt/profiles.yml
wellnest:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: YOUR_ACCOUNT
      user: YOUR_USER
      authenticator: oauth   Use Personal Access Token
      token: YOUR_PAT_TOKEN
      database: WELLNEST
      warehouse: COMPUTE_WH
      schema: PUBLIC
      threads: 4
```

2. **Clone Repository:**
```bash
git clone https://github.com/lakshmi14k/WellNest-Health-Triage.git
cd WellNest-Health-Triage
```

3. **Run dbt Pipeline:**
```bash
 Test connection
dbt debug

 Run all transformations
dbt run

 Run with specific models
dbt run --select staging
dbt run --select marts

 Test data quality
dbt test
```

4. **Generate Documentation:**
```bash
dbt docs generate
dbt docs serve
```

**Research Context:**

This project implements concepts from:
**"Conversational Medical AI: Ready for Practice?"** - Research demonstrating AI-assisted medical conversations enhance patient experience while maintaining safety through physician oversight.

**Key Parallels:**
- Multi-domain LLM architecture for varied health issues
- Specialized routing for clinical domains
- Router LLM classifies patient queries
- Domain-specific LLMs provide specialized guidance
- Structured response formatting for consistency

**WellNest's Additional Contributions:**
- Complete medallion data architecture with dbt
- Feature engineering at scale (96 features across domains)
- Demonstrated improvement: 93.5% (ML baseline) → 99.5% (fine-tuned LLM)

**Documentation:**

- **[Collaboration Context](docs/COLLABORATION_CONTEXT.md)** - Detailed role breakdown and team contributions
- **[dbt Pipeline Guide](docs/DBT_PIPELINE.md)** - Technical deep-dive on data transformations
- **[Data Lineage](docs/DATA_LINEAGE.md)** - Visual architecture and data flow diagrams

**Important Disclaimers:**

**Medical Disclaimer:**
- WellNest is a non-emergency health education tool, NOT a medical diagnosis system
- All guidance includes disclaimers to consult healthcare providers
- Emergency symptoms trigger immediate escalation to 911/emergency services
- System designed for lifestyle health management, not clinical decision-making

**Academic Project:**
- Built as a 13-week capstone project at Northeastern University
- Demonstrates AI/ML engineering capabilities in healthcare domain
- Not intended for production medical use without regulatory compliance

*Built with dbt, Snowflake, Llama 3.1, Claude Sonnet 4, and Streamlit*
