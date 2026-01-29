-- =============================================================================
-- WELLNEST APPLICATION - DATABASE FOUNDATION SETUP
-- =============================================================================
-- Purpose: Create all required schemas and tables for the WellNest application
-- Run this FIRST before any application code
-- =============================================================================

-- Step 1: Create Database and Schemas
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS WELLNEST;

-- Create schemas for organization
CREATE SCHEMA IF NOT EXISTS WELLNEST.USER_MANAGEMENT;
CREATE SCHEMA IF NOT EXISTS WELLNEST.MEDICAL_DATA;
CREATE SCHEMA IF NOT EXISTS WELLNEST.APP_LOGS;

-- Set context
USE DATABASE WELLNEST;
USE WAREHOUSE WELLNEST;  -- Make sure your warehouse exists

-- =============================================================================
-- Step 2: USER AUTHENTICATION TABLES
-- =============================================================================

-- 2.1: Users Table (MUST CREATE FIRST - Everything depends on this)
CREATE TABLE IF NOT EXISTS WELLNEST.USER_MANAGEMENT.USERS (
    USER_ID VARCHAR(36) PRIMARY KEY,                    -- UUID for user
    EMAIL VARCHAR(255) UNIQUE NOT NULL,                 -- Login email
    HASHED_PASSWORD VARCHAR(255) NOT NULL,              -- Bcrypt hashed password
    FULL_NAME VARCHAR(255) NOT NULL,                    -- User's full name
    DATE_OF_BIRTH DATE,                                 -- For age-related features
    GENDER VARCHAR(50),                                 -- male/female/other/prefer_not_to_say
    PHONE_NUMBER VARCHAR(20),                           -- Optional contact
    ACCOUNT_STATUS VARCHAR(20) DEFAULT 'active',        -- active/suspended/deleted
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    LAST_LOGIN TIMESTAMP_NTZ,
    FAILED_LOGIN_ATTEMPTS INTEGER DEFAULT 0,            -- Security tracking
    ACCOUNT_LOCKED_UNTIL TIMESTAMP_NTZ,                 -- Temporary lock after failed attempts
    EMAIL_VERIFIED BOOLEAN DEFAULT FALSE,               -- Email verification status
    TERMS_ACCEPTED BOOLEAN DEFAULT FALSE,               -- Legal agreement tracking
    PRIVACY_CONSENT BOOLEAN DEFAULT FALSE               -- HIPAA/privacy consent
);

-- Security indexes
-- CREATE INDEX IF NOT EXISTS idx_users_email 
--     ON WELLNEST.USER_MANAGEMENT.USERS(EMAIL);

-- CREATE INDEX IF NOT EXISTS idx_users_account_status 
--     ON WELLNEST.USER_MANAGEMENT.USERS(ACCOUNT_STATUS);

-- =============================================================================
-- Step 3: MEDICAL PROFILE TABLE
-- =============================================================================

-- 3.1: User Medical Profiles
CREATE TABLE IF NOT EXISTS WELLNEST.USER_MANAGEMENT.USER_MEDICAL_PROFILES (
    PROFILE_ID VARCHAR(36) PRIMARY KEY,
    USER_ID VARCHAR(36) NOT NULL,                       -- Foreign key to USERS
    
    -- Basic Health Information
    HEIGHT_CM FLOAT,                                    -- Height in centimeters
    WEIGHT_KG FLOAT,                                    -- Current weight
    BMI FLOAT,                                          -- Calculated BMI
    BLOOD_TYPE VARCHAR(10),                             -- A+, B-, O+, etc.
    
    -- Medical History Flags
    HAS_DIABETES BOOLEAN DEFAULT FALSE,
    HAS_HYPERTENSION BOOLEAN DEFAULT FALSE,
    HAS_HEART_DISEASE BOOLEAN DEFAULT FALSE,
    HAS_MENTAL_HEALTH_HISTORY BOOLEAN DEFAULT FALSE,
    HAS_PCOS BOOLEAN DEFAULT FALSE,
    
    -- Lifestyle Factors
    SMOKING_STATUS VARCHAR(50),                         -- never/former/current
    ALCOHOL_CONSUMPTION VARCHAR(50),                    -- none/moderate/heavy
    EXERCISE_FREQUENCY VARCHAR(50),                     -- sedentary/moderate/active
    
    -- Women's Health (if applicable)
    IS_PREGNANT BOOLEAN DEFAULT FALSE,
    PREGNANCY_TRIMESTER INTEGER,                        -- 1, 2, or 3
    MENSTRUAL_CYCLE_REGULAR BOOLEAN,
    LAST_MENSTRUAL_PERIOD DATE,
    
    -- Emergency Contact
    EMERGENCY_CONTACT_NAME VARCHAR(255),
    EMERGENCY_CONTACT_PHONE VARCHAR(20),
    EMERGENCY_CONTACT_RELATIONSHIP VARCHAR(100),
    
    -- Metadata
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    LAST_UPDATED TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Foreign Key Constraint
    FOREIGN KEY (USER_ID) REFERENCES WELLNEST.USER_MANAGEMENT.USERS(USER_ID)
);

-- -- Index for quick profile lookup
-- CREATE INDEX IF NOT EXISTS idx_medical_profile_user 
--     ON WELLNEST.USER_MANAGEMENT.USER_MEDICAL_PROFILES(USER_ID);

-- =============================================================================
-- Step 4: DOCUMENT STORAGE TABLE
-- =============================================================================

-- 4.1: Uploaded Medical Documents
CREATE TABLE IF NOT EXISTS WELLNEST.MEDICAL_DATA.UPLOADED_DOCUMENTS (
    DOCUMENT_ID VARCHAR(36) PRIMARY KEY,
    USER_ID VARCHAR(36) NOT NULL,                       -- Foreign key to USERS
    
    -- Document Metadata
    ORIGINAL_FILENAME VARCHAR(500) NOT NULL,            -- User's original file name
    DOCUMENT_TYPE VARCHAR(100),                         -- lab_report/prescription/imaging/other
    UPLOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FILE_SIZE_BYTES INTEGER,                            -- File size for storage tracking
    
    -- Storage Information
    SNOWFLAKE_STAGE_PATH VARCHAR(1000),                 -- Path in Snowflake stage
    S3_BUCKET_PATH VARCHAR(1000),                       -- If using external S3
    
    -- Processing Status
    PROCESSING_STATUS VARCHAR(50) DEFAULT 'pending',    -- pending/processing/completed/failed
    PROCESSING_STARTED_AT TIMESTAMP_NTZ,
    PROCESSING_COMPLETED_AT TIMESTAMP_NTZ,
    PROCESSING_ERROR_MESSAGE VARCHAR(5000),             -- Error details if failed
    
    -- Extracted Data (JSON format)
    EXTRACTED_DATA VARIANT,                             -- Structured JSON of extracted medical data
    EXTRACTION_CONFIDENCE_SCORE FLOAT,                  -- 0.0 to 1.0 confidence
    
    -- Document Classification
    DETECTED_TEST_TYPES ARRAY,                          -- [glucose, cholesterol, BP, etc.]
    DETECTED_DATE DATE,                                 -- Test/report date from document
    ISSUING_FACILITY VARCHAR(255),                      -- Hospital/lab name if extracted
    
    -- Security & Compliance
    IS_ENCRYPTED BOOLEAN DEFAULT TRUE,
    HIPAA_COMPLIANT BOOLEAN DEFAULT TRUE,
    DATA_RETENTION_UNTIL DATE,                          -- Auto-delete after X years
    
    -- Foreign Key Constraint
    FOREIGN KEY (USER_ID) REFERENCES WELLNEST.USER_MANAGEMENT.USERS(USER_ID)
);

-- -- Indexes for efficient queries
-- CREATE INDEX IF NOT EXISTS idx_documents_user 
--     ON WELLNEST.MEDICAL_DATA.UPLOADED_DOCUMENTS(USER_ID);

-- CREATE INDEX IF NOT EXISTS idx_documents_upload_date 
--     ON WELLNEST.MEDICAL_DATA.UPLOADED_DOCUMENTS(UPLOAD_TIMESTAMP);

-- CREATE INDEX IF NOT EXISTS idx_documents_processing_status 
--     ON WELLNEST.MEDICAL_DATA.UPLOADED_DOCUMENTS(PROCESSING_STATUS);

-- =============================================================================
-- Step 5: CONVERSATION HISTORY TABLE
-- =============================================================================

-- 5.1: Chat Conversation History
CREATE TABLE IF NOT EXISTS WELLNEST.USER_MANAGEMENT.CONVERSATION_HISTORY (
    CONVERSATION_ID VARCHAR(36) PRIMARY KEY,
    USER_ID VARCHAR(36) NOT NULL,                       -- Foreign key to USERS
    SESSION_ID VARCHAR(36) NOT NULL,                    -- Group messages by session
    
    -- Message Content
    MESSAGE_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    USER_MESSAGE VARCHAR(10000) NOT NULL,               -- What user asked
    ASSISTANT_RESPONSE VARCHAR(10000) NOT NULL,         -- WellNest's response
    
    -- Routing Information
    ROUTED_TO_DOMAIN VARCHAR(50),                       -- lifestyle/mental_health/womens_wellness
    USED_LLM_MODEL VARCHAR(100),                        -- Which fine-tuned model was used
    ROUTER_CONFIDENCE_SCORE FLOAT,                      -- Router's confidence (0-1)
    
    -- Clinical Assessment
    URGENCY_LEVEL VARCHAR(20),                          -- emergency/urgent/routine
    DETECTED_SYMPTOMS ARRAY,                            -- Array of symptom keywords
    FOLLOW_UP_RECOMMENDED BOOLEAN DEFAULT FALSE,
    
    -- Context Used
    REFERENCED_DOCUMENTS ARRAY,                         -- Document IDs used in response
    USED_MEDICAL_PROFILE BOOLEAN DEFAULT TRUE,          -- Whether profile data was considered
    
    -- Metadata
    RESPONSE_TIME_SECONDS FLOAT,                        -- Latency tracking
    TOKENS_USED INTEGER,                                -- For cost tracking
    
    -- Foreign Key Constraint
    FOREIGN KEY (USER_ID) REFERENCES WELLNEST.USER_MANAGEMENT.USERS(USER_ID)
);

-- -- Indexes for conversation retrieval
-- CREATE INDEX IF NOT EXISTS idx_conv_history_user 
--     ON WELLNEST.USER_MANAGEMENT.CONVERSATION_HISTORY(USER_ID);

-- CREATE INDEX IF NOT EXISTS idx_conv_history_session 
--     ON WELLNEST.USER_MANAGEMENT.CONVERSATION_HISTORY(SESSION_ID);

-- CREATE INDEX IF NOT EXISTS idx_conv_history_timestamp 
--     ON WELLNEST.USER_MANAGEMENT.CONVERSATION_HISTORY(MESSAGE_TIMESTAMP);

-- =============================================================================
-- Step 6: APPLICATION LOGS TABLE (Optional but recommended)
-- =============================================================================

-- 6.1: Error and Activity Logging
CREATE TABLE IF NOT EXISTS WELLNEST.APP_LOGS.APPLICATION_LOGS (
    LOG_ID VARCHAR(36) PRIMARY KEY,
    LOG_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Log Details
    LOG_LEVEL VARCHAR(20) NOT NULL,                     -- INFO/WARNING/ERROR/CRITICAL
    LOG_SOURCE VARCHAR(100),                            -- Which component logged this
    LOG_MESSAGE VARCHAR(5000),                          -- Detailed log message
    
    -- Contextual Information
    USER_ID VARCHAR(36),                                -- User involved (if applicable)
    SESSION_ID VARCHAR(36),                             -- Session involved (if applicable)
    FUNCTION_NAME VARCHAR(255),                         -- Which function/endpoint
    
    -- Error Details (if applicable)
    ERROR_TYPE VARCHAR(255),                            -- Exception class name
    STACK_TRACE VARCHAR(10000),                         -- Full stack trace
    
    -- Request Information
    REQUEST_PAYLOAD VARIANT,                            -- Input data (sanitized)
    RESPONSE_STATUS INTEGER                             -- HTTP status code
);

-- -- Index for log analysis
-- CREATE INDEX IF NOT EXISTS idx_logs_timestamp 
--     ON WELLNEST.APP_LOGS.APPLICATION_LOGS(LOG_TIMESTAMP);

-- CREATE INDEX IF NOT EXISTS idx_logs_level 
--     ON WELLNEST.APP_LOGS.APPLICATION_LOGS(LOG_LEVEL);

-- =============================================================================
-- Step 7: CREATE INITIAL ADMIN USER (For Testing)
-- =============================================================================

-- Insert a test user so you can log in immediately
-- Password: WellNest2024! (You should change this)

MERGE INTO WELLNEST.USER_MANAGEMENT.USERS AS target
USING (
    SELECT 
        'admin-test-user-001' AS USER_ID,
        'admin@wellnest.com' AS EMAIL,
        '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY5GyYzpLaEQ5K.' AS HASHED_PASSWORD,
        'Admin Test User' AS FULL_NAME,
        '1990-01-01'::DATE AS DATE_OF_BIRTH,
        'prefer_not_to_say' AS GENDER,
        'active' AS ACCOUNT_STATUS,
        TRUE AS EMAIL_VERIFIED,
        TRUE AS TERMS_ACCEPTED,
        TRUE AS PRIVACY_CONSENT
) AS source
ON target.USER_ID = source.USER_ID

-- If user exists, update their info
WHEN MATCHED THEN UPDATE SET
    EMAIL = source.EMAIL,
    FULL_NAME = source.FULL_NAME,
    ACCOUNT_STATUS = source.ACCOUNT_STATUS,
    LAST_LOGIN = CURRENT_TIMESTAMP()

-- If user doesn't exist, create them
WHEN NOT MATCHED THEN INSERT (
    USER_ID,
    EMAIL,
    HASHED_PASSWORD,
    FULL_NAME,
    DATE_OF_BIRTH,
    GENDER,
    ACCOUNT_STATUS,
    EMAIL_VERIFIED,
    TERMS_ACCEPTED,
    PRIVACY_CONSENT,
    CREATED_AT
) VALUES (
    source.USER_ID,
    source.EMAIL,
    source.HASHED_PASSWORD,
    source.FULL_NAME,
    source.DATE_OF_BIRTH,
    source.GENDER,
    source.ACCOUNT_STATUS,
    source.EMAIL_VERIFIED,
    source.TERMS_ACCEPTED,
    source.PRIVACY_CONSENT,
    CURRENT_TIMESTAMP()
);

-- 7.2: MERGE medical profile
MERGE INTO WELLNEST.USER_MANAGEMENT.USER_MEDICAL_PROFILES AS target
USING (
    SELECT 
        'profile-admin-001' AS PROFILE_ID,
        'admin-test-user-001' AS USER_ID,
        170.0 AS HEIGHT_CM,
        70.0 AS WEIGHT_KG,
        24.2 AS BMI
) AS source
ON target.PROFILE_ID = source.PROFILE_ID

WHEN MATCHED THEN UPDATE SET
    HEIGHT_CM = source.HEIGHT_CM,
    WEIGHT_KG = source.WEIGHT_KG,
    BMI = source.BMI,
    LAST_UPDATED = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    PROFILE_ID,
    USER_ID,
    HEIGHT_CM,
    WEIGHT_KG,
    BMI,
    CREATED_AT,
    LAST_UPDATED
) VALUES (
    source.PROFILE_ID,
    source.USER_ID,
    source.HEIGHT_CM,
    source.WEIGHT_KG,
    source.BMI,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);

-- =============================================================================
-- Step 8: GRANT PERMISSIONS (Adjust roles as needed)
-- =============================================================================

-- Grant usage to appropriate roles
GRANT USAGE ON DATABASE WELLNEST TO ROLE SYSADMIN;
GRANT USAGE ON ALL SCHEMAS IN DATABASE WELLNEST TO ROLE SYSADMIN;

-- Grant table permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA WELLNEST.USER_MANAGEMENT TO ROLE SYSADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA WELLNEST.MEDICAL_DATA TO ROLE SYSADMIN;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA WELLNEST.APP_LOGS TO ROLE SYSADMIN;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================

-- Run these to verify everything was created successfully:

-- 1. Check all tables exist
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    ROW_COUNT,
    CREATED
FROM WELLNEST.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('USER_MANAGEMENT', 'MEDICAL_DATA', 'APP_LOGS')
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- 2. Verify test user exists
SELECT 
    USER_ID,
    EMAIL,
    FULL_NAME,
    ACCOUNT_STATUS,
    EMAIL_VERIFIED,
    CREATED_AT
FROM WELLNEST.USER_MANAGEMENT.USERS;


-- 3. Check indexes
SHOW INDEXES IN SCHEMA WELLNEST.USER_MANAGEMENT;

ALTER TABLE WELLNEST.MEDICAL_DATA.UPLOADED_DOCUMENTS
   ADD COLUMN FILE_CONTENT_BASE64 VARCHAR(16777216);




SELECT * FROM WELLNEST.USER_MANAGEMENT.USERS;