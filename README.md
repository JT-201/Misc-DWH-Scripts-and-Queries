# Misc Data Warehouse Scripts and Queries

This repository contains various Python scripts and SQL queries for healthcare/wellness program analytics, focusing on member engagement, health outcomes tracking, and business intelligence reporting for partner organizations.

## **Script Summary**

### **DWH Maintenance Scripts**
- **`Historic_batch_script.py`** - Runs AWS Glue jobs to process historical member-level events data in batches with retry logic and concurrent execution management
- **`solera_correction_manager.py`** - Sends correction events to Solera API to erase/correct data for specific time periods, with batch processing and error handling

### **QBR (Quarterly Business Review) Scripts**
- **`amazon_qbr_analysis.py`** - Comprehensive health outcomes analysis for Amazon users including weight loss, blood pressure, A1C management, and demographic breakdowns with GLP1 medication tracking
- **`apple_qbr_bmi_analysis.py`** - Focused BMI analysis for Apple users showing baseline vs current BMI metrics and improvement statistics
- **`apple_qbr_original.py`** - Full QBR analysis for Apple users covering health outcomes, demographics, module completion, and engagement metrics across multiple categories
- **`kwiktrip_qbr_analysis.py`** - Similar to Amazon analysis but for Kwik Trip users, analyzing health outcomes and demographic patterns
- **`Load_bodyweight_data.py`** - Utility script to load CSV body weight data into the cleaned database table with proper UUID conversion
- **`willscot_analysis_optimized.py`** - QBR analysis for WillScot users with comprehensive health metrics but without GLP1 medication tracking

### **SQL Scripts**
- **`BillableActivities.sql`** - Queries to analyze care team interactions, vital sign recordings, and other billable activities by month/quarter for Amazon users
- **`load_bmi.sql`** - Calculates and inserts BMI values by combining body weight and height data from separate tables
- **`PCOS_CoConditions.sql`** - Analyzes users with PCOS and specific co-conditions, filtered by BMI thresholds and medical eligibility status
- **`user_medication_counts.sql`** - Counts users by medication types (metformin, statins, insulin, therapy types, GLP1) for Amazon users

### **Whitepaper Scripts**
- **`cohort_analysis_optimized_retention.py`** - Advanced cohort analysis focusing on user retention patterns over 6+ months with activity span requirements
- **`cohort_analysis_optimized.py`** - Comprehensive cohort analysis for Apple/Amazon users covering health outcomes, engagement metrics, and retention analysis
- **`cohort_engagement_metrics_standalone_quarterly.py`** - Standalone engagement metrics analysis focused on quarterly retention patterns for current year
- **`cohort_engagement_metrics_standalone.py`** - Engagement metrics analysis for users active within specific date ranges with care team interaction tracking

## **Key Features**

- **Health Outcomes Tracking**: Weight loss analysis, BMI calculations, blood pressure management, A1C monitoring
- **Medication Analysis**: GLP1 prescription tracking, medication adherence, therapy type analysis
- **Engagement Metrics**: Care team interactions, module completion rates, user activity patterns
- **Demographic Analysis**: Breakdowns by age, gender, ethnicity, and geographic distribution
- **Partner-Specific Reports**: Customized analysis for Amazon, Apple, Kwik Trip, and WillScot users
- **Data Quality Management**: Error handling, data validation, and correction workflows
- **Performance Optimization**: Temporary table usage, indexing strategies, and batch processing

## **Technical Stack**

- **Python**: Primary scripting language with pandas, mysql-connector-python
- **MySQL**: Database queries and temporary table operations
- **AWS**: Integration with AWS Glue for data processing
- **Excel/CSV**: Output formats for business reporting
- **APIs**: Integration with Solera API for data corrections

All scripts are designed to support healthcare program analytics and provide actionable insights for program optimization and reporting.