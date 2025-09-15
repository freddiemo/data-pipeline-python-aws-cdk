-- Sample Athena Queries for Data Pipeline
-- Execute these queries after the Glue crawler has cataloged your data

-- 1. Basic data exploration
-- Count total number of users
SELECT COUNT(*) as total_users 
FROM data_pipeline_db.raw_data;

-- View first 10 records
SELECT * 
FROM data_pipeline_db.raw_data 
LIMIT 10;

-- Check data freshness
SELECT 
    MIN(extraction_timestamp) as earliest_data,
    MAX(extraction_timestamp) as latest_data,
    COUNT(*) as total_records
FROM data_pipeline_db.raw_data;

-- 2. Geographic analysis
-- Users by city
SELECT 
    address_city,
    COUNT(*) as user_count
FROM data_pipeline_db.raw_data
WHERE address_city IS NOT NULL
GROUP BY address_city
ORDER BY user_count DESC;

-- Users by geographic coordinates (find unique locations)
SELECT 
    address_lat,
    address_lng,
    address_city,
    COUNT(*) as users_at_location
FROM data_pipeline_db.raw_data
WHERE address_lat IS NOT NULL AND address_lng IS NOT NULL
GROUP BY address_lat, address_lng, address_city
ORDER BY users_at_location DESC;

-- 3. Contact information analysis
-- Email domain analysis
SELECT 
    SUBSTR(email, STRPOS(email, '@') + 1) as domain,
    COUNT(*) as count
FROM data_pipeline_db.raw_data
WHERE email IS NOT NULL
GROUP BY SUBSTR(email, STRPOS(email, '@') + 1)
ORDER BY count DESC;

-- Users with websites
SELECT 
    name,
    website,
    company_name,
    email
FROM data_pipeline_db.raw_data
WHERE website IS NOT NULL AND website != ''
ORDER BY name;

-- Phone number patterns (basic analysis)
SELECT 
    CASE 
        WHEN phone LIKE '1-%' THEN 'US Format'
        WHEN phone LIKE '(%' THEN 'Parentheses Format'
        WHEN REGEXP_LIKE(phone, '^[0-9-]+$') THEN 'Dash Format'
        ELSE 'Other Format'
    END as phone_format,
    COUNT(*) as count
FROM data_pipeline_db.raw_data
WHERE phone IS NOT NULL
GROUP BY 
    CASE 
        WHEN phone LIKE '1-%' THEN 'US Format'
        WHEN phone LIKE '(%' THEN 'Parentheses Format'
        WHEN REGEXP_LIKE(phone, '^[0-9-]+$') THEN 'Dash Format'
        ELSE 'Other Format'
    END
ORDER BY count DESC;

-- 4. Company analysis
-- Most common company names
SELECT 
    company_name,
    COUNT(*) as employee_count
FROM data_pipeline_db.raw_data
WHERE company_name IS NOT NULL
GROUP BY company_name
ORDER BY employee_count DESC;

-- Company catchphrases analysis
SELECT 
    company_catchphrase,
    company_name,
    COUNT(*) as usage_count
FROM data_pipeline_db.raw_data
WHERE company_catchphrase IS NOT NULL
GROUP BY company_catchphrase, company_name
ORDER BY usage_count DESC;

-- Business type analysis (from company_bs field)
SELECT 
    company_bs,
    COUNT(*) as count
FROM data_pipeline_db.raw_data
WHERE company_bs IS NOT NULL
GROUP BY company_bs
ORDER BY count DESC;

-- 5. Data quality checks
-- Check for duplicate emails
SELECT 
    email,
    COUNT(*) as count
FROM data_pipeline_db.raw_data
WHERE email IS NOT NULL
GROUP BY email
HAVING COUNT(*) > 1;

-- Check for missing critical fields
SELECT 
    SUM(CASE WHEN name IS NULL OR name = '' THEN 1 ELSE 0 END) as missing_names,
    SUM(CASE WHEN email IS NULL OR email = '' THEN 1 ELSE 0 END) as missing_emails,
    SUM(CASE WHEN phone IS NULL OR phone = '' THEN 1 ELSE 0 END) as missing_phones,
    SUM(CASE WHEN address_city IS NULL OR address_city = '' THEN 1 ELSE 0 END) as missing_cities,
    COUNT(*) as total_records
FROM data_pipeline_db.raw_data;

-- Data completeness by field
SELECT 
    'name' as field_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN name IS NOT NULL AND name != '' THEN 1 ELSE 0 END) as non_null_records,
    ROUND(
        100.0 * SUM(CASE WHEN name IS NOT NULL AND name != '' THEN 1 ELSE 0 END) / COUNT(*), 
        2
    ) as completeness_percentage
FROM data_pipeline_db.raw_data

UNION ALL

SELECT 
    'email' as field_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as non_null_records,
    ROUND(
        100.0 * SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) / COUNT(*), 
        2
    ) as completeness_percentage
FROM data_pipeline_db.raw_data

UNION ALL

SELECT 
    'phone' as field_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as non_null_records,
    ROUND(
        100.0 * SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) / COUNT(*), 
        2
    ) as completeness_percentage
FROM data_pipeline_db.raw_data;

-- 6. Time-based analysis (if you have historical data)
-- Data extraction trends (if running multiple times)
SELECT 
    DATE(extraction_timestamp) as extraction_date,
    COUNT(*) as records_extracted
FROM data_pipeline_db.raw_data
GROUP BY DATE(extraction_timestamp)
ORDER BY extraction_date DESC;

-- 7. Advanced queries
-- Create a user profile summary
SELECT 
    id,
    name,
    username,
    email,
    CONCAT(address_street, ', ', address_city) as full_address,
    company_name,
    phone,
    website
FROM data_pipeline_db.raw_data
WHERE email IS NOT NULL
ORDER BY name;

-- Find users in the same city working for different companies
SELECT 
    address_city,
    company_name,
    COUNT(*) as user_count,
    STRING_AGG(name, ', ') as users
FROM data_pipeline_db.raw_data
WHERE address_city IS NOT NULL AND company_name IS NOT NULL
GROUP BY address_city, company_name
HAVING COUNT(*) >= 1
ORDER BY address_city, user_count DESC;

-- Performance optimization example: Use partition projection for time-based queries
-- (This would require partitioning your data by date in S3)
/*
SELECT *
FROM data_pipeline_db.raw_data
WHERE year = '2025' AND month = '09' AND day = '14'
LIMIT 100;
*/
