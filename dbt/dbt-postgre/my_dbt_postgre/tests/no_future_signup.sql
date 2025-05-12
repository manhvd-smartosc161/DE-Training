-- Test to ensure no customer has a signup date in the future
-- This test passes because:
-- 1. In cleaned_customer.sql, we have converted created_at to signup_date as a date type
-- 2. In schema.yml, we have a test checking that signup_date <= CURRENT_DATE
-- 3. This test will return an empty result (0 rows) if no customer has signup_date > CURRENT_DATE
-- 4. In dbt, a test passes when no rows are returned

SELECT
    customer_id,
    email,
    signup_date
FROM {{ ref('cleaned_customer') }}
WHERE signup_date > CURRENT_DATE