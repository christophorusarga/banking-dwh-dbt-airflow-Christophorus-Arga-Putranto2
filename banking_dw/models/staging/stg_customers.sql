{{ config(
    materialized='view',
    tags=['staging', 'customers']
) }}

-- Define the raw source for the customers data
-- Assumes your dbt project has a source named 'raw' which includes a table named 'customers'
WITH source AS (
    SELECT * FROM {{ ref('customers') }}
),

-- Apply basic cleaning, standardization, and simple transformations
staged AS (
    SELECT
        -- Primary Identifier
        customer_id::VARCHAR AS customer_id,

        -- Name Fields & Derived Column
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        -- Concatenate names for a full name field
        TRIM(first_name || ' ' || last_name) AS full_name,

        -- Personal Details & Standardization
        LOWER(email) AS email,
        phone::VARCHAR AS phone,
        LOWER(gender) AS gender,
        
        -- Demographic Details
        address,
        city,
        province,
        postal_code,
        LOWER(occupation) AS occupation,
        LOWER(income_level) AS income_level,
        risk_rating,

        -- Date & Time Fields (Explicit Casting)
        date_of_birth::DATE AS date_of_birth,
        registration_date::DATE AS registration_date,
        last_updated::TIMESTAMP AS last_updated

    FROM source
    -- Filter out records missing a primary identifier
    WHERE customer_id IS NOT NULL
)

SELECT * FROM staged