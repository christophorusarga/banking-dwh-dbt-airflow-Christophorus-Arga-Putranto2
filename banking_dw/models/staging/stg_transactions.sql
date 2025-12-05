{{ config(
    materialized='view',
    tags=['staging', 'finance']
) }}

-- Define the raw source for the transactions data
-- Assumes your dbt project has a source named 'raw' which includes a table named 'transactions'
WITH source AS (
    SELECT * FROM {{ ref('transactions') }}
),

-- Apply basic cleaning, standardization, and simple transformations
staged AS (
    SELECT
        -- Primary Identifiers
        transaction_id,
        account_id,

        -- Combine Date and Time into a single TIMESTAMP field for easy analysis
        CAST(
            transaction_date || ' ' || transaction_time
            AS TIMESTAMP
        ) AS transaction_timestamp,
        
        -- Transaction Details & Standardization
        LOWER(transaction_type) AS transaction_type,
        amount,
        balance_after,
        TRIM(merchant_name) AS merchant_name,
        LOWER(merchant_category) AS merchant_category,
        LOWER(channel) AS channel,
        location,
        description,
        LOWER(status) AS status

    FROM source
    -- Filter out records missing a primary identifier
    WHERE transaction_id IS NOT NULL
)

SELECT * FROM staged