{{ config(
    materialized='view',
    tags=['staging', 'finance']
) }}

-- Define the raw source for the accounts data
-- Assumes your dbt project has a source named 'raw' which includes a table named 'accounts'
WITH source AS (
    SELECT * FROM {{ ref('accounts') }}
),

-- Apply basic cleaning and standardization
staged AS (
    SELECT
        -- Primary Identifiers
        account_id::VARCHAR AS account_id,
        customer_id::VARCHAR AS customer_id,
        branch_id::VARCHAR AS branch_id,

        -- Account Details & Standardization
        account_number,
        LOWER(account_type) AS account_type,
        LOWER(account_status) AS account_status,
        branch_name,
        currency,

        -- Financial Metrics (keeping precision)
        interest_rate,
        minimum_balance,
        current_balance,

        -- Date & Time Fields (Explicit Casting)
        -- DuckDB allows direct casting using the :: operator
        open_date::DATE AS open_date,
        -- Ensure close_date is handled properly (can be NULL)
        CASE
            WHEN close_date IS NOT NULL THEN close_date::DATE
            ELSE NULL
        END AS close_date,
        last_transaction_date::TIMESTAMP AS last_transaction_date

    FROM source
    -- Optional: Add a basic filter to exclude invalid records, e.g., accounts with no ID
    WHERE account_id IS NOT NULL
)

SELECT * FROM staged