{{ config(
    materialized='view',
    schema='reporting'
) }}

SELECT
    `Date` AS date,
    `Salesperson` AS salesperson,
    `Lead Name` AS lead_name,
    `Segment` AS segment,
    `Region` AS region,
    `Target Close` AS target_close,
    `Forecasted Monthly Revenue` AS forecasted_monthly_revenue,
    `Opportunity Stage` AS opportunity_stage,
    `Weighted Revenue` AS weighted_revenue,
    `Closed Opportunity` AS closed_opportunity,
    `Active Opportunity` AS active_opportunity,
    `Latest Status Entry` AS latest_status_entry
FROM {{ source('bigquery_training', 'sales_data') }}
