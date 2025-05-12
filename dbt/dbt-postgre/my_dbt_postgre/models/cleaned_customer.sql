-- models/cleaned_customers.sql

{{ config(
    materialized='table'
) }}

with source as (
    select * from {{ source('raw', 'customers') }}
),

renamed as (
    select
        id as customer_id,
        name,
        email,
        created_at::date as signup_date
    from source
    where email is not null
)

select * from renamed
