{{ config(materialized = 'table')}}

WITH

 base_economy AS (
    SELECT 
        _airbyte_economy_hashid,
        gdp_usd,
        location_key,
        gdp_per_capita_usd,
        human_capital_index 
        
    FROM {{ source('raw_covid19', 'economy') }}
)

SELECT 
    * 

FROM base_economy