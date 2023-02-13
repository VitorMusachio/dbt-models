{{ config(materialized = 'table')}}

-- Denormalizing table to daily
WITH
epidemiology AS (
    SELECT
        *
    
    FROM {{ ref('base_epidemiology')}}
),

demographics AS (
    SELECT
        *

    FROM {{ ref('base_demographics')}}
),

economy AS (
    SELECT
        *
    
    FROM {{ ref('base_economy')}}
    ),

index AS (
    SELECT
        *
    
    FROM {{ ref('base_index')}}
    ),

epidemiology_join AS (
    SELECT        
        epidemiology.date,
        epidemiology.location_key,
        IFF(epidemiology.new_confirmed = 'NaN', 0, epidemiology.new_confirmed) AS new_confirmed,
        IFF(epidemiology.new_deceased = 'NaN', 0, epidemiology.new_deceased) AS new_deceased,
        IFF(epidemiology.new_recovered = 'NaN', 0, epidemiology.new_recovered) AS new_recovered,
        IFF(epidemiology.new_tested = 'NaN', 0, epidemiology.new_tested) AS new_tested,
        IFF(epidemiology.cumulative_confirmed = 'NaN', 0, epidemiology.cumulative_confirmed) AS cumulative_confirmed,
        IFF(epidemiology.cumulative_deceased = 'NaN', 0, epidemiology.cumulative_deceased) AS cumulative_deceased,
        IFF(epidemiology.cumulative_recovered = 'NaN', 0, epidemiology.cumulative_recovered) AS cumulative_recovered,
        IFF(epidemiology.cumulative_tested = 'NaN', 0, epidemiology.cumulative_tested) AS cumulative_tested,
        economy.gdp_usd,
        economy.gdp_per_capita_usd,
        economy.human_capital_index,
        demographics.population,
        demographics.population_male,
        demographics.population_female,
        demographics.population_rural,
        demographics.population_urban,
        demographics.population_largest_city,
        demographics.population_clustered,
        demographics.population_density,
        demographics.human_development_index,
        demographics.population_age_00_09,
        demographics.population_age_10_19,
        demographics.population_age_20_29,
        demographics.population_age_30_39,
        demographics.population_age_40_49,
        demographics.population_age_50_59,
        demographics.population_age_60_69,
        demographics.population_age_70_79,
        demographics.population_age_80_and_older,
        index.place_id, 
        index.wikidata_id, 
        index.country_code,
        index.country_name,
        index.locality_code, 
        index.locality_name, 
        index.datacommons_id, 
        index.subregion1_code, 
        index.subregion1_name, 
        index.subregion2_code, 
        index.subregion2_name, 
        index.aggregation_level, 
        index.iso_3166_1_alpha_2,
        index.iso_3166_1_alpha_3

    FROM epidemiology
    
    LEFT JOIN demographics
         ON epidemiology.location_key = demographics.location_key

    LEFT JOIN economy
        ON epidemiology.location_key = economy.location_key

    LEFT JOIN index
        ON epidemiology.location_key = index.location_key
    )

SELECT 
    * 
    
FROM epidemiology_join