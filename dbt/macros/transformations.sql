-- Custom macros for ODS ETL transformations
-- These macros standardize common transformations across models

{% macro clean_text_field(column_name) %}
    TRIM(REGEXP_REPLACE({{ column_name }}, '\r|\n|\x00', '', 'g'))
{% endmacro %}

{% macro standardize_date_field(column_name) %}
    CASE 
        WHEN {{ column_name }}::text LIKE '9999%' THEN 
            ('2261' || SUBSTRING({{ column_name }}::text FROM 5))::timestamp
        ELSE 
            {{ column_name }}::timestamp
    END
{% endmacro %}

{% macro handle_null_numeric(column_name, default_value=0) %}
    COALESCE({{ column_name }}, {{ default_value }})
{% endmacro %}

{% macro generate_surrogate_key(columns) %}
    {{ dbt_utils.generate_surrogate_key(columns) }}
{% endmacro %}

{% macro get_current_timestamp() %}
    CURRENT_TIMESTAMP
{% endmacro %}
