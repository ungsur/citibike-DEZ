{#
This macro returns the description of the gender
#}

{% macro get_gender_type_description(gender) -%}

    CASE {{ gender }}
        WHEN 0 THEN 'Unknown'
        WHEN 1 THEN 'Male'
        WHEN 2 THEN 'Female'
    END

{%- endmacro %}