{% test dbt_utils_free_positive(model, column_name) %}
    -- Generic test tự viết (không cần dbt_utils): cột phải > 0.
    select *
    from {{ model }}
    where {{ column_name }} <= 0
{% endtest %}
