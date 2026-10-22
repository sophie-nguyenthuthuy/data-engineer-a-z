{% macro mask_pii(column) -%}
    {#- Giữ 3 số cuối để support đối soát, phần còn lại thay bằng * -#}
    concat(repeat('*', greatest(length({{ column }}) - 3, 0)), right({{ column }}, 3))
{%- endmacro %}
