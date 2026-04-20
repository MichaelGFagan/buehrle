{% macro enumerate_extracted_columns(column_dicts) %}
    {%- set column_dicts = column_dicts | sum(start=[]) -%}
    {%- for column_dict in column_dicts -%}
    {%- if loop.first %}
        extracted_list[{{ loop.index }}]::{{ column_dict['type'] }} AS {{ column_dict['column_name'] }}
    {%- else %}
      , extracted_list[{{ loop.index }}]::{{ column_dict['type'] }} AS {{ column_dict['column_name'] }}
    {%- endif %}
    {%- endfor %}
{% endmacro %}