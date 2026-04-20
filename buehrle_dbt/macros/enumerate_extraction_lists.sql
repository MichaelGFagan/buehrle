{%- macro enumerate_extraction_lists(column_dicts) -%}
    {%- set column_dicts = column_dicts | sum(start=[]) -%}
    {%- for column_dict in column_dicts %}
            '{{ column_dict['raw_name'] }}',
    {%- endfor -%}

{%- endmacro -%}