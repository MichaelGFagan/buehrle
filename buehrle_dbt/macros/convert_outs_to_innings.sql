{%- macro convert_outs_to_innings(outs_column) -%}
trunc({{ outs_column }} / 3)::text || '.' || mod({{ outs_column }}, 3)::text
{%- endmacro -%}