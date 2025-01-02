{% macro safe_divide(dividend, divisor, decimal=0) %}

case
    when {{ divisor }} = 0 then round(0, {{decimal}})
    else round({{ dividend }} / {{ divisor }}, {{decimal}})
end

{% endmacro %}