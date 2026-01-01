{% macro delivery_distance_category(delivery_distance_meters_column) %}
    case
        when {{ delivery_distance_meters_column }} < 2000 then 'Very Close'
        when {{ delivery_distance_meters_column }} < 5000 then 'Close'
        when {{ delivery_distance_meters_column }} < 10000 then 'Medium'
        else 'Far'
    end
{% endmacro %}

