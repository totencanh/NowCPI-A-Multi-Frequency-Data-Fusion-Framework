{% macro drop_legacy_gold_tables() %}
    {% set legacy_tables = [
        'dim_category',
        'dim_date',
        'dim_frequency',
        'dim_geography',
        'dim_series',
        'dim_unit',
        'fact_cpi_monthly',
        'fact_economic_observation',
        'fact_market_monthly',
        'olap_annual_economic',
        'olap_monthly_macro'
    ] %}

    {% for table_name in legacy_tables %}
        {% do run_query(
            'drop table if exists '
            ~ adapter.quote(target.database)
            ~ '.' ~ adapter.quote('gold')
            ~ '.' ~ adapter.quote(table_name)
        ) %}
    {% endfor %}
{% endmacro %}
