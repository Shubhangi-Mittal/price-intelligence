{#
  initcap() exists in BigQuery, Snowflake and Postgres but not in DuckDB, which
  is the local development target. This macro implements the same behaviour with
  functions every supported warehouse has, so the models build unchanged on both
  the local DuckDB target and the cloud target.
#}
{% macro initcap_compat(expr) %}
case
  when {{ expr }} is null then null
  when length(trim({{ expr }})) = 0 then null
  else upper(substr(trim({{ expr }}), 1, 1)) || lower(substr(trim({{ expr }}), 2))
end
{% endmacro %}
