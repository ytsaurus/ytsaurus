PY3_LIBRARY()

# Запросы отдаются кубикам как ресурсы: .sql остаётся единственным местом,
# где живёт логика, и его же можно открыть в YQL UI.
RESOURCE_FILES(
    PREFIX pod_size_actualization/yql/
    collect_bundle_usage_by_period.sql
)

END()
