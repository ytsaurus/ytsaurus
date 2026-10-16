PY3_LIBRARY()

# The shared monitoring config, embedded so the integration-test monitoring stack
# (library/python/integration_test_base/monitoring_stack.py) can materialize it in-sandbox. The
# grafana dashboards are NOT bundled -- they are rendered by generate.sh and supplied at test time
# via --test-param MONITORING_DASHBOARDS_DIR.
RESOURCE_FILES(
    PREFIX yt/yt/flow/docker/monitoring/
    aggr_rules.py
    docker-compose.yml
    grafana/dashboards/generate.sh
    grafana/provisioning/dashboards/dashboards.yml
    grafana/provisioning/datasources/datasource.yml
    prometheus.yml
)

END()
