PY3_PROGRAM()
STYLE_PYTHON()

PY_SRCS(
    __init__.py
    check_registry.py
    cloudfunction_client.py
    compatibility_graph.py
    component_registry.py
    consts.py
    ghcr.py
    MAIN main.py
    models.py
    scenario_processor.py
)

PEERDIR(
    contrib/python/click
    contrib/python/curlify
    contrib/python/Jinja2
    contrib/python/pytest
    contrib/python/PyYAML
    contrib/python/requests
    library/python/resource
)

RESOURCE_FILES(
    configs/compat-chyt.yaml
    configs/compat-operator.yaml
    configs/compat-query_tracker.yaml
    configs/compat-spyt.yaml
    configs/compat-strawberry.yaml
    configs/compat-ytsaurus.yaml
    configs/components.yaml
    configs/scenarios.yaml
    templates/base-spec.yaml
    tests/configs/compat-operator.yaml
    tests/configs/compat-ytsaurus.yaml
    tests/configs/components.yaml
    tests/configs/scenarios.yaml
)

END()

RECURSE(tests)
