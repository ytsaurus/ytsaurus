PY3_PROGRAM()
STYLE_PYTHON()

PY_SRCS(
    __init__.py
    base_client.py
    check_registry.py
    cloudfunction_client.py
    compatibility_graph.py
    component_registry.py
    components.py
    consts.py
    ghcr.py
    MAIN main.py
    models.py
    pretty.py
    enums.py
    registry_clients.py
    scenario_processor.py
    task.py
    yandex_cr.py
)

PEERDIR(
    contrib/python/click
    contrib/python/curlify
    contrib/python/Jinja2
    contrib/python/pytest
    contrib/python/PyYAML
    contrib/python/requests
    contrib/python/frozendict
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
    configs/upgrades.yaml
    snapshots/ytsaurus
    templates/base-spec.yaml
    templates/spec-with-update.yaml
    tests/configs/compat-operator.yaml
    tests/configs/compat-ytsaurus.yaml
    tests/configs/components.yaml
    tests/configs/scenarios.yaml
)

IF (NOT OPENSOURCE OR YT_CUSTOM_INTERNAL_BUILD)
    RESOURCE_FILES(
        configs/components-internal.yaml
        configs/compat-ytsaurus-custom-internal-build.yaml
        configs/compat-query_tracker-custom-internal-build.yaml
        templates/spec-for-custom-internal-build.yaml
    )
ENDIF()

END()

RECURSE(tests)
