import json

import pytest
import yatest
import os
import hashlib

from library.python import resource
from yt.admin.ytsaurus_ci import compatibility_graph
from yt.admin.ytsaurus_ci import component_registry
from yt.admin.ytsaurus_ci import consts


def _build_graph_and_find_suites(graph_name):
    registry = component_registry.VersionComponentRegistry(component_registry.load_components_config())
    components = set(registry.get_components_in_graph(graph_name))
    graph = compatibility_graph.CompatibilityGraph(registry, components=components)

    return graph.find_all_test_suites(components=components)


def _canonize_paths(tmpdir, filename, paths):
    f = tmpdir.join(filename)
    f.ensure(dir=False)
    result = {}
    for idx, path in enumerate(paths):
        result[idx] = path

    f.write(json.dumps(result, indent=4, ensure_ascii=False, sort_keys=True))

    return yatest.common.canonical_file(str(f), local=True)


def test_valid_dependencies_graph(tmpdir):
    paths = _build_graph_and_find_suites(component_registry.DEFAULT_GRAPH)
    return _canonize_paths(tmpdir, "dependencies_graph.json", paths)


def test_valid_dependencies_graph_custom_internal_build(tmpdir):
    if not component_registry.has_internal_components():
        pytest.skip("components-internal.yaml is not available")

    paths = _build_graph_and_find_suites("custom_internal_build")
    return _canonize_paths(tmpdir, "dependencies_graph_custom_internal_build.json", paths)


def test_updated_docs():
    registry = component_registry.VersionComponentRegistry(component_registry.load_components_config())

    component = compatibility_graph.PIVOT_COMPONENT
    expected_snapshot = resource.resfs_read(os.path.join(consts.SNAPSHOTS_PATH, component)).decode("utf-8")
    md = compatibility_graph.format_compat_table(registry)
    new_snapshot = hashlib.sha512(md.encode()).hexdigest()
    error_msg = "Please, update documentation via ./yt/admin/ytsaurus_ci/ytsaurus_ci matrix docs --output $(ARCADIA_ROOT)/yt/docs/(en,ru)/_includes/compatibility"

    assert expected_snapshot == new_snapshot, error_msg
