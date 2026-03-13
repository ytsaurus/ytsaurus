import json

import yaml
import yatest
import os
import hashlib

from library.python import resource
from yt.admin.ytsaurus_ci import compatibility_graph
from yt.admin.ytsaurus_ci import component_registry
from yt.admin.ytsaurus_ci import consts


def test_valid_dependencies_graph(tmpdir):
    registry = component_registry.VersionComponentRegistry(yaml.safe_load(
        resource.resfs_read(consts.COMPONENTS_PATH)))

    graph = compatibility_graph.CompatibilityGraph(registry)

    paths = graph.find_all_test_suites()

    f = tmpdir.join("dependencies_graph.json")
    f.ensure(dir=False)
    result = {}
    for idx, path in enumerate(paths):
        result[idx] = path

    f.write(json.dumps(result, indent=4, ensure_ascii=False, sort_keys=True))

    return yatest.common.canonical_file(str(f), local=True)


def test_updated_docs():
    registry = component_registry.VersionComponentRegistry(yaml.safe_load(
        resource.resfs_read(consts.COMPONENTS_PATH)))

    component = compatibility_graph.PIVOT_COMPONENT
    expected_snapshot = resource.resfs_read(
        os.path.join(consts.SNAPSHOTS_PATH, component)).decode("utf-8")
    md = compatibility_graph.format_compat_table(registry)
    new_snapshot = hashlib.sha512(md.encode()).hexdigest()
    error_msg = "Please, update documentation via ./yt/admin/ytsaurus_ci/ytsaurus_ci matrix docs --output $(ARCADIA_ROOT)/yt/docs/(en,ru)/_includes/compatibility"

    assert expected_snapshot == new_snapshot, error_msg
