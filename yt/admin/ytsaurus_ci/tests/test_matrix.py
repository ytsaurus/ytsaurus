import json

import yaml
import yatest

from library.python import resource
from yt.admin.ytsaurus_ci import compatibility_graph, consts, component_registry


def test_valid_dependencies_graph(tmpdir):
    registry = component_registry.VersionComponentRegistry(
        yaml.safe_load(resource.resfs_read(consts.COMPONENTS_PATH))
    )

    graph = compatibility_graph.CompatibilityGraph(registry)

    paths = graph.find_all_test_suites()

    f = tmpdir.join("dependencies_graph.json")
    f.ensure(dir=False)
    result = {}
    for idx, path in enumerate(paths):
        result[idx] = path

    f.write(json.dumps(result, indent=4, ensure_ascii=False, sort_keys=True))

    return yatest.common.canonical_file(str(f), local=True)
