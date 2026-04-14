import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from library.python import resource  # noqa
from yt.admin.ytsaurus_ci import component_registry  # noqa
from yt.admin.ytsaurus_ci import ghcr  # noqa
from yt.admin.ytsaurus_ci import scenario_processor  # noqa


def test_components(ghcr_client):
    release_component = scenario_processor.ClusterComponent(
        name="ytsaurus",
        source=component_registry.Source(
            repo="ytsaurus",
            container="ytsaurus-nightly",
            image_tag=r"^dev-(?P<version>{{ version }})-\d{4}-\d{2}-\d{2}-(?P<commit_hash>\w+)-relwithdebinfo$",
        ),
        version="25.3",
        ghcr_client=ghcr_client(),
    )
    assert release_component.image == "ghcr.io/ytsaurus/ytsaurus-nightly:dev-25.3-2025-12-04-e6756828dc868ad2f8e643e65326b33d997796d5-relwithdebinfo"
    assert release_component.to_dict() == {
        "branch": None,
        "revision": "e6756828dc868ad2f8e643e65326b33d997796d5",
        "commitDate": "2025-12-07 00:00:00",
        "name": "YTSAURUS",
        "version": "25.3",
    }

    nightly_component = scenario_processor.ClusterComponent(
        name="ytsaurus",
        source=component_registry.Source(
            repo="ytsaurus",
            container="ytsaurus-nightly",
            image_tag=r"^dev-\d{4}-\d{2}-\d{2}-(?P<commit_hash>\w+)-relwithdebinfo$",
        ),
        version="",
        ghcr_client=ghcr_client(),
    )
    assert nightly_component.image == "ghcr.io/ytsaurus/ytsaurus-nightly:dev-2025-10-29-lastcommit-relwithdebinfo"
    assert nightly_component.to_dict() == {
        "branch": None,
        "revision": "lastcommit",
        "commitDate": "2025-10-29 12:24:45",
        "name": "YTSAURUS",
        "version": "dev-2025-10-29-lastcommit-relwithdebinfo",
    }

    component_operator = scenario_processor.OperatorComponent(
        source=component_registry.Source(
            repo="ytsaurus",
            container="ytop-chart",
            image_tag="{{ version }}",
        ),
        version="0.27.0",
        ghcr_client=ghcr_client(),
    )
    assert component_operator.image == "ghcr.io/ytsaurus/ytop-chart"
    assert component_operator.to_dict() == {
        "helmUrl": "ghcr.io/ytsaurus/ytop-chart",
        "operator": {
            "branch": None,
            "revision": None,
            "commitDate": None,
            "name": "OPERATOR",
            "version": "0.27.0",
        }
    }

    nightly_component_operator = scenario_processor.OperatorComponent(
        source=component_registry.Source(
            repo="ytsaurus",
            container="ytop-chart-nightly",
            image_tag=r"^[\d\.]+-dev-(?P<commit_hash>\w+)$",
        ),
        ghcr_client=ghcr_client(),
        version="",
    )
    assert nightly_component_operator.image == "ghcr.io/ytsaurus/ytop-chart-nightly"
    assert nightly_component_operator.to_dict() == {
        "helmUrl": "ghcr.io/ytsaurus/ytop-chart-nightly",
        "operator": {
            "branch": None,
            "revision": "81deb0a1e700bcc38d8fb1653b9577515a370d97",
            "commitDate": "2025-12-01 12:00:00",
            "name": "OPERATOR",
            "version": "0.0.666-dev-81deb0a1e700bcc38d8fb1653b9577515a370d97",
        }
    }


def test_process_scenario_simple(configs, templates, ghcr_client):
    with patch.object(scenario_processor, "TEMPLATES_PATH", templates), \
            patch.object(resource, "resfs_read") as mock_resfs, \
            patch.object(ghcr, "GitHubPackagesClient", ghcr_client):

        mock_resfs.side_effect = configs
        scenarios = scenario_processor.ProcessScenario("nightly-dev", MagicMock())
        assert len(scenarios) == 1
        result = scenarios[0].to_dict()
        assert isinstance(result, dict)
        assert result["scenario"] == "nightly-dev/simple"
        assert result["ttl"] == 600
        assert "k8sSpec" in result and isinstance(result["k8sSpec"], str)

        assert "operator" in result
        operator = result["operator"]
        assert operator["helmUrl"] == "ghcr.io/ytsaurus/ytop-chart-nightly"
        assert operator["operator"]["version"] == "0.0.666-dev-81deb0a1e700bcc38d8fb1653b9577515a370d97"
        assert operator["operator"]["revision"] == "81deb0a1e700bcc38d8fb1653b9577515a370d97"

        assert "components" in result and len(result["components"]) == 1

        checks = result["checks"]
        assert len(checks) == 1
        assert checks[0] == {
            "type": "ODIN",
            "description": {
                "helmUrl": "oci://ghcr.io/ytsaurus/odin-chart",
                "releaseName": "odin",
                "version": "0.0.4",
            }
        }

        spec = result["k8sSpec"]
        assert "name: ytsaurus-ci" in spec
        assert "ytsaurus: ghcr.io/ytsaurus/ytsaurus-nightly:dev-2025-10-29-lastcommit-relwithdebinfo" in spec


def test_scenario_compat_simple(configs, templates, ghcr_client):
    with patch.object(scenario_processor, "TEMPLATES_PATH", templates), \
            patch.object(resource, "resfs_read") as mock_resfs, \
            patch.object(ghcr, "GitHubPackagesClient", ghcr_client):

        mock_resfs.side_effect = configs
        tasks = scenario_processor.ProcessScenario("scenario-compat-first", MagicMock())
        assert len(tasks) == 1

        task = tasks[0].to_dict()
        assert task["operator"] == {
            "helmUrl": "ghcr.io/ytsaurus/ytop-chart-release",
            "operator": {
                "commitDate": None,
                "name": "OPERATOR",
                "revision": None,
                "version": "0.25.0",
                "branch": None
            }
        }

        assert len(task["components"]) == 1
        component = task["components"][0]
        assert component == {
            "name": "YTSAURUS",
            "version": "25.2",
            "branch": None,
            "commitDate": None,
            "revision": None,
        }

        assert task["scenario"] == "scenario-compat-first/compat"


def test_scenario_compat_empty(configs, templates, ghcr_client):
    # simple test, nothing components are compatible
    with patch.object(scenario_processor, "TEMPLATES_PATH", templates), \
            patch.object(resource, "resfs_read") as mock_resfs, \
            patch.object(ghcr, "GitHubPackagesClient", ghcr_client):

        mock_resfs.side_effect = configs
        tasks = scenario_processor.ProcessScenario("scenario-compat-empty", MagicMock())
        assert len(tasks) == 0


def test_scenario_compat_multi(configs, templates, ghcr_client):
    # operator:0.25.0 and ytsaurus:25.2
    # operator:0.26.0 and ytsaurus:25.2
    # operator:0.26.0 and ytsaurus:25.3
    with patch.object(scenario_processor, "TEMPLATES_PATH", templates), \
            patch.object(resource, "resfs_read") as mock_resfs, \
            patch.object(ghcr, "GitHubPackagesClient", ghcr_client):

        mock_resfs.side_effect = configs
        tasks = scenario_processor.ProcessScenario("scenario-compat-multi", MagicMock())
        assert len(tasks) == 3


def test_scenario_compat_ytsaurus(configs, templates, ghcr_client):
    # simple test, base filter
    with patch.object(scenario_processor, "TEMPLATES_PATH", templates), \
            patch.object(resource, "resfs_read") as mock_resfs, \
            patch.object(ghcr, "GitHubPackagesClient", ghcr_client):

        mock_resfs.side_effect = configs
        tasks = scenario_processor.ProcessScenario("scenario-compat-filter-ytsaurus", MagicMock())
        assert len(tasks) == 1

        task = tasks[0].to_dict()
        assert task["operator"] == {
            "helmUrl": "ghcr.io/ytsaurus/ytop-chart-release",
            "operator": {
                "commitDate": None,
                "name": "OPERATOR",
                "revision": None,
                "version": "0.26.0",
                "branch": None
            }
        }

        assert len(task["components"]) == 1
        assert task["components"][0] == {
            "name": "YTSAURUS",
            "version": "25.3",
            "branch": None,
            "commitDate": None,
            "revision": None,
        }


def test_scenario_compat_with_main(configs, templates, ghcr_client):
    # simple test, ytsaurus and operator from main
    with patch.object(scenario_processor, "TEMPLATES_PATH", templates), \
            patch.object(resource, "resfs_read") as mock_resfs, \
            patch.object(ghcr, "GitHubPackagesClient", ghcr_client):
        operator_expected = {
            "helmUrl": "ghcr.io/ytsaurus/ytop-chart-nightly",
            "operator": {
                "branch": None,
                "revision": "81deb0a1e700bcc38d8fb1653b9577515a370d97",
                "commitDate": "2025-12-01 12:00:00",
                "name": "OPERATOR",
                "version": "0.0.666-dev-81deb0a1e700bcc38d8fb1653b9577515a370d97",
            },
        }
        component_expected = {
            "branch": None,
            "revision": "lastcommit",
            "commitDate": "2025-10-29 12:24:45",
            "name": "YTSAURUS",
            "version": "dev-2025-10-29-lastcommit-relwithdebinfo",
        }

        mock_resfs.side_effect = configs
        tasks = scenario_processor.ProcessScenario("scenario-compat-main", MagicMock())
        assert len(tasks) == 1
        task = tasks[0].to_dict()
        assert task["operator"] == operator_expected

        assert len(task["components"]) == 1
        assert task["components"][0] == component_expected


def test_scenario_compat_with_both_filters(configs, templates, ghcr_client):
    # ytsaurus:23.1 works with operator:0.0.1 and operator:main
    # but we reduce the set with operator:version_filter=main
    with patch.object(scenario_processor, "TEMPLATES_PATH", templates), \
            patch.object(resource, "resfs_read") as mock_resfs, \
            patch.object(ghcr, "GitHubPackagesClient", ghcr_client):

        mock_resfs.side_effect = configs
        tasks = scenario_processor.ProcessScenario("scenario-compat-both-filters", MagicMock())
        assert len(tasks) == 1
        task = tasks[0].to_dict()
        assert task["operator"] == {
            "helmUrl": "ghcr.io/ytsaurus/ytop-chart-nightly",
            "operator": {
                "branch": None,
                "commitDate": "2025-12-01 12:00:00",
                "name": "OPERATOR",
                "revision": "81deb0a1e700bcc38d8fb1653b9577515a370d97",
                "version": "0.0.666-dev-81deb0a1e700bcc38d8fb1653b9577515a370d97",
            }
        }

        assert len(task["components"]) == 1
        assert task["components"][0] == {
            "name": "YTSAURUS",
            "branch": None,
            "version": "23.1",
            "revision": None,
            "commitDate": None,
        }


def test_scenario_compat_difficult_constraint(configs, templates, ghcr_client):
    # ytsaurus:23.1 works with operator:0.0.1 and operator:main
    with patch.object(scenario_processor, "TEMPLATES_PATH", templates), \
            patch.object(resource, "resfs_read") as mock_resfs, \
            patch.object(ghcr, "GitHubPackagesClient", ghcr_client):

        mock_resfs.side_effect = configs
        tasks = scenario_processor.ProcessScenario("scenario-compat-difficult-constraint", MagicMock())
        assert len(tasks) == 2
