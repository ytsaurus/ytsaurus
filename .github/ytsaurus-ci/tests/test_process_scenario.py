import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent))

import cfg_loader
import ghcr

def _mock_versions_func(package_name, **kwargs):
    if package_name == "ytop-chart-nightly":
        return [{"metadata": {"container": {"tags": ["0.0.666-dev-81deb0a1e700bcc38d8fb1653b9577515a370d97"]}}}]
    elif package_name == "ytsaurus-nightly":
        return [{"metadata": {"container": {"tags": ["dev-2025-10-29-957a6a4bd442995eac071a1fc4ca0ef28a468803-relwithdebinfo"]}}}]

    return [{"metadata": {"container": {"tags": ["dev-2025-10-29-7c7787c3c12fe6c18fcea4ea3da34537278fab63"]}}}]


def test_process_scenario_common(tmp_path):
    configs_dir = tmp_path / "configs"
    templates_dir = tmp_path / "templates"
    configs_dir.mkdir()
    templates_dir.mkdir()

    scenarios_yaml = configs_dir / "scenarios.yaml"
    scenarios_yaml.write_text(
        """
scenarios:
  nightly-dev:
    k8sTemplate: base-spec.yaml
    checks: [test-check]
    ttl: 600
    operator:
      package: ytop-chart-nightly
      version: dev
      repo: ytsaurus-k8s-operator
    components:
      ytsaurus:
        package: ytsaurus-nightly
        version: dev
      strawberry:
        package: strawberry-nightly
        version: dev
      query_tracker:
        package: query-tracker-nightly
        version: dev
checks:
  test-check:
    type: ODIN
    description:
      helmUrl: oci://ghcr.io/ytsaurus/odin-chart
      releaseName: odin
      version: 0.0.4
        """.strip(),
        encoding="utf-8",
    )

    base_spec = templates_dir / "base-spec.yaml"
    base_spec.write_text(
        """
name: {{ cluster_name }}
ytsaurus: {{ images.ytsaurus }}
strawberry: {{ images.strawberry }}
query_tracker: {{ images.query_tracker }}
        """.strip()
    )

    with patch.object(cfg_loader, "SCENARIOS_PATH", scenarios_yaml), \
         patch.object(cfg_loader, "TEMPLATES_PATH", templates_dir), \
         patch.object(ghcr.GitHubPackagesClient, "get_package_versions") as mock_versions, \
         patch.object(ghcr.GitHubPackagesClient, "get_commit_info") as mock_commit:

        mock_versions.side_effect = _mock_versions_func
        mock_commit.return_value = {"commit": {"author": {"date": "1545-12-13T00:00:00Z"}}}

        auth = ghcr.GitHubAuth(token="x", base_url="https://api.github.com")
        scenario = cfg_loader.ProcessScenario("nightly-dev", auth)

        result = scenario.to_json()
        assert isinstance(result, dict)
        assert result["ttl"] == 600
        assert "k8sSpec" in result and isinstance(result["k8sSpec"], str)
        assert "operator" in result
        operator = result["operator"]
        assert operator["helmUrl"] == "ghcr.io/ytsaurus/ytop-chart-nightly"
        assert operator["operator"]["version"] == "0.0.666-dev-81deb0a1e700bcc38d8fb1653b9577515a370d97"
        assert operator["operator"]["revision"] == "81deb0a1e700bcc38d8fb1653b9577515a370d97"
        assert "components" in result and len(result["components"]) == 3

        spec = result["k8sSpec"]
        assert "name: ytsaurus-ci" in spec
        assert "ytsaurus: ghcr.io/ytsaurus/ytsaurus-nightly:dev-2025-10-29-957a6a4bd442995eac071a1fc4ca0ef28a468803-relwithdebinfo" in spec
        assert "strawberry: ghcr.io/ytsaurus/strawberry-nightly:dev-2025-10-29-7c7787c3c12fe6c18fcea4ea3da34537278fab63" in spec
        assert "query_tracker: ghcr.io/ytsaurus/query-tracker-nightly:dev-2025-10-29-7c7787c3c12fe6c18fcea4ea3da34537278fab63" in spec
