import tempfile
from pathlib import Path

import pytest
from unittest.mock import MagicMock

from library.python import resource


class MockGitHubPackagesClient:
    def __init__(self, auth=MagicMock()):
        pass

    def get_commit_info(self, repo: str, commit_hash: str):
        if commit_hash == "lastcommit":
            return {
                "commit": {"author": {"date": "2025-10-29T12:24:45Z"}}
            }
        elif commit_hash == "81deb0a1e700bcc38d8fb1653b9577515a370d97":
            return {
                "commit": {"author": {"date": "2025-12-01T12:00:00Z"}}
            }
        elif commit_hash == "e6756828dc868ad2f8e643e65326b33d997796d5":
            return {
                "commit": {"author": {"date": "2025-12-07T00:00:00Z"}}
            }

    def get_package_versions(self, package_name):
        storage = {
            "ytop-chart": [
                {"metadata": {"container": {"tags": ["0.27.0"]}}},
            ],
            "ytop-chart-nightly": [
                {"metadata": {"container": {"tags": ["0.0.666-dev-81deb0a1e700bcc38d8fb1653b9577515a370d97"]}}},
            ],
            "ytsaurus-nightly": [
                {"metadata": {"container": {"tags": ["dev-2025-10-29-lastcommit-relwithdebinfo"]}}},
                {"metadata": {"container": {"tags": ["dev-25.3-2025-12-04-e6756828dc868ad2f8e643e65326b33d997796d5-relwithdebinfo"]}}},
            ],
            "ytop-chart-release": [
                {"metadata": {"container": {"tags": ["0.0.1"]}}},
                {"metadata": {"container": {"tags": ["0.24.0"]}}},
                {"metadata": {"container": {"tags": ["0.25.0"]}}},
                {"metadata": {"container": {"tags": ["0.26.0"]}}},
            ],
            "ytsaurus-release": [
                {"metadata": {"container": {"tags": ["23.1"]}}},
                {"metadata": {"container": {"tags": ["24.1"]}}},
                {"metadata": {"container": {"tags": ["25.2"]}}},
                {"metadata": {"container": {"tags": ["25.3"]}}},
                {"metadata": {"container": {"tags": ["25.4"]}}},
            ],
            "special-container": [
                {"metadata": {"container": {"tags": ["0.0.1"]}}},
            ],
            "default": [
                {"metadata": {"container": {"tags": ["dev-2025-10-29-7c7787c3c12fe6c18fcea4ea3da34537278fab63"]}}},
            ]
        }

        return storage.get(package_name, storage.get("default"))


@pytest.fixture(scope="session")
def ghcr_client():
    return MockGitHubPackagesClient


@pytest.fixture(scope="session")
def configs():
    configs_content = {}
    configs = (
        "tests/configs/scenarios.yaml",
        "tests/configs/components.yaml",
        "tests/configs/compat-ytsaurus.yaml",
        "tests/configs/compat-operator.yaml",
    )

    for cfg in configs:
        configs_content[cfg] = resource.resfs_read(cfg)

    def wrap(filename):
        return configs_content["tests/" + filename]

    return wrap


@pytest.fixture(scope="session")
def templates():
    tmpdir = tempfile.mkdtemp()
    templates_dir = Path(tmpdir) / "templates"
    templates_dir.mkdir(parents=True)

    base_spec = templates_dir / "base-spec.yaml"
    base_spec.write_text(
        """
name: {{ cluster_name }}
ytsaurus: {{ images.ytsaurus }}
        """.strip()
    )

    return templates_dir
