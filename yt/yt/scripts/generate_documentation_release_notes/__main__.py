from datetime import datetime
import argparse
import os
import re
import requests

REPO_OWNER = "ytsaurus"
OUTPUT_FILE = "release_notes.md"

YTSAURUS_SERVER_DESCRIPTION = """
All main components are released as docker images.
"""

JAVA_SDK_DESCRIPTION = """
Is released as packages in [Maven Central](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client).
"""

QUERY_TRACKER_DESCRIPTION = """
Is released as a docker image.
"""

CHYT_DESCRIPTION = """
Is released as a docker image.
"""

K8S_DESCRIPTION = """
Is released as helm charts on [GitHub Packages](https://github.com/ytsaurus/ytsaurus-k8s-operator/pkgs/container/ytop-chart).
"""

ODIN_DESCRIPTION = """
Is released as a docker image. Installation details are available in the [Odin installation guide](https://ytsaurus.tech/docs/ru/admin-guide/install-odin).
"""

PYTHON_SDK_DESCRIPTION = """
Available as a package in [PyPI](https://pypi.org/project/ytsaurus-client/).
"""

PYTHON_YSON_DESCRIPTION = """
Available as a package in [PyPI](https://pypi.org/project/ytsaurus-yson/). Release history is available on [PyPI](https://pypi.org/project/ytsaurus-yson/#history).
"""

STRAWBERRY_DESCRIPTION = """
Is released as a docker image.
"""

SPYT_DESCRIPTION = """
Is released as a docker image.
"""

UI_DESCRIPTION = """
Is released as a docker image.
"""

GENERIC_COMPONENT_DESCRIPTION = """
Release notes for this component.
"""

repo_name_to_releases = {}
package_page_to_html = {}


class Artifact:
    def __init__(self, label, kind, page_url=None, version_url_template=None):
        self.label = label
        self.kind = kind
        self.page_url = page_url
        self.version_url_template = version_url_template

    def get_version_url(self, version):
        if self.version_url_template:
            return self.version_url_template.format(version=version)
        return self.page_url


class DockerArtifact(Artifact):
    def __init__(self, label, repo_name, image_repo, tag_templates, version_transform=None, package_repo_name=None):
        super().__init__(label=label, kind="docker")
        self.repo_name = repo_name
        self.image_repo = image_repo
        self.tag_templates = tag_templates or []
        self.version_transform = version_transform or (lambda version: [version])
        self.package_repo_name = package_repo_name or repo_name

    def get_info(self, component, version):
        package_page_url = f"https://github.com/{REPO_OWNER}/{self.package_repo_name}/pkgs/container/{self.image_repo}"
        package_versions_url = f"{package_page_url}/versions?filters%5Bversion_type%5D=tagged"
        html = component._get_package_page_html(package_versions_url)

        for transformed_version in self.version_transform(version):
            for tag_template in self.tag_templates:
                docker_tag = tag_template.format(version=transformed_version)
                artifact_name = f"ghcr.io/{REPO_OWNER}/{self.image_repo}:{docker_tag}"

                if html:
                    pattern = rf'href="([^\"]*\?tag={re.escape(docker_tag)})"'
                    match = re.search(pattern, html)
                    if match:
                        artifact_url = f"https://github.com{match.group(1)}"
                        return artifact_name, artifact_url

        return None


class GithubPackageArtifact(Artifact):
    def __init__(self, label, repo_name, package_name, package_kind="container", version_templates=None, version_transform=None, display_name_template=None):
        super().__init__(label=label, kind="github-package")
        self.repo_name = repo_name
        self.package_name = package_name
        self.package_kind = package_kind
        self.version_templates = version_templates or ["{version}"]
        self.version_transform = version_transform or (lambda version: [version])
        self.display_name_template = display_name_template or "{version}"

    def get_info(self, component, version):
        package_page_url = f"https://github.com/{REPO_OWNER}/{self.repo_name}/pkgs/{self.package_kind}/{self.package_name}"
        package_versions_url = f"{package_page_url}/versions?filters%5Bversion_type%5D=tagged"
        html = component._get_package_page_html(package_versions_url)

        for transformed_version in self.version_transform(version):
            for version_template in self.version_templates:
                package_version = version_template.format(version=transformed_version)
                if html:
                    pattern = rf'href="([^\"]*{re.escape(self.package_name)}[^\"]*\?tag={re.escape(package_version)})"'
                    match = re.search(pattern, html)
                    if match:
                        package_url = f"https://github.com{match.group(1)}"
                        display_name = self.display_name_template.format(version=package_version, release_version=version)
                        return display_name, package_url

        return None


class Component:
    def __init__(self, repo_name, component_name, tag_name, description, filename, release_tag_splitter="/", artifacts=None, fallback_releases=None):
        self.repo_name = repo_name
        self.component_name = component_name
        self.tag_name = tag_name
        self.description = description
        self.filename = filename
        self.release_tag_splitter = release_tag_splitter
        self.artifacts = artifacts or []
        self.fallback_releases = fallback_releases or []

    def _get_releases_page(self, per_page=30, page=1):
        url = f"https://api.github.com/repos/{REPO_OWNER}/{self.repo_name}/releases"
        params = {
            'per_page': per_page,
            'page': page,
        }
        headers = {}

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            releases = response.json()
            return releases
        else:
            print(f"Failed to fetch releases: {response.status_code}")
            print(response.text)
        return None

    def _get_all_releases(self):
        if self.repo_name in repo_name_to_releases:
            return repo_name_to_releases[self.repo_name]

        releases = []
        page = 1

        while True:
            batch = self._get_releases_page(per_page=30, page=page)
            if not batch:
                break
            releases.extend(batch)
            page += 1

        repo_name_to_releases[self.repo_name] = releases
        return releases

    def _get_releases(self):
        all_releases = self._get_all_releases()
        if not self.tag_name:
            return all_releases
        return [release for release in all_releases if release["tag_name"].startswith(self.tag_name)]

    def _get_package_page_html(self, package_page_url):
        if package_page_url in package_page_to_html:
            return package_page_to_html[package_page_url]

        response = requests.get(package_page_url)
        if response.status_code == 200:
            package_page_to_html[package_page_url] = response.text
            return response.text

        return None

    def _get_artifact_lines(self, version):
        lines = []

        for artifact in self.artifacts:
            if artifact.kind in {"docker", "github-package"}:
                artifact_info = artifact.get_info(self, version)
                if artifact_info:
                    artifact_name, artifact_url = artifact_info
                    lines.append(f"**{artifact.label}:** [{artifact_name}]({artifact_url})")
                    lines.append("\n")
            else:
                artifact_url = artifact.get_version_url(version)
                if artifact_url:
                    lines.append(f"**{artifact.label}:** [{version}]({artifact_url})")
                    lines.append("\n")

        return lines

    def _generate_markdown(self):
        lines = [f"## {self.component_name}\n"]
        lines += [self.description, "\n\n"]

        lines += ["**Releases:**\n"]

        releases = self._get_releases()
        if not releases and self.fallback_releases:
            releases = self.fallback_releases

        for release in releases:
            version = release["tag_name"].split(self.release_tag_splitter)[-1] if self.release_tag_splitter else release["tag_name"]
            lines.append(f"{{% cut \"**{version}**\" %}}\n")

            parsed_date = datetime.strptime(release["created_at"], "%Y-%m-%dT%H:%M:%SZ")
            lines.append(f"**Release date:** {parsed_date.strftime("%Y-%m-%d")}")
            lines.append("\n")

            release_url = release.get("html_url")
            if release_url:
                lines.append(f"**Release page:** [{version}]({release_url})")
                lines.append("\n")

            lines.extend(self._get_artifact_lines(version))

            description = release["body"]
            description = re.sub(r"^####\s+(.*)", r"##### \1", description, flags=re.MULTILINE)
            description = re.sub(r"^#{1,3}\s+(.*)", r"#### \1", description, flags=re.MULTILINE)
            description = description.replace("\r\n", "\n")
            description = description.replace("\r", "\n")

            lines.append(description or "No description")
            lines.append("\n{% endcut %}\n\n")
        return "\n".join(lines)

    def dump_release_notes(self, output_paths):
        content = self._generate_markdown()
        for output_path in output_paths:
            os.makedirs(output_path, exist_ok=True)
            with open(f"{output_path}/{self.filename}", "w", encoding="utf-8") as f:
                f.write(content)


COMPONENTS = [
    Component(
        repo_name="ytsaurus",
        component_name="YTsaurus server",
        tag_name="docker/ytsaurus",
        description=YTSAURUS_SERVER_DESCRIPTION,
        filename="yt-server.md",
        artifacts=[
            DockerArtifact(label="Docker image", repo_name="ytsaurus", image_repo="ytsaurus", tag_templates=["stable-{version}", "{version}"]),
        ],
    ),
    Component(
        repo_name="ytsaurus",
        component_name="Query tracker",
        tag_name="docker/query-tracker",
        description=QUERY_TRACKER_DESCRIPTION,
        filename="query-tracker.md",
        artifacts=[
            DockerArtifact(label="Docker image", repo_name="ytsaurus", image_repo="query-tracker", tag_templates=["stable-{version}", "{version}"]),
        ],
    ),
    Component(
        repo_name="ytsaurus",
        component_name="Java SDK",
        tag_name="java-sdk",
        description=JAVA_SDK_DESCRIPTION,
        filename="java-sdk.md",
        artifacts=[
            Artifact(
                label="Maven Central",
                kind="package",
                page_url="https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/versions",
                version_url_template="https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/{version}",
            ),
        ],
    ),
    Component(
        repo_name="ytsaurus",
        component_name="CHYT",
        tag_name="chyt",
        description=CHYT_DESCRIPTION,
        filename="chyt.md",
        artifacts=[
            DockerArtifact(label="Docker image", repo_name="ytsaurus", image_repo="chyt", tag_templates=["stable-{version}", "{version}"]),
        ],
    ),
    Component(
        repo_name="ytsaurus-k8s-operator",
        component_name="Kubernetes operator",
        tag_name="",
        description=K8S_DESCRIPTION,
        filename="k8s.md",
        artifacts=[
            GithubPackageArtifact(label="Helm chart", repo_name="ytsaurus-k8s-operator", package_name="ytop-chart", version_transform=lambda version: [version, version.removeprefix("v")]),
        ],
    ),
    Component(repo_name="ytsaurus", component_name="Python YSON bindings", tag_name="python/ytsaurus-yson", description=PYTHON_YSON_DESCRIPTION, filename="python-yson.md", artifacts=[
        Artifact(label="PyPI package", kind="package", page_url="https://pypi.org/project/ytsaurus-yson/", version_url_template="https://pypi.org/project/ytsaurus-yson/{version}/"),
    ]),
    Component(
        repo_name="ytsaurus",
        component_name="Python SDK",
        tag_name="python/ytsaurus-client",
        description=PYTHON_SDK_DESCRIPTION,
        filename="python-sdk.md",
        artifacts=[
            Artifact(label="PyPI package", kind="package", page_url="https://pypi.org/project/ytsaurus-client/", version_url_template="https://pypi.org/project/ytsaurus-client/{version}/"),
        ],
    ),
    Component(
        repo_name="ytsaurus-spyt",
        component_name="SPYT",
        tag_name="spyt",
        description=SPYT_DESCRIPTION,
        filename="spyt.md",
        artifacts=[
            DockerArtifact(label="Docker image", repo_name="ytsaurus-spyt", image_repo="spyt", tag_templates=["{version}"]),
        ],
    ),
    Component(
        repo_name="ytsaurus",
        component_name="Strawberry",
        tag_name="yt/chyt/controller",
        description=STRAWBERRY_DESCRIPTION,
        filename="strawberry.md",
        artifacts=[
            DockerArtifact(
                label="Docker image",
                repo_name="ytsaurus",
                image_repo="strawberry",
                tag_templates=["{version}", "stable-{version}"],
                version_transform=lambda version: [version, version.removeprefix("v")],
            ),
        ],
    ),
    Component(
        repo_name="ytsaurus-ui",
        component_name="UI",
        tag_name="ui",
        description=UI_DESCRIPTION,
        filename="ui.md",
        release_tag_splitter="ui-v",
        artifacts=[
            DockerArtifact(label="Docker image", repo_name="ytsaurus-ui", image_repo="ui", tag_templates=["{version}", "stable-{version}"]),
        ],
    ),
    Component(repo_name="ytsaurus", component_name="Cron", tag_name="docker/cron", description=GENERIC_COMPONENT_DESCRIPTION, filename="cron.md", artifacts=[
        GithubPackageArtifact(label="Helm chart", repo_name="ytsaurus", package_name="cron-chart"),
    ]),
    Component(
        repo_name="ytsaurus",
        component_name="Odin",
        tag_name="docker/odin",
        description=ODIN_DESCRIPTION,
        filename="odin.md",
        artifacts=[
            GithubPackageArtifact(label="Docker image", repo_name="ytsaurus", package_name="odin-chart"),
        ],
    ),
    Component(
        repo_name="ytsaurus-excel-integration",
        component_name="Excel-integration",
        tag_name="docker/excel",
        description=GENERIC_COMPONENT_DESCRIPTION,
        filename="excel-integration.md",
        artifacts=[
            GithubPackageArtifact(
                label="Helm chart",
                repo_name="ytsaurus-excel-integration",
                package_name="ytsaurus-excel-chart",
            ),
        ],
    ),
    Component(
        repo_name="ytsaurus",
        component_name="Tutorial",
        tag_name="docker/tutorial",
        description=GENERIC_COMPONENT_DESCRIPTION,
        filename="tutorial.md",
        artifacts=[
            Artifact(
                label="Docker image",
                kind="package",
                page_url="https://github.com/orgs/ytsaurus/packages/container/tutorial-chart/784041843?tag=0.0.4",
                version_url_template="https://github.com/orgs/ytsaurus/packages/container/tutorial-chart/784041843?tag={version}",
            ),
        ],
        fallback_releases=[
            {
                "tag_name": "docker/tutorial/0.0.4",
                "created_at": "2026-04-16T00:00:00Z",
                "html_url": "https://github.com/ytsaurus/ytsaurus/releases/tag/docker%2Ftutorial%2F0.0.4",
                "body": "Tutorial chart release 0.0.4.",
            },
            {
                "tag_name": "docker/tutorial/0.0.3",
                "created_at": "2026-02-13T00:00:00Z",
                "html_url": "https://github.com/ytsaurus/ytsaurus/releases/tag/docker%2Ftutorial%2F0.0.3",
                "body": "Tutorial chart release 0.0.3.",
            },
            {
                "tag_name": "docker/tutorial/0.0.2",
                "created_at": "2025-12-19T00:00:00Z",
                "html_url": "https://github.com/ytsaurus/ytsaurus/releases/tag/docker%2Ftutorial%2F0.0.2",
                "body": "Tutorial chart release 0.0.2.",
            },
        ],
    ),
    Component(repo_name="ytsaurus-task-proxy", component_name="Task-proxy", tag_name="release/", description=GENERIC_COMPONENT_DESCRIPTION, filename="task-proxy.md", artifacts=[
        GithubPackageArtifact(label="Helm chart", repo_name="ytsaurus-task-proxy", package_name="task-proxy-chart"),
    ]),
]


def main():
    parser = argparse.ArgumentParser(
        description="Generate release notes"
    )

    parser.add_argument(
        "-o", "--output-path",
        action="append",
        help="Path to the output directory."
    )

    args = parser.parse_args()

    try:
        for c in COMPONENTS:
            c.dump_release_notes(args.output_path)
    except Exception as e:
        print(f"Error occured: {e}")


if __name__ == "__main__":
    main()
