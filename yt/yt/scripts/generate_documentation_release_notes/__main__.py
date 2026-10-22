from datetime import datetime
from html.parser import HTMLParser
import argparse
import os
import re
import requests
import time
from urllib.parse import parse_qs, urljoin, urlsplit

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

FLOW_DESCRIPTION = """
One release covers every Flow component: the server as docker images, plain and with a Java or a
Python runtime, the Java SDK in Maven Central, the Python SDK in PyPI and the Go SDK as a Go module,
all at the same version.
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
package_to_versions = {}


def get_url(url, **kwargs):
    for attempt in range(3):
        try:
            response = requests.get(url, timeout=(10, 30), **kwargs)
            if response.status_code not in {429, 500, 502, 503, 504} or attempt == 2:
                response.raise_for_status()
                return response
        except (requests.ConnectionError, requests.Timeout):
            if attempt == 2:
                raise
        time.sleep(0.5 * 2 ** attempt)


class PackagePageParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
        self.next_url = None

    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        attrs = dict(attrs)
        href = attrs.get("href")
        if not href:
            return
        self.links.append(href)
        if attrs.get("aria-disabled") == "true":
            return
        if "next" in (attrs.get("rel") or "").split() or "next_page" in (attrs.get("class") or "").split():
            self.next_url = href


def html_block_end(line):
    """Return the terminator of a raw HTML block starting on this line."""
    if re.match(r"^ {0,3}<!--", line):
        return re.compile(r"-->")
    if re.match(r"^ {0,3}<\?", line):
        return re.compile(r"\?>")
    if re.match(r"^ {0,3}<!\[CDATA\[", line):
        return re.compile(r"\]\]>")
    if re.match(r"^ {0,3}<![A-Z]", line):
        return re.compile(r">")
    tag = re.match(r"^ {0,3}<(pre|script|style|textarea)(?=[ \t>]|$)", line, flags=re.IGNORECASE)
    if tag:
        return re.compile(r"</" + tag[1] + r"[ \t]*>", flags=re.IGNORECASE)
    return None


def prepare_description(body, labels):
    """Extract complete metadata paragraphs in the introduction, preserving code and HTML."""
    lines = (body or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
    labels = {label.casefold(): label for label in labels}
    metadata = {}
    removed = set()
    protected = set()
    introduction = set()
    atx_headings = set()
    fence = None
    html_end = None
    in_introduction = True
    field_pattern = re.compile(r"^\*\*([^*]+?)(?::\*\*|\*\*:)[ \t]*(.*?)[ \t]*$")

    def field(line):
        match = field_pattern.fullmatch(line)
        if match and match[1].casefold() in labels:
            return labels[match[1].casefold()], match[2]
        return None

    for index, line in enumerate(lines):
        if html_end:
            protected.add(index)
            if html_end.search(line):
                html_end = None
            continue
        marker = re.match(r"^ {0,3}(`{3,}|~{3,})(.*)$", line)
        if fence:
            protected.add(index)
            if marker and marker[1][0] == fence[0] and len(marker[1]) >= len(fence) and not marker[2].strip():
                fence = None
            continue
        if marker:
            protected.add(index)
            fence = marker[1]
            continue
        html_end = html_block_end(line)
        if html_end:
            protected.add(index)
            if html_end.search(line):
                html_end = None
            continue
        is_setext_heading = re.fullmatch(r" {0,3}(?:=+|-+)[ \t]*", line) and index > 0 and lines[index - 1].strip()
        is_atx_heading = re.match(r"^ {0,3}#{1,6}(?:[ \t]+|$)", line)
        if is_atx_heading:
            atx_headings.add(index)
        if is_atx_heading or is_setext_heading:
            in_introduction = False
        if in_introduction:
            introduction.add(index)
        if re.match(r"^####[ \t]+", line):
            lines[index] = re.sub(r"^####[ \t]+", "##### ", line)
        else:
            lines[index] = re.sub(r"^#{1,3}[ \t]+(.*)", r"#### \1", line)

    paragraph = []
    for index in range(len(lines) + 1):
        if index < len(lines) and lines[index].strip() and index not in atx_headings:
            paragraph.append(index)
            continue
        if paragraph and all(i in introduction and i not in protected for i in paragraph):
            fields = [field(lines[i]) for i in paragraph]
            # A paragraph is metadata only if every line is a known field.
            # Never extract only part of a paragraph adjoining ordinary text.
            if all(fields):
                for label, value in fields:
                    if label in metadata and metadata[label] != value:
                        raise ValueError(f"Conflicting values for release metadata field {label!r}")
                    metadata[label] = value
                removed.update(paragraph)
                if index < len(lines) and not lines[index].strip():
                    removed.add(index)
        paragraph = []

    return metadata, "\n".join(line for index, line in enumerate(lines) if index not in removed)


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
        versions = component._get_package_versions("container", self.image_repo)

        for transformed_version in self.version_transform(version):
            for tag_template in self.tag_templates:
                docker_tag = tag_template.format(version=transformed_version)
                artifact_name = f"ghcr.io/{REPO_OWNER}/{self.image_repo}:{docker_tag}"

                if docker_tag in versions:
                    return artifact_name, versions[docker_tag]

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
        versions = component._get_package_versions(self.package_kind, self.package_name)

        for transformed_version in self.version_transform(version):
            for version_template in self.version_templates:
                package_version = version_template.format(version=transformed_version)
                if package_version in versions:
                    display_name = self.display_name_template.format(version=package_version, release_version=version)
                    return display_name, versions[package_version]

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

        response = get_url(url, headers=headers, params=params)
        releases = response.json()
        if not isinstance(releases, list):
            raise ValueError(f"Expected a list of releases from {url}")
        return releases

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

        response = get_url(package_page_url)
        package_page_to_html[package_page_url] = response.text
        return response.text

    def _get_package_versions(self, package_kind, package_name):
        key = package_kind, package_name
        if key in package_to_versions:
            return package_to_versions[key]

        package_path = f"/{package_kind}/{package_name}/"
        url = f"https://github.com/orgs/{REPO_OWNER}/packages{package_path}versions?filters%5Bversion_type%5D=tagged"
        versions = {}
        visited = set()
        while url:
            if url in visited:
                raise ValueError(f"Cyclic package pagination at {url}")
            visited.add(url)
            parser = PackagePageParser()
            parser.feed(self._get_package_page_html(url))
            for href in parser.links:
                version_url = urljoin(url, href)
                parsed = urlsplit(version_url)
                if parsed.scheme != "https" or parsed.netloc != "github.com":
                    continue
                if package_path not in parsed.path or not parsed.path.rsplit("/", 1)[-1].isdigit():
                    continue
                for tag in parse_qs(parsed.query).get("tag", []):
                    versions.setdefault(tag, version_url)
            url = urljoin(url, parser.next_url) if parser.next_url else None
            if url:
                parsed = urlsplit(url)
                if parsed.scheme != "https" or parsed.netloc != "github.com" or not parsed.path.endswith(package_path + "versions"):
                    raise ValueError(f"Unexpected package pagination URL: {url}")

        package_to_versions[key] = versions
        return versions

    def _get_artifact_lines(self, version, metadata=None):
        lines = []
        metadata = metadata or {}

        for artifact in self.artifacts:
            if artifact.label in metadata:
                lines.append(f"**{artifact.label}:** {metadata[artifact.label]}")
                lines.append("\n")
            elif artifact.kind in {"docker", "github-package"}:
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
            labels = ["Release date", "Release page"] + [artifact.label for artifact in self.artifacts]
            metadata, description = prepare_description(release.get("body"), labels)
            version = release["tag_name"].split(self.release_tag_splitter)[-1] if self.release_tag_splitter else release["tag_name"]
            lines.append(f"{{% cut \"**{version}**\" %}}\n")

            parsed_date = datetime.strptime(release["created_at"], "%Y-%m-%dT%H:%M:%SZ")
            lines.append(f"**Release date:** {parsed_date.strftime('%Y-%m-%d')}")
            lines.append("\n")

            release_url = release.get("html_url")
            if release_url:
                lines.append(f"**Release page:** [{version}]({release_url})")
                lines.append("\n")
            elif "Release page" in metadata:
                lines.append(f"**Release page:** {metadata['Release page']}")
                lines.append("\n")

            lines.extend(self._get_artifact_lines(version, metadata))

            lines.append(description or "No description")
            lines.append("\n{% endcut %}\n\n")
        return "\n".join(lines)

    def dump_release_notes(self, output_paths, content=None):
        if content is None:
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
        repo_name="ytsaurus",
        component_name="Flow",
        tag_name="flow",
        description=FLOW_DESCRIPTION,
        filename="flow.md",
        artifacts=[
            DockerArtifact(label="Docker image", repo_name="ytsaurus", image_repo="flow", tag_templates=["{version}"]),
            DockerArtifact(label="Docker image with JRE 17", repo_name="ytsaurus", image_repo="flow-java", tag_templates=["{version}"]),
            DockerArtifact(label="Docker image with Python 3", repo_name="ytsaurus", image_repo="flow-python", tag_templates=["{version}"]),
            Artifact(
                label="Java SDK in Maven Central",
                kind="package",
                page_url="https://central.sonatype.com/artifact/tech.ytsaurus/flow-core/versions",
                version_url_template="https://central.sonatype.com/artifact/tech.ytsaurus/flow-core/{version}",
            ),
            Artifact(
                label="Python SDK in PyPI",
                kind="package",
                page_url="https://pypi.org/project/ytsaurus-flow-companion/",
                version_url_template="https://pypi.org/project/ytsaurus-flow-companion/{version}/",
            ),
            Artifact(
                label="yt_sync_mini in PyPI",
                kind="package",
                version_url_template="https://pypi.org/project/ytsaurus-flow-yt-sync-mini/{version}/",
            ),
            Artifact(
                label="Go SDK module",
                kind="package",
                page_url="https://pkg.go.dev/go.ytsaurus.tech/yt/go/flow",
                version_url_template="https://pkg.go.dev/go.ytsaurus.tech/yt/go/flow@v{version}",
            ),
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


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Generate release notes"
    )

    parser.add_argument(
        "-o", "--output-path",
        action="append",
        required=True,
        help="Path to the output directory."
    )

    args = parser.parse_args(argv)

    try:
        contents = [(c, c._generate_markdown()) for c in COMPONENTS]
        for component, content in contents:
            component.dump_release_notes(args.output_path, content)
    except Exception as e:
        parser.exit(1, f"Error generating release notes: {e}\n")


if __name__ == "__main__":
    main()
