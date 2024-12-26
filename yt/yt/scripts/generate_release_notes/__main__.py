from datetime import datetime

import argparse
import requests


REPO_OWNER = "ytsaurus"
OUTPUT_FILE = "release_notes.md"

YTSAURUS_SERVER_DESCRIPTION = """
All main components are released as a docker image.
"""

JAVA_SDK_DESCRIPTION = """
Is released as packages in [maven](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client).
"""

QUERY_TRACKER_DESCRIPTION = """
Is released as a docker image.
"""

CHYT_DESCRIPTION = """
Is released as a docker image.
"""

K8S_DESCRIPTION = """
Is released as helm charts on [Github Packages](https://github.com/ytsaurus/ytsaurus-k8s-operator/pkgs/container/ytop-chart).
"""

PYTHON_SDK_DESCRIPTION = """
Availabe as a package in [PyPI](https://pypi.org/project/ytsaurus-client/).
"""

PYTHON_YSON_DESCRIPTION = """
Availabe as a package in [PyPI](https://pypi.org/project/ytsaurus-yson/).
"""

STRAWBERRY_DESCRIPTION = """
Is released as a docker image.
"""

SPYT_DESCRIPTION = """
Is released as a docker image.
"""

repo_name_to_releases = {}


class Component:
    def __init__(self, repo_name, component_name, tag_name, description, filename):
        self.repo_name = repo_name
        self.component_name = component_name
        self.tag_name = tag_name
        self.description = description
        self.filename = filename

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
        return [release for release in all_releases if release["tag_name"].startswith(self.tag_name)]

    def _generate_markdown(self):
        lines = [f"## {self.component_name}\n"]
        lines += [self.description, "\n\n"]

        lines += ["**Releases:**\n"]

        releases = self._get_releases()

        for release in releases:
            version = release["tag_name"].split("/")[-1]
            lines.append(f"{{% cut \"**{version}**\" %}}\n")

            parsed_date = datetime.strptime(release["created_at"], "%Y-%m-%dT%H:%M:%SZ")
            lines.append(f"**Release date:** {parsed_date.strftime("%Y-%m-%d")}")
            lines.append("\n")

            description = release["body"]
            description = description.replace("\r\n", "\n")
            description = description.replace("\r", "\n")

            lines.append(description or "No description")
            lines.append("\n{% endcut %}\n\n")
        return "\n".join(lines)

    def dump_release_notes(self, output_paths):
        content = self._generate_markdown()
        for output_path in output_paths:
            with open(f"{output_path}/{self.filename}", "w", encoding="utf-8") as f:
                f.write(content)


COMPONENTS = [
    Component(repo_name="ytsaurus", component_name="YTsaurus server", tag_name="docker/ytsaurus", description=YTSAURUS_SERVER_DESCRIPTION, filename="yt-server.md"),
    Component(repo_name="ytsaurus", component_name="Query tracker", tag_name="query-tracker", description=QUERY_TRACKER_DESCRIPTION, filename="query-tracker.md"),
    Component(repo_name="ytsaurus", component_name="Java SDK", tag_name="java-sdk", description=JAVA_SDK_DESCRIPTION, filename="java-sdk.md"),
    Component(repo_name="ytsaurus", component_name="CHYT", tag_name="chyt", description=CHYT_DESCRIPTION, filename="chyt.md"),
    Component(repo_name="ytsaurus-k8s-operator", component_name="Kubernetes operator", tag_name="release", description=K8S_DESCRIPTION, filename="k8s.md"),
    Component(repo_name="ytsaurus", component_name="Python YSON bindings", tag_name="python/ytsaurus-yson", description=PYTHON_YSON_DESCRIPTION, filename="python-yson.md"),
    Component(repo_name="ytsaurus", component_name="Python SDK", tag_name="python/ytsaurus-client", description=PYTHON_SDK_DESCRIPTION, filename="python-sdk.md"),
    Component(repo_name="ytsaurus-spyt", component_name="SPYT", tag_name="spyt", description=SPYT_DESCRIPTION, filename="spyt.md"),
    Component(repo_name="ytsaurus", component_name="Strawberry", tag_name="yt/chyt/controller", description=STRAWBERRY_DESCRIPTION, filename="strawberry.md"),
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
