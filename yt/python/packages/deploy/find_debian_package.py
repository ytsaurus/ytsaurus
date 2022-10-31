import gzip
import re
import requests
import logging


def parse_package_versions(data_stream, package_name):
    PACKAGE_PATTERN = re.compile(r"Package: (.*)")
    VERSION_PATTERN = re.compile(r"Version: (.*)")

    package = None
    version = None

    versions = []

    for line in data_stream:
        if not line.strip() and package == package_name:
            versions.append(version)

        package_match = PACKAGE_PATTERN.match(line)
        if package_match:
            package = package_match.group(1)
            continue

        version_match = VERSION_PATTERN.match(line)
        if version_match:
            version = version_match.group(1)
            continue

    return versions


def fetch_package_versions(repo, branch, package_name):
    url = "http://dist.yandex.ru/{repo}/{branch}/all/Packages.gz".format(repo=repo, branch=branch)

    response = requests.get(url, stream=True)
    if not response.ok:
        escaped_content = response.content.replace(b"\n", b"\\n")
        logging.error("Response: {}".format(escaped_content))
        response.raise_for_status()

    contents = gzip.decompress(response.raw.read()).decode("utf-8")
    return parse_package_versions(contents.split("\n"), package_name)


def find_debian_package(repo, package_name, target_version):
    for branch in ["unstable", "testing", "stable"]:
        if target_version in fetch_package_versions(repo, branch, package_name):
            return True
    return False
