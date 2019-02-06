from os_helpers import run_captured
from teamcity_helpers import teamcity_message

# Used to teamcity to import paths.
import teamcity_helpers  # noqa
from teamcity.helpers import dch  # noqa

import requests

import gzip
from cStringIO import StringIO


def get_package_versions(package_name, repo):
    arch_output = run_captured(["dpkg-architecture"])
    arch = filter(lambda line: line.startswith("DEB_BUILD_ARCH="), arch_output.split("\n"))[0].split("=")[1]

    versions = set()
    for branch in ["unstable", "testing", "prestable", "stable"]:
        url = "http://dist.yandex.ru/{0}/{1}/{2}/Packages.gz".format(repo, branch, arch)
        rsp = requests.get(url)
        # Temporary workaround, remove it when problem with dist will be fixed.
        if rsp.status_code != 200:
            teamcity_message("Failed to get packages list from {0}, skipping it".format(url), "WARNING")
            continue
        content_gz = rsp.content
        content = gzip.GzipFile(fileobj=StringIO(content_gz))
        current_package = None
        current_version = None
        for line in content:
            line = line.strip()
            if line.startswith("Package: "):
                current_package = line.split()[1]
            if line.startswith("Version: "):
                current_version = line.split()[1]
            if not line:
                if current_package == package_name and current_version is not None:
                    versions.add(current_version)
                current_package = None
                current_version = None

    return versions

def get_local_package_version():
    return run_captured("dpkg-parsechangelog | grep Version | awk '{print $2}'", shell=True).strip()
