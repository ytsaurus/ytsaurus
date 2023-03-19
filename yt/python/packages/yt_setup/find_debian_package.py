import requests
import logging


def fetch_package_versions(repo, package_name):
    url = "http://dist.yandex.ru/api/v1/search"
    params = {
        "pkg": package_name,
        "repo": repo,
        "strict": "true",
    }

    response = requests.get(url, params=params)
    if not response.ok:
        escaped_content = response.content.replace(b"\n", b"\\n")
        logging.error("Response: {}".format(escaped_content))
        response.raise_for_status()

    result = response.json()["result"]
    return [(entry["version"], entry["environment"]) for entry in result]


def find_debian_package(repo, package_name, target_version):
    for version, _ in fetch_package_versions(repo, package_name):
        if version == target_version:
            return True
    return False
