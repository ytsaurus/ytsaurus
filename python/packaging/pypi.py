import requests

# Keep it sync with your pypi settings.
PYPI_URL = "https://pypi.yandex-team.ru/simple/{package_name}"

def extract_package_versions(package_name):
    def parts_to_version(parts):
        assert len(parts) == 4
        return "{}.{}.{}-{}".format(*parts)

    rsp = requests.get(PYPI_URL.format(package_name=package_name))
    for r in rsp.history:
        print r.headers
    rsp.raise_for_status()
    
    html = rsp.text
    lines_with_download_links = [line for line in html.split("\n") if "rel=\"package\"" in line]
    return set(
        map(
            parts_to_version,
            # Some magic to extract major.minor.patch version from download link.
            [
                line.split(package_name.replace("-", "_"))[1].split("-")[1].replace("-", ".").replace("_", ".").replace("post", "").split(".")[:4]
                for line in lines_with_download_links
            ]
        )
    )
