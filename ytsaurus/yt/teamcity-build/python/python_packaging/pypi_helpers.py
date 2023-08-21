import requests

# Keep it sync with your pypi settings.
PYPI_URL = "https://pypi.yandex-team.ru/repo/default/{package_name}/"

def get_package_versions(package_name, tag_filters=None, version_part_count=4):
    def extract_full_version_string(line):
        # Some magic to extract full version string from html link.
        return line.split(package_name.replace("-", "_"))[1].split("-", 1)[1].rsplit(".whl", 1)[0]

    def parts_to_version(parts):
        assert len(parts) == version_part_count
        if version_part_count < 4:
            return ".".join(parts)
        elif version_part_count == 4:
            return "{}.{}.{}-{}".format(*parts)
        else:
            assert False

    def extract_version(full_version_string):
        # Some magic to extract major.minor.patch version from full version string.
        return parts_to_version(full_version_string.replace("-", ".").replace("_", ".", 1).replace("post", "").split(".")[:version_part_count])

    rsp = requests.get(PYPI_URL.format(package_name=package_name))
    if rsp.status_code == 404:  # Url not found.
        return set()
    rsp.raise_for_status()

    html = rsp.text
    lines_with_download_links = [line for line in html.split("\n") if "rel=\"package\"" in line]
    full_version_strings = map(extract_full_version_string, lines_with_download_links)

    if tag_filters is None:
        tag_filters = []
    tag_filter = lambda full_version_string: all(tag in full_version_string for tag in tag_filters)
    fill_version_strings_filtered = filter(tag_filter, full_version_strings)

    return set(map(extract_version, fill_version_strings_filtered))
