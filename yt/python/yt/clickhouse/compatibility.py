from yt.wrapper.common import YtError

import yt.logger as logger

import re

LAUNCHER_VERSION = 1

MIN_YT_VERSION = (19, 8)
MIN_CHYT_VERSION = (0, 0)


def extract_version_tuple(version):
    occurrences = re.findall(r"[0-9]+\.[0-9]+\.[0-9]+", version)
    if not occurrences:
        return (0, 0, 0)
    assert len(occurrences) == 1
    result = tuple(map(int, occurrences[0].split(".")))
    assert len(result) == 3
    return result


def validate_ytserver_clickhouse_version(ytserver_clickhouse_attributes):
    errors = []

    # YT native code version.
    yt_version = extract_version_tuple(ytserver_clickhouse_attributes.get("yt_version", "0.0.0"))
    logger.info("YT native code version: %s", yt_version)
    if yt_version < MIN_YT_VERSION:
        errors.append(
            YtError("YT native code is too old: expected {} or newer, found {}".format(MIN_YT_VERSION, yt_version)))

    # CHYT version.
    chyt_version = extract_version_tuple(ytserver_clickhouse_attributes.get("version", "0.0.0"))
    logger.info("CHYT version: %s", chyt_version)
    if chyt_version < MIN_CHYT_VERSION:
        errors.append(
            YtError("CHYT code is too old: expected {} or newer, found {}".format(MIN_CHYT_VERSION, chyt_version)))

    if errors:
        raise YtError("ytserver-clickhouse binary is too old, see inner errors", inner_errors=errors)
