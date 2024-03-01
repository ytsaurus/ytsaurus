import json
from enum import Enum
from functools import total_ordering
from os.path import join, exists
from typing import Dict, NamedTuple, List


class PackedVersion:
    def __init__(self, json_object: Dict[str, str]):
        self.scala = json_object.get('scala')
        self.python = json_object.get('python')
        self.is_snapshot = "SNAPSHOT" in self.scala
        if self.python is not None and self.is_snapshot != ("b" in self.python):
            raise RuntimeError("Incorrect release mode of version")

    def __repr__(self):
        return f"Scala = {self.scala}. Python = {self.python}"

    def get_release_mode(self) -> str:
        return "snapshots" if self.is_snapshot else "releases"

    def get_scala_version(self) -> str:
        return self.scala

    def get_version_directory(self) -> str:
        return f"{self.get_release_mode()}/{self.get_scala_version()}"


def load_version_file(file_path: str) -> PackedVersion:
    with open(file_path, 'r') as file:
        version_json = json.load(file)
        return PackedVersion(version_json)


@total_ordering
class ReleaseLevel(Enum):
    EMPTY = -1
    SPYT = 1
    SPARK_FORK = 2

    def __lt__(self, other):
        if isinstance(other, ReleaseLevel):
            return self.value < other.value
        else:
            return NotImplemented


class Versions(NamedTuple):
    spyt_version: PackedVersion
    spark_version: PackedVersion


def load_versions(sources_path: str) -> Versions:
    spyt_version = load_version_file(join(sources_path, 'version.json'))
    spark_version = load_version_file(join(sources_path, 'spark_version.json'))
    return Versions(spyt_version, spark_version)


def check_existence(sources_path: str, files: List[str]) -> bool:
    return all(exists(join(sources_path, file)) for file in files)


def check_spyt_files(sources_path: str) -> bool:
    return check_existence(sources_path, [
        'conf',
        'spyt-package.zip',
        'setup-spyt-env.sh'])


def check_spark_files(sources_path: str) -> bool:
    return check_existence(sources_path, ['spark.tgz'])


def get_release_level(sources_path: str) -> ReleaseLevel:
    if not check_spyt_files(sources_path):
        return ReleaseLevel.EMPTY
    if not check_spark_files(sources_path):
        return ReleaseLevel.SPYT
    return ReleaseLevel.SPARK_FORK


def approve(text: str) -> bool:
    while True:
        user_input = input(text)
        if user_input.lower() in ['yes', 'y']:
            return True
        elif user_input.lower() in ['no', 'n']:
            return False
