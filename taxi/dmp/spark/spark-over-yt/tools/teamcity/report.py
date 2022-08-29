import os
import pathlib
import re
import sys
import typing


class ComponentVersion(typing.NamedTuple):
    component: str
    version: str


class VersionCollector(typing.NamedTuple):
    path: typing.Union[os.PathLike, str]
    extractor: typing.Callable[[pathlib.Path], typing.Iterable[ComponentVersion]]

    def extract_version(
        self,
        project_root: typing.Optional[typing.Union[os.PathLike, str]] = None,
    ) -> typing.Iterable[ComponentVersion]:
        path = pathlib.Path(self.path)
        if project_root:
            path = pathlib.Path(project_root).joinpath(path)
        return self.extractor(path)


def extract_client_version(path: pathlib.Path):
    with open(path) as f:
        s = f.read()
        match = re.search(r'spytClientVersion := "(.+?)"', s, re.MULTILINE)
        if match is None:
            raise RuntimeError("No client version found")
        yield ComponentVersion('client', match.group(1))
        match = re.search(r'spytClientPythonVersion := "(.+?)"', s, re.MULTILINE)
        if match is None:
            raise RuntimeError("No python client version found")
        yield ComponentVersion('client-python', match.group(1))


def extract_cluster_version(path: pathlib.Path):
    with open(path) as f:
        s = f.read()
        match = re.search(r'spytClusterVersion := "(.+?)"', s, re.MULTILINE)
        if match is None:
            raise RuntimeError("No cluster version found")
        yield ComponentVersion('cluster', match.group(1))


def extract_spark_fork_version(path: pathlib.Path):
    with open(path) as f:
        s = f.read()
        match = re.search(r'val sparkForkVersion = "(.+?)"', s, re.MULTILINE)
        if match is None:
            raise RuntimeError("No Spark fork version found")
        yield ComponentVersion('spark-fork', match.group(1))


project_version_collectors: typing.Mapping[str, VersionCollector] = {
    'client': VersionCollector('spark-over-yt/client_version.sbt', extract_client_version),
    'cluster': VersionCollector('spark-over-yt/cluster_version.sbt', extract_cluster_version),
    'spark-fork': VersionCollector('spark-over-yt/project/SparkForkVersion.scala', extract_spark_fork_version),
}


def main(*items):
    project_root = pathlib.Path(__file__).absolute().parent.parent.parent.parent
    collected_versions: typing.List[ComponentVersion] = []
    for item in items:
        collected_versions += list(project_version_collectors[item].extract_version(project_root))

    status = []

    print('Released components:')
    for version in collected_versions:
        version_string = f'{version.component}@{version.version}'
        print(version_string)
        status.append(version_string)

    print(f'##teamcity[buildStatus status=\'SUCCESS\' text=\'Released: {" ".join(status)}\']')


if __name__ == '__main__':
    main(*sys.argv[1:])
