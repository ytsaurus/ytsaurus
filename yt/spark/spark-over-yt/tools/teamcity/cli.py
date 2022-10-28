#!/usr/bin/env/ python3

from xml.etree import ElementTree
import os
import pathlib
import re
import subprocess
import typing

import click


ARCADIA_PROJECT_ROOT = 'yt/spark/'


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


def collect_released_versions(*items):
    project_root = pathlib.Path(__file__).absolute().parent.parent.parent.parent
    collected_versions: typing.List[ComponentVersion] = []
    for item in items:
        collected_versions.extend(project_version_collectors[item].extract_version(project_root))
    return collected_versions


def generate_xml_creds(src, dst):
    username = ''
    password = ''
    for line in src:
        key, value, *_ = line.split('=')

        key = key.strip()
        value = value.strip()

        if key == 'user':
            username = value
        elif key == 'password':
            password = value

    if not username or not password:
        raise RuntimeError('Username or password not present in artifactory deploy creds')

    settings = ElementTree.Element('settings')

    servers = ElementTree.Element('servers')
    settings.append(servers)

    server = ElementTree.Element('server')
    servers.append(server)

    id_ = ElementTree.Element('id')
    id_.text = 'yandex-spark'
    server.append(id_)

    uname = ElementTree.Element('username')
    uname.text = username
    server.append(uname)

    pwd = ElementTree.Element('password')
    pwd.text = password
    server.append(pwd)

    tree = ElementTree.ElementTree(settings)

    tree.write(dst)


def _get_changed_files(branch='trunk'):
    diff = subprocess.run([
        'arc',
        'diff',
        '--name-only',
        branch,
        '.',
    ],
        check=True,
        capture_output=True,
    )
    output = diff.stdout.decode('utf-8').strip()
    changed_files = output.split('\n') if output else set()
    for file in changed_files:
        yield str(pathlib.Path(file).relative_to(ARCADIA_PROJECT_ROOT))


@click.group()
def cli():
    pass


@cli.command(
    name='generate-xml-creds',
    help='generate XML for Maven credentials from sbt credentials file',
)
@click.argument('src', type=click.File())
@click.argument('dst', type=click.File(mode='wb'))
def _(src, dst):
    generate_xml_creds(src, dst)


@cli.command(
    name='teamcity-report',
    help='report released versions as teamcity status',
)
@click.argument('component', type=click.STRING, nargs=-1)
def _(component):
    collected_versions = collect_released_versions(*component)

    status = []

    click.echo('Released components:')
    for version in collected_versions:
        version_string = f'{version.component}@{version.version}'
        status.append(version_string)

    print(f'##teamcity[buildStatus status=\'SUCCESS\' text=\'Released: {" ".join(status)}\']')


@cli.command(
    name='bump',
    help='send version bump to arcadia',
)
def _():
    changed_files = set(_get_changed_files('trunk'))
    components = ['cluster', 'client']
    files_to_commit = []
    for component_name in project_version_collectors:
        collector = project_version_collectors[component_name]
        if str(collector.path) in changed_files:
            components.append(component_name)
            files_to_commit.append(collector.path)
    released_versions = collect_released_versions(*components)

    branch_name = 'spyt-release-' + '-'.join(f'{version.component}{version.version}' for version in released_versions)

    subprocess.run([
        'arc',
        'checkout',
        '-b',
        branch_name,
    ], check=True,
    )

    subprocess.run([
        'arc',
        'add',
        *files_to_commit,
    ], check=True),

    subprocess.run([
        'arc',
        'commit',
        '-m',
        'Bump versions',
    ], check=True)

    release_message = 'Released versions: ' + ' '.join(
        f'{version.component} {version.version}'
        for version in released_versions
    )
    subprocess.run([
        'arc',
        'pr',
        'create',
        '--push',
        '--auto',
        '--message',
        release_message,
    ], check=True,
    )


if __name__ == '__main__':
    cli()
