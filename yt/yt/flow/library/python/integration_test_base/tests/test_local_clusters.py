import json

import pytest
import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import _load_local_yt_clusters


@pytest.mark.authors(["pechatnov"])
@pytest.mark.parametrize(
    "metadata,proxy,aliases,expected",
    [
        ([('primary', 'localhost:80'), ('replica', 'localhost:81')], None, '{}', ['primary', 'replica']),
        ([('primary', 'localhost:80')], 'localhost:80', '{}', ['primary']),
        ([('primary', 'localhost:80')], 'primary', '{primary="localhost:80";}', ['primary']),
        (None, 'local', '{local="localhost:80";extra="localhost:81";}', ['local']),
        (None, 'localhost:80', '{local="localhost:80";extra="localhost:81";}', ['local']),
        (None, 'local', '{local="localhost:80";other="localhost:80";}', ['local']),
        (
            [('primary', 'localhost:80'), ('replica', 'localhost:81')],
            'primary',
            '{primary="localhost:80";replica="localhost:81";extra="localhost:82";}',
            ['primary', 'replica'],
        ),
    ],
)
def test_local_clusters(tmp_path, monkeypatch, metadata, proxy, aliases, expected):
    path = tmp_path / 'yt_cluster_info.json'
    monkeypatch.setattr(yatest.common, 'output_path', lambda *args: str(path))
    if metadata is not None:
        path.write_text(json.dumps([{'id': name, 'proxy_address': address} for name, address in metadata]))
    environ = {'YT_PROXY_URL_ALIASING_CONFIG': aliases}
    if proxy is not None:
        environ['YT_PROXY'] = proxy
    clusters = _load_local_yt_clusters(environ)
    assert list(clusters) == expected
    assert list(clusters.values()) == ['localhost:80', 'localhost:81'][: len(expected)]


@pytest.mark.authors(["pechatnov"])
@pytest.mark.parametrize(
    "metadata,proxy,aliases,message",
    [
        (None, None, '{}', 'set YT_PROXY'),
        (None, 'localhost:80', '{}', 'unique cluster identity'),
        (None, 'localhost:80', '{first="localhost:80";second="localhost:80";}', 'unique cluster identity'),
        ([], None, '{}', 'at least one'),
        ([('primary', 'localhost:80')], 'localhost:81', '{}', 'primary cluster address'),
        ([('primary', 'localhost:80')], '', '{}', 'must not be empty'),
        (
            [('primary', 'localhost:80'), ('replica', 'localhost:81')],
            None,
            '{replica="localhost:82";}',
            'address of cluster',
        ),
        (None, 'local', '[]', 'must map cluster names'),
        (None, 'local', '{local=0;}', 'must map cluster names'),
    ],
)
def test_conflicting_local_clusters(tmp_path, monkeypatch, metadata, proxy, aliases, message):
    path = tmp_path / 'yt_cluster_info.json'
    monkeypatch.setattr(yatest.common, 'output_path', lambda *args: str(path))
    if metadata is not None:
        path.write_text(json.dumps([{'id': name, 'proxy_address': address} for name, address in metadata]))
    environ = {'YT_PROXY_URL_ALIASING_CONFIG': aliases}
    if proxy is not None:
        environ['YT_PROXY'] = proxy
    with pytest.raises(ValueError, match=message):
        _load_local_yt_clusters(environ)


@pytest.mark.authors(["pechatnov"])
def test_malformed_metadata_is_not_replaced_by_environment(tmp_path, monkeypatch):
    path = tmp_path / 'yt_cluster_info.json'
    path.write_text('{')
    monkeypatch.setattr(yatest.common, 'output_path', lambda *args: str(path))
    with pytest.raises(json.JSONDecodeError):
        _load_local_yt_clusters({'YT_PROXY': 'local', 'YT_PROXY_URL_ALIASING_CONFIG': '{local="localhost:80";}'})
