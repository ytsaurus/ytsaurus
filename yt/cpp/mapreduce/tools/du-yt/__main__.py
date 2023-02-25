# coding=utf-8

import argparse
import library.python.init_log
import library.python.resource
import logging
import os
import re
import SimpleHTTPServer
import simplejson
import socket
import SocketServer

import yt.wrapper as yt

_L = logging.getLogger(__name__)


def _parse_options():
    p = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument(
        '-s', '--yt-proxy',
        dest='yt_proxy',
        required=True,
        help='YT proxy')
    p.add_argument(
        '-r', '--root',
        dest='yt_root',
        required=True,
        help='Discovery root', )
    g = p.add_mutually_exclusive_group(required=False)
    g.add_argument(
        '--only-run-server',
        dest='only_run_server',
        action='store_true',
        help='Only run server', )
    g.add_argument(
        '--dont-run-server',
        dest='dont_run_server',
        action='store_true',
        help='Don\'t run server', )
    p.add_argument(
        '-p', '--port',
        dest='server_port',
        type=int,
        default=8000,
        help='Server port', )
    p.add_argument(
        '--use-number-sign',
        dest='use_number_sign',
        action='store_true',
        help='Replace digits in table names with #', )
    p.add_argument(
        '--use-resource-type',
        dest='use_resource_type',
        default="disk_space",
        choices=('disk_space', 'node_count', 'chunk_count', ),
        help='resource type to account', )
    return p.parse_args()


def _add_path(m, path, size):
    prev = None
    last = m
    for name in path.split('/'):
        if name not in last:
            last[name] = {}
        prev = last
        last = last[name]

    if isinstance(prev[name], int):
        prev[name] += size
    else:
        prev[name] = size


def _acquire_yt_token(proxy):
    if os.environ.get('YT_TOKEN') is not None:
        return os.environ['YT_TOKEN']
    if os.environ.get('YQL_TOKEN') is not None:
        return os.environ['YQL_TOKEN']

    c = yt.YtClient(proxy=proxy)
    c.config['allow_receive_token_by_current_ssh_session'] = True
    return yt.http_helpers.get_token(client=c)


def _get_fs_map(
        yt_proxy,
        yt_root,
        replace_digit_with_number_sign,
        resource_name
):
    ytc = yt.YtClient(proxy=yt_proxy, token=_acquire_yt_token(yt_proxy))

    r = re.compile('\d')  # noqa: W605
    m = {}
    assert 'map_node' == ytc.get(yt_root + '/@type')
    for t in ytc.search(yt_root,
                        node_type=['table', 'file'],
                        attributes=['resource_usage', ]):
        size = int(t.attributes.get('resource_usage').get(resource_name))
        if not size:
            continue

        name = 'root/' + str(t)[2:]
        if replace_digit_with_number_sign:
            name = r.sub('#', name)
        _add_path(m, name, size)

    # gathering stub for every folder
    for t in ytc.search(yt_root,
                        node_type=['map_node'],
                        attributes=['resource_usage', ]):
        name = 'root/' + str(t)[2:]
        if replace_digit_with_number_sign:
            name = r.sub('#', name)
        _add_path(m, os.path.join(name, '_empty_folder_'), 0)

    return m


def _sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


_NAME_STR = 'name'
_DATA_STR = 'data'
_AREA_STR = '$area'
_CHILDREN_STR = 'children'


def _map_to_tree_map(name, m, use_fmt, apdx):

    def stub(x):
        return str(x)

    if use_fmt:
        sz_fmt = _sizeof_fmt
    else:
        sz_fmt = stub

    if isinstance(m, int):
        return {
            _NAME_STR: ' / '.join((name, sz_fmt(m), )),
            _DATA_STR: {
                _AREA_STR: m,
            },
        }

    c = []
    for k, v in m.iteritems():
        c.append(_map_to_tree_map(k, v, use_fmt, apdx))

    size = sum(x[_DATA_STR][_AREA_STR] for x in c) + apdx
    return {
        _NAME_STR: ' / '.join((name, sz_fmt(size), )),
        _DATA_STR: {
            _AREA_STR: size,
        },
        _CHILDREN_STR: c
    }


def _ensure_dir_exists(path):
    try:
        os.makedirs(path)
    except OSError:
        if not os.path.isdir(path):
            raise


def _prepare_directory(out_dir):
    _ensure_dir_exists(out_dir)
    with open(os.path.join(out_dir, 'index.html'), 'wb') as f:
        f.write(library.python.resource.find('/index.html'))
    with open(os.path.join(out_dir, 'webtreemap.css'), 'wb') as f:
        f.write(library.python.resource.find('/webtreemap.css'))
    with open(os.path.join(out_dir, 'webtreemap.js'), 'wb') as f:
        f.write(library.python.resource.find('/webtreemap.js'))


def _get_url(port):
    host_name = socket.gethostname()
    return 'http://{}:{}'.format(host_name, port)


class _TCPServer(SocketServer.TCPServer):

    def server_bind(self):
        # Override this method to be sure v6only is false: we want to
        # listen to both IPv4 and IPv6!
        self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, False)
        SocketServer.TCPServer.server_bind(self)


class _SilentHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    def log_message(self, format, *args):
        _L.debug(format, args)


def _run_server(html_dir, port):
    os.chdir(html_dir)

    url = _get_url(port)

    try:
        handler = _SilentHandler
        _TCPServer.address_family = socket.AF_INET6
        server = _TCPServer(('', port), handler)
        _L.info('Server started at %s', url)
        server.serve_forever()
    except KeyboardInterrupt:
        _L.info('Shutting down server at %s', url)
        server.socket.close()


def _make_tree_map(html_dir, yt_proxy, yt_root, replace_digit_with_number_sign, resource_name):
    if resource_name == "disk_space":
        sz_fmt = True
    else:
        sz_fmt = False
    if resource_name == "node_count":
        apdx = 1
    else:
        apdx = 0
    b = _map_to_tree_map('root', _get_fs_map(yt_proxy, yt_root, replace_digit_with_number_sign, resource_name), sz_fmt, apdx)
    # b[_DATA_STR]['size'] = b[_DATA_STR][_AREA_STR]
    with open(os.path.join(html_dir, 'du_yt.js'), 'wb') as f:
        f.write('var kTree = ')
        for c in simplejson.JSONEncoderForHTML().iterencode(b):
            f.write(c)


def _main(args):
    library.python.init_log.init_log(level='INFO')

    html_dir = 'du-yt-html'
    _prepare_directory(html_dir)
    if not args.only_run_server:
        _make_tree_map(html_dir, args.yt_proxy, args.yt_root, args.use_number_sign, args.use_resource_type)

    if not args.dont_run_server:
        _run_server(html_dir, args.server_port)


if '__main__' == __name__:
    args = _parse_options()
    _main(args)
