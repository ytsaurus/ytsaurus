import exts.http_client
import exts.http_server
import exts.tmp
import exts.os2


def test_http_get():
    with exts.tmp.temp_dir() as temp_dir:
        with exts.os2.change_dir(temp_dir):
            with exts.http_server.SilentHTTPServer() as server:
                url = 'http://{host}:{port}/'.format(host=server.host, port=server.port)
                return bytes(exts.http_client.http_get(url))
