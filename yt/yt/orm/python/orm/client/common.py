from yt.wrapper.common import is_arcadia_python

from yt_yson_bindings import dumps_proto

from yt.common import get_value

try:
    from yt.packages.six import (
        iterbytes,
        iteritems,
    )
except ImportError:
    from six import (
        iterbytes,
        iteritems,
    )

from copy import deepcopy
import os


def get_certificate_path(path):
    return get_value(path, os.path.join(os.path.dirname(__file__), "YandexInternalRootCA.crt"))


def load_certificate(path):
    if path is None and is_arcadia_python():
        import library.python.resource

        return library.python.resource.find("/yt/orm/YandexInternalRootCA.crt")
    return open(get_certificate_path(path), "rb").read()


def hexify(message):
    def convert(byte):
        if byte < 128:
            return chr(byte)
        else:
            return hex(byte)

    return "".join(map(convert, iterbytes(message)))


def hide_token(obj):
    def hide_token_recursive(obj):
        if isinstance(obj, dict):
            if "Authorization" in obj:
                obj["Authorization"] = "x" * 32
            if "X-Ya-User-Ticket" in obj:
                obj["X-Ya-User-Ticket"] = "x" * 32
            for key, value in iteritems(obj):
                obj[key] = hide_token_recursive(value)
        if isinstance(obj, list):
            for index, item in enumerate(obj):
                if (
                    isinstance(item, tuple)
                    and len(item) == 2
                    and (item[0] == "yt-auth-token" or item[0] == "yt-auth-user-ticket")
                ):
                    obj[index] = (item[0], "x" * 32)
            for index, item in enumerate(obj):
                obj[index] = hide_token_recursive(item)
        return obj

    return hide_token_recursive(deepcopy(obj))


def _truncate(message, size_limit):
    if size_limit is None:
        return message
    truncated_message = b"...truncated"
    if len(message) > size_limit + len(truncated_message):
        message = message[:size_limit] + truncated_message
    return message


def format_grpc_request(request, size_limit=None):
    return hexify(
        _truncate(dumps_proto(request, yson_format="text", output_limit=size_limit), size_limit)
    )


def format_grpc_response(response, size_limit=None):
    return hexify(
        _truncate(dumps_proto(response, yson_format="text", output_limit=size_limit), size_limit)
    )


def try_close(channel):
    # C++ implementation of channel requires method 'close' to be called.
    # Python implementation has no method 'close'.
    if hasattr(channel, "close"):
        channel.close()
