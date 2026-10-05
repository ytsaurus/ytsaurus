from datetime import date, datetime, time
from enum import Enum
from functools import wraps
from uuid import UUID

from pytz import timezone


escape_chars_map = {
    "\b": "\\b",
    "\f": "\\f",
    "\r": "\\r",
    "\n": "\\n",
    "\t": "\\t",
    "\0": "\\0",
    "\a": "\\a",
    "\v": "\\v",
    "\\": "\\\\",
    "'": "\\'"
}


def escape_datetime(item, context):
    server_tz = timezone(context.server_info.get_timezone())

    if item.tzinfo is not None:
        item = item.astimezone(server_tz)

    return "'%s'" % item.strftime('%Y-%m-%d %H:%M:%S')


def maybe_enquote_for_server(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        rv = f(*args, **kwargs)

        if kwargs.get('for_server'):
            is_str = isinstance(rv, str)

            if not is_str or (is_str and not rv.startswith("'")):
                rv = "'%s'" % rv

        return rv

    return wrapper


@maybe_enquote_for_server
def escape_param(item, context, for_server=False):
    if item is None:
        return 'NULL'

    elif isinstance(item, datetime):
        return escape_datetime(item, context)

    elif isinstance(item, date):
        return "'%s'" % item.strftime('%Y-%m-%d')

    elif isinstance(item, time):
        return "'%s'" % item.strftime('%H:%M:%S')

    elif isinstance(item, str):
        # We need double escaping for server-side parameters.
        if for_server:
            item = ''.join(escape_chars_map.get(c, c) for c in item)
        return "'%s'" % ''.join(escape_chars_map.get(c, c) for c in item)

    elif isinstance(item, (list, tuple)):
        brackets = '[%s]' if isinstance(item, list) else '(%s)'
        rv = brackets % ', '.join(
            str(escape_param(x, context)) for x in item
        )
        # Server expects the whole collection literal as an escaped
        # string (Field dump format), not a bare [...] or (...).
        if for_server:  # pragma: requires-clickhouse-22.8
            rv = "'%s'" % ''.join(escape_chars_map.get(c, c) for c in rv)
        return rv

    elif isinstance(item, Enum):
        return escape_param(item.value, context, for_server=for_server)

    elif isinstance(item, UUID):
        return "'%s'" % str(item)

    else:
        return item


def escape_params(params, context, for_server=False):
    escaped = {}

    for key, value in params.items():
        escaped[key] = escape_param(value, context, for_server=for_server)

    return escaped
