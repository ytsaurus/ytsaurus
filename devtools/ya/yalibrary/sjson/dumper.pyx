import io
import sys
import typing

cdef extern from "devtools/ya/yalibrary/sjson/lib/dump.h" namespace "NSJson":
    void DumpToStream(object obj, object stream) except *


def dump(obj: typing.Any, stream: io.IOBase) -> None:
    DumpToStream(obj, stream)


def dumps(obj: typing.Any) -> bytes:
    stream = io.BytesIO()
    dump(obj, stream)
    return stream.getvalue()
