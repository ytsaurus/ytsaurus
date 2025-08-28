from libcpp cimport bool
from util.generic.hash_set cimport THashSet
from util.generic.string cimport TString

import io
import six
import sys
import typing


cdef extern from "devtools/ya/yalibrary/sjson/lib/load.h" namespace "NSJson":
    cppclass TLoaderOptions:
        bool InternKeys
        bool InternValues
        THashSet[TString] RootKeyBlackList
        THashSet[TString] RootKeyWhiteList

    object LoadFromStream(object stream, const TLoaderOptions& options)


wb_list_type = typing.Optional[typing.Iterable[str]]


def load(
    stream: io.IOBase,
    intern_keys: bool = False,
    intern_vals: bool = False,
    root_key_white_list: wb_list_type = None,
    root_key_black_list: wb_list_type = None,
) -> typing.Any:
    cdef TLoaderOptions opts
    opts.InternKeys = intern_keys
    opts.InternValues = intern_vals
    if root_key_white_list and root_key_black_list:
        raise ValueError("Only one of lists is allowed: root_key_white_list or root_key_black_list")
    if root_key_white_list:
        for key in root_key_white_list:
            opts.RootKeyWhiteList.insert(six.ensure_binary(key))
    if root_key_black_list:
        for key in root_key_black_list:
            opts.RootKeyBlackList.insert(six.ensure_binary(key))

    return LoadFromStream(stream, opts)


def loads(
    s: str|bytes,
    intern_keys: bool = False,
    intern_vals: bool = False,
    root_key_white_list: wb_list_type = None,
    root_key_black_list: wb_list_type = None,
) -> typing.Any:
    if isinstance(s, str) and six.PY3:
        s = s.encode()
    stream = io.BytesIO(s)
    return load(
        stream,
        intern_keys=intern_keys,
        intern_vals=intern_vals,
        root_key_white_list=root_key_white_list,
        root_key_black_list=root_key_black_list,
    )
