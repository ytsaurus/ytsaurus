from cpython.version cimport PY_MAJOR_VERSION

from libcpp cimport bool as cpp_bool
from util.generic.vector cimport TVector
from util.generic.hash cimport THashMap
from util.generic.string cimport TString
from util.system.types cimport i64, ui64

from yt.yson.yson_types import YsonStringProxy, get_bytes


cdef extern from "library/cpp/yson/node/node.h" namespace "NYT" nogil:
    cdef cppclass TNode:
        TNode() except +
        TNode(const char*) except +
        TNode(TString) except +
        TNode(double) except +
        TNode(cpp_bool) except +
        TNode(i64) except +
        TNode(ui64) except +

        cpp_bool IsString()
        cpp_bool IsInt64()
        cpp_bool IsUint64()
        cpp_bool IsDouble()
        cpp_bool IsBool()
        cpp_bool IsList()
        cpp_bool IsMap()
        cpp_bool IsEntity()
        cpp_bool IsUndefined()

        TString& AsString()
        i64 AsInt64()
        ui64 AsUint64()
        double AsDouble()
        cpp_bool AsBool()
        TVector[TNode]& AsList()
        THashMap[TString, TNode]& AsMap()

        @staticmethod
        TNode CreateList()
        @staticmethod
        TNode CreateMap()

        TNode operator()(TString, TNode)
        TNode Add(TNode)


class Node(object):
    INT64 = 0
    UINT64 = 1
    _ALL_TYPES = {INT64, UINT64}

    def __init__(self, data, node_type):
        self.data = data
        if node_type not in Node._ALL_TYPES:
            raise Exception('unsupported node_type')
        self.node_type = node_type


def node_i64(i):
    return Node(i, Node.INT64)


def node_ui64(ui):
    return Node(ui, Node.UINT64)


cdef TString _to_TString(s):
    assert isinstance(s, (basestring, bytes, YsonStringProxy))
    s = get_bytes(s)
    return TString(<const char*>s, len(s))


cdef _TNode_to_pyobj(TNode node) except +:
    if node.IsString():
        return node.AsString()
    elif node.IsInt64():
        return node.AsInt64()
    elif node.IsUint64():
        return node.AsUint64()
    elif node.IsDouble():
        return node.AsDouble()
    elif node.IsBool():
        return node.AsBool()
    elif node.IsEntity():
        return None
    elif node.IsUndefined():
        return None
    elif node.IsList():
        node_list = node.AsList()
        return [_TNode_to_pyobj(n) for n in node_list]
    elif node.IsMap():
        node_map = node.AsMap()
        return {p.first: _TNode_to_pyobj(p.second) for p in node_map}
    else:
        # should never happen
        raise Exception()


cdef TNode _pyobj_to_TNode(obj) except +:
    if isinstance(obj, Node):
        if obj.node_type == Node.INT64:
            return TNode(<i64>obj.data)
        elif obj.node_type == Node.UINT64:
            return TNode(<ui64>obj.data)
        else:
            # should never happen
            raise Exception()
    elif isinstance(obj, bool):
        return TNode(<cpp_bool>obj)
    elif isinstance(obj, (basestring, bytes, YsonStringProxy)):
        return TNode(_to_TString(obj))
    elif isinstance(obj, long):
        if obj < 2**63:
            return TNode(<i64>obj)
        else:
            return TNode(<ui64>obj)
    elif isinstance(obj, int):
        return TNode(<i64>obj)
    elif isinstance(obj, float):
        return TNode(<float>obj)
    elif isinstance(obj, dict):
        node = TNode.CreateMap()
        items_iterator = (obj.iteritems() if PY_MAJOR_VERSION < 3 else obj.items())
        for k, v in items_iterator:
            node(_to_TString(k), _pyobj_to_TNode(v))
        return <TNode&&>node
    elif isinstance(obj, (list, tuple)):
        node = TNode.CreateList()
        for x in obj:
            node.Add(_pyobj_to_TNode(x))
        return <TNode&&>node
    elif obj is None:
        return TNode()
    else:
        raise Exception('Can\'t convert {} object to TNode'.format(type(obj)))
