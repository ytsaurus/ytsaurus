import json
import six

from util.generic.string cimport TString, TStringBuf

cdef extern from "devtools/libs/yaplatform/platform_map.h" namespace "NYa" nogil:
    cdef cppclass TPlatformMap
    cdef TPlatformMap MappingFromJsonString(TStringBuf mapping) except +
    cdef TString MappingVarName[T](TStringBuf baseName, const T& mapping)
    cdef TString MappingPatternToJsonString[T](TStringBuf varNameWithHash, const T& mapping)

cdef TString MappingVarNameFromJson(TStringBuf baseName, TString mappingJson):
    return MappingVarName(baseName, MappingFromJsonString(mappingJson))

cdef TString GraphJsonNameFromJson(TStringBuf varNameWithHash, TStringBuf mappingJson):
    return MappingPatternToJsonString(varNameWithHash, MappingFromJsonString(mappingJson))

def mapping_var_name_from_json(base_name, mapping_json):
    mapping_json = six.ensure_binary(mapping_json)
    base_name = six.ensure_binary(base_name)
    return MappingVarNameFromJson(base_name, mapping_json)

def graph_json_from_resource_json(name, mapping_json):
    mapping_json = six.ensure_binary(mapping_json)
    name = six.ensure_binary(name)
    return json.loads(GraphJsonNameFromJson(name, mapping_json))
