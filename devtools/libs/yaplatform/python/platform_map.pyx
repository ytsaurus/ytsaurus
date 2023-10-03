import json
import six

from util.generic.string cimport TString, TStringBuf
from util.system.types cimport ui32

cdef extern from "devtools/libs/yaplatform/platform_map.h" namespace "NYa" nogil:
    cdef cppclass TPlatformMap
    cdef TPlatformMap MappingFromJsonString(TStringBuf mapping) except +
    cdef TString MappingVarName[T](TStringBuf baseName, const T& mapping)
    cdef TString MappingPatternToJsonString[T](TStringBuf varNameWithHash, const T& mapping)
    cdef TString ResourceDirName(TStringBuf uri, ui32 stripPrefix) except +
    cdef TString ResourceVarName(TStringBuf baseName, TStringBuf uri, ui32 stripPrefix) except +

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

def get_resource_dir_name(uri, strip_prefix):
    uri = six.ensure_binary(uri)
    strip_prefix = strip_prefix or 0
    return six.ensure_str(ResourceDirName(uri, strip_prefix))

def get_resource_var_name(base_name, uri, strip_prefix):
    base_name = six.ensure_binary(base_name)
    uri = six.ensure_binary(uri)
    strip_prefix = strip_prefix or 0
    return six.ensure_str(ResourceVarName(base_name, uri, strip_prefix))
