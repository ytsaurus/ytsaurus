#pragma once

#include <yt/yt/client/formats/config.h>
#include <yt/yt/library/formats/yaml_parser.h>
#include <yt/yt/core/yson/yson_builder.h>
#include <yt/yt/core/ytree/convert.h>

#include <util/stream/mem.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Parses a YAML string and deserializes the result into type T
//! using the standard YsonStruct deserialization machinery.
template <class T>
T ConvertFromYaml(TStringBuf yaml);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define YAML_HELPERS_INL_H_
#include "yaml_helpers-inl.h"
#undef YAML_HELPERS_INL_H_
