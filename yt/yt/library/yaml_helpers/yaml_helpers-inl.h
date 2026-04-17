#ifndef YAML_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include yaml_helpers.h"
#include "yaml_helpers.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T ConvertFromYaml(TStringBuf yaml)
{
    TMemoryInput input(yaml);
    auto config = New<NFormats::TYamlFormatConfig>();
    NYson::TYsonStringBuilder builder(NYson::EYsonFormat::Binary, NYson::EYsonType::Node);
    NFormats::ParseYaml(&input, builder.GetConsumer(), config, NYson::EYsonType::Node);
    return NYTree::ConvertTo<T>(NYTree::ConvertToNode(builder.Flush()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
