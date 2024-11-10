#pragma once

#include <yt/yt/client/formats/public.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

//! Parses a YAML stream in pull mode (may be used for structured driver commands).
void ParseYaml(
    IInputStream* input,
    NYson::IYsonConsumer* consumer,
    TYamlFormatConfigPtr config,
    NYson::EYsonType ysonType);

//! Parses a YAML stream in push mode (may be used for tabular driver commands).
std::unique_ptr<IParser> CreateParserForYaml(
    NYson::IYsonConsumer* consumer,
    TYamlFormatConfigPtr config,
    NYson::EYsonType ysonType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
