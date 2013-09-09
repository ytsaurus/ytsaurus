#pragma once

#include "public.h"
#include "config.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForYamr(
    NYson::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config = NULL);

////////////////////////////////////////////////////////////////////////////////

void ParseYamr(
    TInputStream* input,
    NYson::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config = NULL);

void ParseYamr(
    const TStringBuf& data,
    NYson::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config = NULL);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
