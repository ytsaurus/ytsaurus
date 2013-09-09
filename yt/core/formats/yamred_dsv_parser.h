#pragma once

#include "public.h"
#include "config.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForYamredDsv(
    NYson::IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config = NULL);

////////////////////////////////////////////////////////////////////////////////

void ParseYamredDsv(
    TInputStream* input,
    NYson::IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config = NULL);

void ParseYamredDsv(
    const TStringBuf& data,
    NYson::IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config = NULL);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

