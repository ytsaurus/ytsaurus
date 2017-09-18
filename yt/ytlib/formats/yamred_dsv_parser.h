#pragma once

#include "public.h"
#include "config.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForYamredDsv(
    NYson::IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config = nullptr);

////////////////////////////////////////////////////////////////////////////////

void ParseYamredDsv(
    IInputStream* input,
    NYson::IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config = nullptr);

void ParseYamredDsv(
    const TStringBuf& data,
    NYson::IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

