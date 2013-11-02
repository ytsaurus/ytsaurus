#pragma once

#include "public.h"
#include "config.h"

#include <ytlib/yson/consumer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForSchemedDsv(
    NYson::IYsonConsumer* consumer,
    TSchemedDsvFormatConfigPtr config = New<TSchemedDsvFormatConfig>());

////////////////////////////////////////////////////////////////////////////////

void ParseSchemedDsv(
    TInputStream* input,
    NYson::IYsonConsumer* consumer,
    TSchemedDsvFormatConfigPtr config = New<TSchemedDsvFormatConfig>());

void ParseSchemedDsv(
    const TStringBuf& data,
    NYson::IYsonConsumer* consumer,
    TSchemedDsvFormatConfigPtr config = New<TSchemedDsvFormatConfig>());

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

