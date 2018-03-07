#pragma once

#include "public.h"
#include "config.h"

#include <yt/core/yson/consumer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForSchemafulDsv(
    NYson::IYsonConsumer* consumer,
    TSchemafulDsvFormatConfigPtr config = New<TSchemafulDsvFormatConfig>());

////////////////////////////////////////////////////////////////////////////////

void ParseSchemafulDsv(
    IInputStream* input,
    NYson::IYsonConsumer* consumer,
    TSchemafulDsvFormatConfigPtr config = New<TSchemafulDsvFormatConfig>());

void ParseSchemafulDsv(
    const TStringBuf& data,
    NYson::IYsonConsumer* consumer,
    TSchemafulDsvFormatConfigPtr config = New<TSchemafulDsvFormatConfig>());

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

