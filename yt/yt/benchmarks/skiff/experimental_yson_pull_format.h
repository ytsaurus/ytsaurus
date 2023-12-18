#pragma once

#include <yt/yt/client/formats/config.h>
#include <yt/yt/client/formats/parser.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForYsonPull(
    NTableClient::IValueConsumer* consumer,
    TYsonFormatConfigPtr config,
    int tableIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
