#pragma once

#include "public.h"
#include "config.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForProtobuf(
    NTableClient::IValueConsumer* consumer,
    TProtobufFormatConfigPtr config,
    int tableIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

