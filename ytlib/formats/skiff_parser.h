#pragma once

#include "public.h"
#include "config.h"

#include <yt/core/skiff/skiff.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForSkiff(
    NSkiff::TSkiffSchemaPtr skiffSchema,
    NTableClient::IValueConsumer* consumer);

std::unique_ptr<IParser> CreateParserForSkiff(
    NTableClient::IValueConsumer* consumer,
    TSkiffFormatConfigPtr config,
    int tableIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
