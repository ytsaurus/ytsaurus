#pragma once

#include "public.h"
#include "config.h"

#include <yt/library/skiff/skiff.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForSkiff(
    NSkiff::TSkiffSchemaPtr skiffSchema,
    NTableClient::IValueConsumer* consumer);

std::unique_ptr<IParser> CreateParserForSkiff(
    NSkiff::TSkiffSchemaPtr skiffSchema,
    const NTableClient::TTableSchema& tableSchema,
    NTableClient::IValueConsumer* consumer);

std::unique_ptr<IParser> CreateParserForSkiff(
    NTableClient::IValueConsumer* consumer,
    TSkiffFormatConfigPtr config,
    int tableIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
