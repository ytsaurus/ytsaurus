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
    NTableClient::TTableSchemaPtr tableSchema,
    NTableClient::IValueConsumer* consumer);

std::unique_ptr<IParser> CreateParserForSkiff(
    NTableClient::IValueConsumer* consumer,
    const std::vector<NSkiff::TSkiffSchemaPtr>& skiffSchemas,
    TSkiffFormatConfigPtr config,
    int tableIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
