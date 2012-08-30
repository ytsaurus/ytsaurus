#pragma once

#include "public.h"
#include "config.h"

#include <ytlib/ytree/parser.h>
#include <ytlib/ytree/yson_consumer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TAutoPtr<NYTree::IParser> CreateParserForDsv(
    NYTree::IYsonConsumer* consumer,
    TDsvFormatConfigPtr config = NULL,
    bool makeRecordProcessing = true);

////////////////////////////////////////////////////////////////////////////////

void ParseDsv(
    TInputStream* input,
    NYTree::IYsonConsumer* consumer,
    TDsvFormatConfigPtr config = NULL);

void ParseDsv(
    const TStringBuf& data,
    NYTree::IYsonConsumer* consumer,
    TDsvFormatConfigPtr config = NULL);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
