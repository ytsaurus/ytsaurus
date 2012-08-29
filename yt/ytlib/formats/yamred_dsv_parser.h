#pragma once

#include "public.h"
#include "config.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TAutoPtr<NYTree::IParser> CreateParserForYamredDsv(
    NYTree::IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config = NULL);

////////////////////////////////////////////////////////////////////////////////

void ParseYamredDsv(
    TInputStream* input,
    NYTree::IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config = NULL);

void ParseYamredDsv(
    const TStringBuf& data,
    NYTree::IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config = NULL);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

