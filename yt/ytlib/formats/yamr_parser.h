#pragma once

#include "public.h"
#include "config.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TAutoPtr<NYTree::IParser> CreateParserForYamr(
    NYTree::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config = NULL);

////////////////////////////////////////////////////////////////////////////////

void ParseYamr(
    TInputStream* input,
    NYTree::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config = NULL);

void ParseYamr(
    const TStringBuf& data,
    NYTree::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config = NULL);

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NFormats
} // namespace NYT
