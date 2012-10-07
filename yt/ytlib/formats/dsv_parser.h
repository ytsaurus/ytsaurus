#pragma once

#include "public.h"
#include "config.h"

#include <ytlib/ytree/parser.h>
#include <ytlib/ytree/yson_consumer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \param wrapWithMap If True then the parser wraps values with calls to
 *  #IYsonConsumer::OnBeginMap and #IYsonConsumer::OnEndMap.
 */
TAutoPtr<NYTree::IParser> CreateParserForDsv(
    NYTree::IYsonConsumer* consumer,
    TDsvFormatConfigPtr config = NULL,
    bool wrapWithMap = true);

////////////////////////////////////////////////////////////////////////////////

void ParseDsv(
    TInputStream* input,
    NYTree::IYsonConsumer* consumer,
    TDsvFormatConfigPtr config = New<TDsvFormatConfig>());

void ParseDsv(
    const TStringBuf& data,
    NYTree::IYsonConsumer* consumer,
    TDsvFormatConfigPtr config = New<TDsvFormatConfig>());

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
