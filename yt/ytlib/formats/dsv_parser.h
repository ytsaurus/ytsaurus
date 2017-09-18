#pragma once

#include "public.h"
#include "config.h"

#include <yt/core/yson/consumer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \param wrapWithMap If True then the parser wraps values with calls to
 *  #IYsonConsumer::OnBeginMap and #IYsonConsumer::OnEndMap.
 */
std::unique_ptr<IParser> CreateParserForDsv(
    NYson::IYsonConsumer* consumer,
    TDsvFormatConfigPtr config = New<TDsvFormatConfig>(),
    bool wrapWithMap = true);

////////////////////////////////////////////////////////////////////////////////

void ParseDsv(
    IInputStream* input,
    NYson::IYsonConsumer* consumer,
    TDsvFormatConfigPtr config = New<TDsvFormatConfig>());

void ParseDsv(
    const TStringBuf& data,
    NYson::IYsonConsumer* consumer,
    TDsvFormatConfigPtr config = New<TDsvFormatConfig>());

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
